#include "flecs_private.h"


/* -- Private functions -- */

ecs_column_t* ecs_columns_new(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table)
{
    ecs_type_t type = table->type;

    if (!type) {
        return NULL;
    }

    ecs_column_t *result = ecs_os_calloc(
        sizeof(ecs_column_t), ecs_vector_count(type) + 1);

    ecs_assert(result != NULL, ECS_OUT_OF_MEMORY, NULL);

    ecs_entity_t *buf = ecs_vector_first(type);
    uint32_t i, count = ecs_vector_count(type);

    /* First column is reserved for storing entity id's */
    result[0].size = sizeof(ecs_entity_t);
    result[0].data = NULL;

    for (i = 0; i < count; i ++) {
        ecs_entity_t e = buf[i];
        uint32_t size = 0;

        if (e == EEcsComponent) {
            size = sizeof(EcsComponent);
        } else if (e == EEcsId) {
            size = sizeof(EcsId);
        } else {
            ecs_entity_info_t info = {.entity = buf[i]};
            EcsComponent *component = ecs_get_ptr_intern(
                world, stage, &info, EEcsComponent, false, false);
            if (component) {
                size = component->size;
            }
        }

        result[i + 1].size = size;
    }
    
    return result;
}

void ecs_column_free(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_column_t *columns)
{
    if (!columns) {
        return;
    }
    
    uint32_t i, count = ecs_vector_count(table->type);

    for (i = 0; i < count; i ++) {
        ecs_vector_free(columns[i].data);
    }

    free(columns);
}

uint32_t ecs_columns_insert(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_column_t *columns,
    ecs_entity_t entity)
{
    uint32_t column_count = ecs_vector_count(table->type);

    /* Fist add entity to column with entity ids */
    ecs_entity_t *e = ecs_vector_add(&columns[0].data, &handle_arr_params);
    ecs_assert(e != NULL, ECS_INTERNAL_ERROR, NULL);

    *e = entity;

    /* Add elements to each column array */
    uint32_t i;
    bool reallocd = false;

    for (i = 1; i < column_count + 1; i ++) {
        uint32_t size = columns[i].size;
        if (size) {
            ecs_vector_params_t params = {.element_size = size};
            void *old_vector = columns[i].data;

            ecs_vector_add(&columns[i].data, &params);
            
            if (old_vector != columns[i].data) {
                reallocd = true;
            }
        }
    }

    uint32_t index = ecs_vector_count(columns[0].data) - 1;

    if (reallocd && table->columns == columns) {
        world->should_resolve = true;
    }

    /* Return index of last added entity */
    return index;
}

void ecs_columns_delete(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_column_t *columns,
    int32_t index)
{
    ecs_vector_t *entity_column = columns[0].data;
    uint32_t count = ecs_vector_count(entity_column);

    ecs_assert(count != 0, ECS_INTERNAL_ERROR, NULL);
    count --;
    
    ecs_assert(index <= count, ECS_INTERNAL_ERROR, NULL);

    uint32_t column_last = ecs_vector_count(table->type) + 1;
    uint32_t i;

    if (index != count) {
        /* Move last entity in array to index */
        ecs_entity_t *entities = ecs_vector_first(entity_column);
        ecs_entity_t to_move = entities[count];
        entities[index] = to_move;

        for (i = 1; i < column_last; i ++) {
            if (columns[i].size) {
                ecs_vector_params_t params = {.element_size = columns[i].size};
                ecs_vector_remove_index(columns[i].data, &params, index);
            }
        }

        /* Last entity in table is now moved to index of removed entity */
        ecs_record_t row;
        row.table = table;
        row.row = index + 1;
        ecs_set_entity(world, stage, to_move, &row);

        /* Decrease size of entity column */
        ecs_vector_remove_last(entity_column);

    /* This is the last entity in the table, just decrease column counts */
    } else {
        ecs_vector_remove_last(entity_column);

        for (i = 1; i < column_last; i ++) {
            if (columns[i].size) {
                ecs_vector_remove_last(columns[i].data);
            }
        }
    }
}

uint32_t ecs_columns_grow(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_column_t *columns,
    uint32_t count,
    ecs_entity_t first_entity)
{
    uint32_t column_count = ecs_vector_count(table->type);

    /* Fist add entity to column with entity ids */
    ecs_entity_t *e = ecs_vector_addn(&columns[0].data, &handle_arr_params, count);
    ecs_assert(e != NULL, ECS_INTERNAL_ERROR, NULL);

    uint32_t i;
    for (i = 0; i < count; i ++) {
        e[i] = first_entity + i;
    }

    bool reallocd = false;

    /* Add elements to each column array */
    for (i = 1; i < column_count + 1; i ++) {
        ecs_vector_params_t params = {.element_size = columns[i].size};
        if (!params.element_size) {
            continue;
        }
        void *old_vector = columns[i].data;

        ecs_vector_addn(&columns[i].data, &params, count);

        if (old_vector != columns[i].data) {
            reallocd = true;
        }
    }

    uint32_t row_count = ecs_vector_count(columns[0].data);

    if (reallocd && table->columns == columns) {
        world->should_resolve = true;
    }

    /* Return index of first added entity */
    return row_count - count + 1;
}

int16_t ecs_columns_set_size(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_column_t *columns,
    uint32_t count)
{
    uint32_t column_count = ecs_vector_count(table->type);
    if (!columns) {
        columns = table->columns = ecs_columns_new(world, stage, table);
    }

    uint32_t size = ecs_vector_set_size(
        &columns[0].data, &handle_arr_params, count);
    ecs_assert(size != 0, ECS_INTERNAL_ERROR, NULL);
    (void)size;

    uint32_t i;
    for (i = 1; i < column_count + 1; i ++) {
        ecs_vector_params_t params = {.element_size = columns[i].size};
        uint32_t size = ecs_vector_set_size(&columns[i].data, &params, count);
        ecs_assert(size != 0, ECS_INTERNAL_ERROR, NULL);
        (void)size;
    }

    return 0;
}

uint64_t ecs_column_count(
    ecs_column_t *columns)
{
    return ecs_vector_count(columns[0].data);
}
