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
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_vector_t *entity_column = columns[0].data;
    int32_t count = ecs_vector_count(entity_column);

    ecs_assert(count > 0, ECS_INTERNAL_ERROR, NULL);
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

void ecs_columns_swap(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_column_t *columns,
    int32_t row_1,
    int32_t row_2,
    ecs_record_t *row_ptr_1,
    ecs_record_t *row_ptr_2)
{    
    ecs_assert(row_1 >= 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(row_2 >= 0, ECS_INTERNAL_ERROR, NULL);

    if (row_1 == row_2) {
        return;
    }

    ecs_entity_t *entities = ecs_vector_first(columns[0].data);
    ecs_entity_t e1 = entities[row_1];
    ecs_entity_t e2 = entities[row_2];
    
    /* Get pointers to records in entity index */
    if (!row_ptr_1) {
        row_ptr_1 = ecs_get_entity(world, stage, e1);
    }

    if (!row_ptr_2) {
        row_ptr_2 = ecs_get_entity(world, stage, e2);
    }

    /* Swap entities */
    entities[row_1] = e2;
    entities[row_2] = e1;
    row_ptr_1->row = row_2 + 1;
    row_ptr_2->row = row_1 + 1;

    /* Swap columns */
    uint32_t i, column_count = ecs_vector_count(table->type);
    
    for (i = 0; i < column_count; i ++) {
        void *data = ecs_vector_first(columns[i + 1].data);
        uint32_t size = columns[i + 1].size;

        if (size) {
            void *tmp = _ecs_os_alloca(size, 1);

            void *el_1 = ECS_OFFSET(data, size * row_1);
            void *el_2 = ECS_OFFSET(data, size * row_2);

            memcpy(tmp, el_1, size);
            memcpy(el_1, el_2, size);
            memcpy(el_2, tmp, size);
        }
    }
}

void ecs_columns_move_back_and_swap(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_column_t *columns,
    uint32_t row,
    uint32_t count)
{
    ecs_entity_t *entities = ecs_vector_first(columns[0].data);
    uint32_t i;

    /* First move back and swap entities */
    ecs_entity_t e = entities[row - 1];
    for (i = 0; i < count; i ++) {
        ecs_entity_t cur = entities[row + i];
        entities[row + i - 1] = cur;

        ecs_record_t *row_ptr = ecs_get_entity(world, stage, cur);
        row_ptr->row = row + i;
    }

    entities[row + count - 1] = e;
    ecs_record_t *row_ptr = ecs_get_entity(world, stage, e);
    row_ptr->row = row + count;

    /* Move back and swap columns */
    uint32_t column_count = ecs_vector_count(table->type);
    
    for (i = 0; i < column_count; i ++) {
        void *data = ecs_vector_first(columns[i + 1].data);
        uint32_t size = columns[i + 1].size;

        if (size) {
            /* Backup first element */
            void *tmp = _ecs_os_alloca(size, 1);
            void *el = ECS_OFFSET(data, size * (row - 1));
            memcpy(tmp, el, size);

            /* Move component values */
            for (i = 0; i < count; i ++) {
                void *dst = ECS_OFFSET(data, size * (row + i - 1));
                void *src = ECS_OFFSET(data, size * (row + i));
                memcpy(dst, src, size);
            }

            /* Move first element to last element */
            void *dst = ECS_OFFSET(data, size * (row + count - 1));
            memcpy(dst, tmp, size);
        }
    }
}

void ecs_columns_merge(
    ecs_world_t *world,
    ecs_table_t *new_table,
    ecs_table_t *old_table)
{
    ecs_assert(old_table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(new_table != old_table, ECS_INTERNAL_ERROR, NULL);

    ecs_type_t new_type = new_table ? new_table->type : NULL;
    ecs_type_t old_type = old_table->type;
    ecs_assert(new_type != old_type, ECS_INTERNAL_ERROR, NULL);

    ecs_column_t *new_columns = new_table ? new_table->columns : NULL;
    ecs_column_t *old_columns = old_table->columns;

    if (!old_columns) {
        return;
    }

    uint32_t old_count = old_columns->data ? ecs_vector_count(old_columns->data) : 0;
    uint32_t new_count = 0;
    if (new_columns) {
        new_count = new_columns->data ? ecs_vector_count(new_columns->data) : 0;
    }

    /* First, update entity index so old entities point to new type */
    ecs_entity_t *old_entities = ecs_vector_first(old_columns[0].data);
    uint32_t i;
    for(i = 0; i < old_count; i ++) {
        ecs_record_t *row = ecs_sparse_get_or_set_sparse(
            world->entity_index, ecs_record_t, old_entities[i], NULL);

        row->table = new_table;
        row->row = i + new_count;
    }

    if (!new_table) {
        ecs_columns_clear(old_table, old_columns);
        return;
    }

    uint16_t i_new, new_component_count = ecs_vector_count(new_type);
    uint16_t i_old = 0, old_component_count = ecs_vector_count(old_type);
    ecs_entity_t *new_components = ecs_vector_first(new_type);
    ecs_entity_t *old_components = ecs_vector_first(old_type);

    if (!old_count) {
        return;
    }

    for (i_new = 0; i_new <= new_component_count; ) {
        if (i_old == old_component_count) {
            break;
        }

        ecs_entity_t new_component = 0;
        ecs_entity_t old_component = 0;
        uint32_t size = 0;

        if (i_new) {
            new_component = new_components[i_new - 1];
            old_component = old_components[i_old - 1];
            size = new_columns[i_new].size;
        } else {
            size = sizeof(ecs_entity_t);
        }

        if ((new_component & ECS_ENTITY_FLAGS_MASK) || 
            (old_component & ECS_ENTITY_FLAGS_MASK)) 
        {
            break;
        }

        if (new_component == old_component) {
            /* If the new table is empty, move column to new table */
            if (!new_count) {
                if (!new_columns) {
                    new_columns = ecs_columns_new(world, &world->main_stage, new_table);
                    new_table->columns = new_columns;
                }

                if (new_columns[i_new].data) {
                    ecs_vector_free(new_columns[i_new].data);
                }
                new_columns[i_new].data = old_columns[i_old].data;
                old_columns[i_old].data = NULL;
            
            /* If the new table is not empty, copy the contents from the
             * smallest into the largest vector. */
            } else {
                ecs_vector_t *dst = new_columns[i_new].data;
                ecs_vector_t *src = old_columns[i_old].data;

                ecs_vector_params_t params = {.element_size = size};
                ecs_vector_set_count(&dst, &params, new_count + old_count);
                
                void *dst_ptr = ecs_vector_first(dst);
                void *src_ptr = ecs_vector_first(src);

                dst_ptr = ECS_OFFSET(dst_ptr, size * old_count);
                memcpy(dst_ptr, src_ptr, size * old_count);

                ecs_vector_free(src);
                old_columns[i_old].data = NULL;
                new_columns[i_new].data = dst;
            }
            
            i_new ++;
            i_old ++;
        } else if (new_component < old_component) {
            /* This should not happen. A table should never be merged to
             * another table of which the type is not a subset. */
            ecs_abort(ECS_INTERNAL_ERROR, NULL);
        } else if (new_component > old_component) {
            /* Old column does not occur in new table, remove */
            ecs_vector_free(old_columns[i_old].data);
            old_columns[i_old].data = NULL;
            i_old ++;
        }
    }
}

void ecs_columns_clear(
    ecs_table_t *table,
    ecs_column_t *columns)
{
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);
    
    uint32_t i, column_count = ecs_vector_count(table->type);
    
    for (i = 0; i < column_count + 1; i ++) {
        ecs_vector_free(columns[i].data);
        columns[i].data = NULL;
    }
}
