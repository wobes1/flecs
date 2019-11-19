#include "flecs_private.h"


/* -- Private functions -- */

ecs_columns_t* ecs_columns_new(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_type_t type = table->type;

    if (!type) {
        return NULL;
    }

    ecs_columns_t *result = ecs_os_malloc(sizeof(ecs_columns_t));
    ecs_assert(result != NULL, ECS_OUT_OF_MEMORY, NULL);

    result->entities = ecs_vector_new(ecs_entity_t, 0);
    result->record_ptrs = ecs_vector_new(ecs_record_t*, 0);

    result->components = ecs_os_calloc(
        sizeof(ecs_column_t), ecs_vector_count(type));
    ecs_assert(result->components != NULL, ECS_OUT_OF_MEMORY, NULL);

    ecs_entity_t *buf = ecs_vector_first(type);
    uint32_t i, count = ecs_vector_count(type);

    for (i = 0; i < count; i ++) {
        ecs_entity_t e = buf[i];
        uint32_t size = 0;

        if (e == EEcsComponent) {
            size = sizeof(EcsComponent);
        } else if (e == EEcsId) {
            size = sizeof(EcsId);
        } else {
            EcsComponent *component = ecs_get_ptr_intern(
                world, &world->main_stage, buf[i], EEcsComponent, 
                false, false);
            if (component) {
                size = component->size;
            }
        }

        result->components[i].size = size;
    }
    
    return result;
}

void ecs_columns_free(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns)
{
    if (!columns) {
        return;
    }

    ecs_vector_free(columns->entities);
    ecs_vector_free(columns->record_ptrs);
    
    uint32_t i, count = ecs_vector_count(table->type);

    for (i = 0; i < count; i ++) {
        ecs_vector_free(columns->components[i].data);
    }

    free(columns->components);
    free(columns);
}

uint32_t ecs_columns_insert(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    ecs_entity_t entity,
    ecs_record_t *record)
{
    uint32_t column_count = ecs_vector_count(table->type);

    ecs_assert(record || stage != &world->main_stage, ECS_INTERNAL_ERROR, NULL);

    /* Fist add entity to column with entity ids */
    ecs_entity_t *e = ecs_vector_add(&columns->entities, ecs_entity_t);
    ecs_assert(e != NULL, ECS_INTERNAL_ERROR, NULL);
    *e = entity;

    /* Store pointer to main stage record */
    ecs_record_t **r = ecs_vector_add(&columns->record_ptrs, ecs_record_t*);
    ecs_assert(r != NULL, ECS_INTERNAL_ERROR, NULL);
    *r = record;

    /* Add elements to each column array */
    uint32_t i;
    bool reallocd = false;
    ecs_column_t *components = columns->components;

    for (i = 0; i < column_count; i ++) {
        ecs_column_t *component = &components[i];

        uint32_t size = component->size;
        if (size) {
            void *old_vector = component->data;

            _ecs_vector_add(&component->data, size);
            
            if (old_vector != component->data) {
                reallocd = true;
            }
        }
    }

    /* If columns of a main stage table were reallocd, we need to re-resolve */
    if (reallocd && table->columns[0] == columns) {
        world->should_resolve = true;
    }

    /* Return index of last added entity */
    return ecs_vector_count(columns->entities) - 1;;
}

void ecs_columns_delete(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    int32_t index)
{
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_vector_t *entity_column = columns->entities;
    int32_t count = ecs_vector_count(entity_column);

    ecs_assert(count > 0, ECS_INTERNAL_ERROR, NULL);
    count --;
    
    ecs_assert(index <= count, ECS_INTERNAL_ERROR, NULL);

    uint32_t column_count = ecs_vector_count(table->type);
    uint32_t i;

    if (index != count) {   
        /* Move last entity id to index */     
        ecs_entity_t *entities = ecs_vector_first(entity_column);
        ecs_entity_t entity_to_move = entities[count];
        entities[index] = entity_to_move;
        ecs_vector_remove_last(entity_column);

        /* Move last record ptr to index */
        ecs_vector_t *record_column = columns->record_ptrs;     
        ecs_record_t **records = ecs_vector_first(record_column);
        ecs_record_t *record_to_move = records[count];
        records[index] = record_to_move;
        ecs_vector_remove_last(record_column);

        /* Move each component value in array to index */
        ecs_column_t *components = columns->components;
        for (i = 0; i < column_count; i ++) {
            ecs_column_t *component_column = &components[i];
            uint32_t size = component_column->size;
            if (size) {
                _ecs_vector_remove_index(
                    component_column->data, size, index);
            }
        }

        /* Update record of moved entity in entity index */
        if (stage == &world->main_stage) {
            record_to_move->row = index + 1;
        } else {
            ecs_record_t row;
            row.table = table;
            row.row = index + 1;
            ecs_set_entity(world, stage, entity_to_move, &row);
        }

    /* If this is the last entity in the table, just decrease column counts */
    } else {
        ecs_vector_remove_last(entity_column);
        ecs_vector_remove_last(columns->record_ptrs);

        ecs_column_t *components = columns->components;
        for (i = 0; i < column_count; i ++) {
            ecs_column_t *component_column = &components[i];
            if (component_column->size) {
                ecs_vector_remove_last(component_column->data);
            }
        }
    }
}

uint32_t ecs_columns_grow(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t count,
    ecs_entity_t first_entity)
{
    uint32_t column_count = ecs_vector_count(table->type);

    /* Grow column with entity ids */
    ecs_entity_t *e = ecs_vector_addn(&columns->entities, ecs_entity_t, count);
    ecs_assert(e != NULL, ECS_INTERNAL_ERROR, NULL);

    /* Grow column with record pointers */
    ecs_record_t **r = ecs_vector_addn(&columns->record_ptrs, ecs_record_t*, count);
    ecs_assert(r != NULL, ECS_INTERNAL_ERROR, NULL);

    uint32_t i;
    for (i = 0; i < count; i ++) {
        e[i] = first_entity + i;
        r[i] = NULL; /* Will have to be populated by callee */
    }

    bool reallocd = false;

    /* Add elements to each column array */
    ecs_column_t *components = columns->components;
    for (i = 0; i < column_count ; i ++) {
        ecs_column_t *component = &components[i];
        uint32_t size = component->size;
        if (!size) {
            continue;
        }
    
        ecs_vector_t *old_vector = component->data;

        _ecs_vector_addn(&component->data, size, count);

        if (old_vector != component->data) {
            reallocd = true;
        }
    }

    uint32_t row_count = ecs_vector_count(columns->entities);

    /* If columns of a main stage table were reallocd, we need to re-resolve */
    if (reallocd && table->columns[0] == columns) {
        world->should_resolve = true;
    }

    /* Return index of first added entity */
    return row_count - count + 1;
}

int16_t ecs_columns_set_size(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t count)
{
    uint32_t column_count = ecs_vector_count(table->type);

    if (!columns) {
        columns = table->columns[0] = ecs_columns_new(world, table);
    }

    ecs_vector_set_size(&columns->entities, ecs_entity_t, count);
    ecs_vector_set_size(&columns->record_ptrs, ecs_record_t*, count);

    uint32_t i;
    ecs_column_t *components = columns->components;
    for (i = 0; i < column_count; i ++) {
        ecs_column_t* component = &components[i];
        uint32_t size = component->size;
        if (!size) {
            continue;
        }

        _ecs_vector_set_size(
            &component->data, size, count);
    }

    return 0;
}

uint64_t ecs_columns_count(
    ecs_columns_t *columns)
{
    return ecs_vector_count(columns->entities);
}

void ecs_columns_swap(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
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

    ecs_entity_t *entities = ecs_vector_first(columns->entities);
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
    ecs_column_t *components = columns->components;
    
    for (i = 0; i < column_count; i ++) {
        ecs_column_t *component = &components[i];
        void *data = ecs_vector_first(component->data);
        uint32_t size = component->size;

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

void ecs_columns_copy_back_and_swap(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t row,
    uint32_t count)
{
    ecs_entity_t *entities = ecs_vector_first(columns->entities);
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
    ecs_column_t *components = columns->components;

    for (i = 0; i < column_count; i ++) {
        ecs_column_t *component = &components[i];
        void *data = ecs_vector_first(component->data);
        uint32_t size = component->size;

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

void merge_vector(
    ecs_vector_t **dst_out,
    ecs_vector_t *src,
    uint32_t size)
{
    ecs_vector_t *dst = *dst_out;
    uint32_t dst_count = ecs_vector_count(dst);

    if (!dst_count) {
        if (dst) {
            ecs_vector_free(dst);
        }

        *dst_out = src;
    
    /* If the new table is not empty, copy the contents from the
     * src into the dst. */
    } else {
        uint32_t src_count = ecs_vector_count(src);
        _ecs_vector_set_count(&dst, size, dst_count + src_count);
        
        void *dst_ptr = ecs_vector_first(dst);
        void *src_ptr = ecs_vector_first(src);

        dst_ptr = ECS_OFFSET(dst_ptr, size * src_count);
        memcpy(dst_ptr, src_ptr, size * src_count);

        ecs_vector_free(src);
        *dst_out = dst;
    }    
}

void ecs_columns_merge(
    ecs_world_t *world,
    ecs_table_t *dst_table,
    ecs_table_t *src_table)
{
    ecs_assert(src_table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(dst_table != src_table, ECS_INTERNAL_ERROR, NULL);

    ecs_type_t dst_type = dst_table ? dst_table->type : NULL;
    ecs_type_t src_type = src_table->type;
    ecs_assert(dst_type != src_type, ECS_INTERNAL_ERROR, NULL);
    ecs_columns_t *src_columns = src_table->columns[0];

    if (!src_columns) {
        return;
    }

    uint32_t src_count = src_columns->entities ? ecs_vector_count(src_columns->entities) : 0;
    uint32_t dst_count = 0;

    /* First, update entity index so old entities point to new type */
    ecs_record_t **src_records = ecs_vector_first(src_columns->record_ptrs);
    uint32_t i;
    for(i = 0; i < src_count; i ++) {
        ecs_record_t *row = src_records[i];
        row->table = dst_table;
        row->row = i + dst_count;
    }

    /* If there is no destination table, just clear data in source table */
    if (!dst_table) {
        ecs_columns_clear(src_table, src_columns);
        return;
    }

    ecs_columns_t *dst_columns = dst_table->columns[0];
    if (!dst_columns) {
        dst_columns = dst_table->columns[0] = ecs_columns_new(world, dst_table);
    }

    dst_count = dst_columns->entities ? ecs_vector_count(dst_columns->entities) : 0;

    uint16_t i_dst, dst_component_count = ecs_vector_count(dst_type);
    uint16_t i_src = 0, src_component_count = ecs_vector_count(src_type);
    ecs_entity_t *dst_components = ecs_vector_first(dst_type);
    ecs_entity_t *src_components = ecs_vector_first(src_type);

    if (!src_count) {
        return;
    }

    ecs_column_t *src_component_columns = src_columns->components;
    ecs_column_t *dst_component_columns = dst_columns->components;

    for (i_dst = 0; i_dst < dst_component_count; ) {
        if (i_src == src_component_count) {
            break;
        }

        ecs_column_t *dst_component_column = &dst_component_columns[i_dst];
        ecs_entity_t dst_component = 0;
        ecs_entity_t src_component = 0;
        uint32_t size = 0;

        if (i_dst) {
            dst_component = dst_components[i_dst - 1];
            src_component = src_components[i_src - 1];
            size = dst_component_column->size;
        } else {
            size = sizeof(ecs_entity_t);
        }

        if ((dst_component & ECS_ENTITY_FLAGS_MASK) || 
            (src_component & ECS_ENTITY_FLAGS_MASK)) 
        {
            break;
        }

        ecs_column_t *src_component_column = &src_component_columns[i_src];

        if (dst_component == src_component) {
            merge_vector(
                &dst_component_column->data, src_component_column->data, size);
            src_component_column->data = NULL;
            
            i_dst ++;
            i_src ++;
        } else if (dst_component < src_component) {
            /* This should not happen. A table should never be merged to
             * another table of which the type is not a subset. */
            ecs_abort(ECS_INTERNAL_ERROR, NULL);
        } else if (dst_component > src_component) {
            /* Old column does not occur in new table, remove */
            ecs_vector_free(src_component_column->data);
            src_component_column->data = NULL;
            i_src ++;
        }
    }

    /* Merge entities and record_ptrs vector */
    merge_vector(&dst_columns->entities, src_columns->entities, 
        sizeof(ecs_entity_t));
    merge_vector(&dst_columns->record_ptrs, src_columns->record_ptrs, 
        sizeof(ecs_record_t*));
    src_columns->entities = NULL;
    src_columns->record_ptrs = NULL;    
}

static
void copy_column(
    ecs_column_t *dst_column,
    int32_t dst_index,
    ecs_column_t *src_column,
    int32_t src_index)
{
    ecs_assert(dst_index >= 0, ECS_INTERNAL_ERROR, NULL);

    uint32_t size = dst_column->size;

    if (size) {
        uint32_t size = dst_column->size;

        if (src_index < 0) src_index *= -1;
        
        void *dst = _ecs_vector_get(dst_column->data, size, dst_index);
        void *src = _ecs_vector_get(src_column->data, size, src_index);
            
        ecs_assert(dst != NULL, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);

        memcpy(dst, src, size);
    }
}

void ecs_columns_copy(
    ecs_type_t dst_type,
    ecs_columns_t *dst_columns,
    int32_t dst_index,
    ecs_type_t src_type,
    ecs_columns_t *src_columns,
    int32_t src_index)
{
    uint16_t i_dst, dst_component_count = ecs_vector_count(dst_type);
    uint16_t i_src = 0, src_component_count = ecs_vector_count(src_type);
    ecs_entity_t *dst_components = ecs_vector_first(dst_type);
    ecs_entity_t *src_components = ecs_vector_first(src_type);

    ecs_assert(dst_index >= 0, ECS_INTERNAL_ERROR, NULL);

    ecs_assert(src_columns->entities != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(dst_columns->entities != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_column_t *dst_component_columns = dst_columns->components;
    ecs_column_t *src_component_columns = src_columns->components;

    for (i_dst = 0; i_dst < dst_component_count; ) {
        if (i_src == src_component_count) {
            break;
        }

        ecs_entity_t dst_component = dst_components[i_dst];
        ecs_entity_t src_component = src_components[i_src];

        if ((dst_component & ECS_ENTITY_FLAGS_MASK) || 
            (src_component & ECS_ENTITY_FLAGS_MASK)) 
        {
            /* If we hit a type mask, there is no more data to copy */
            break;
        }

        if (dst_component == src_component) {
            ecs_column_t *dst_component_column = &dst_component_columns[i_dst];
            ecs_column_t *src_component_column = &src_component_columns[i_src];            

            copy_column(
                dst_component_column, dst_index, 
                src_component_column, src_index);

            i_dst ++;
            i_src ++;
        } else if (dst_component < src_component) {
            i_dst ++;
        } else if (dst_component > src_component) {
            i_src ++;
        }
    }
}

void ecs_columns_clear(
    ecs_table_t *table,
    ecs_columns_t *columns)
{
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);
    
    uint32_t i, column_count = ecs_vector_count(table->type);
    
    ecs_vector_free(columns->entities);
    columns->entities = NULL;

    ecs_vector_free(columns->record_ptrs);
    columns->record_ptrs = NULL;

    ecs_column_t *components = columns->components;
    for (i = 0; i < column_count + 1; i ++) {
        ecs_column_t *component = &components[i];
        ecs_vector_free(component->data);
        component->data = NULL;
    }
}
