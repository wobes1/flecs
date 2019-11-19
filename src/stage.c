#include "flecs_private.h"

static
void merge_data(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    /* Loop the tables for which the stage has modified data */
    uint32_t i, count = ecs_vector_count(stage->dirty_tables);
    ecs_table_t **tables = ecs_vector_first(stage->dirty_tables);

    for (i = 0; i < count; i ++) {
        ecs_table_t *table = tables[i];
        ecs_columns_t *main_columns = ecs_table_get_columns(world, &world->main_stage, table);
        ecs_columns_t *columns = ecs_table_get_columns(world, stage, table);
        uint32_t i, count = ecs_columns_count(columns);
        ecs_entity_t *entities = ecs_vector_first(columns->entities);
        ecs_record_t **record_ptrs = ecs_vector_first(columns->record_ptrs);
        uint32_t c, column_count = ecs_vector_count(table->type);

        ecs_assert(main_columns != columns, ECS_INTERNAL_ERROR, NULL);

        /* Ensure destination row array is large enough */
        ecs_vector_set_min_size(&table->dst_rows, int32_t, count);
        uint32_t *dst_rows = ecs_vector_first(table->dst_rows);

        /* First make sure that all entities have entries in the main stage */
        for (i = 0; i < count; i ++) {
            ecs_record_t *record = record_ptrs[i];
            ecs_entity_t entity = entities[i];

            /* If the entity did not yet exist in the main stage, register it */
            if (!record) {
                record = ecs_set_entity_in_main(world, entity);
                record->table = NULL;
            }

            /* Check if entity was already stored in this main stage table. If
             * not, add a new row */
            ecs_table_t *src_table = record->table;
            if (src_table != table) {
                record->table = table;
                dst_rows[i] = record->row = ecs_columns_insert(
                    world, &world->main_stage, table, main_columns, entity, record);
            } else {
                dst_rows[i] = record->row;
            }
        }

        /* Now copy data column by column from the stage to the main stage */
        for (c = 0; c < column_count; c ++) {
            ecs_column_t *main_column = &main_columns->components[c];
            void *main_data = ecs_vector_first(main_column->data);

            ecs_column_t *column = &columns->components[c];
            void *data = ecs_vector_first(column->data);
            uint16_t size = column->size;
            void *src = data;

            for (i = 0; i < count; i ++) {
                void *dst = ECS_OFFSET(main_data, size * dst_rows[i]);
                memcpy(dst, src, size);
                void *src = ECS_OFFSET(src, size);
            }

            ecs_vector_clear(column->data);
        }

        ecs_vector_clear(columns->entities);
        ecs_vector_clear(columns->record_ptrs);
    }

    ecs_vector_clear(stage->dirty_tables);
    ecs_map_clear(stage->entity_index);
}

static
void clean_tables(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    uint32_t i, count = ecs_sparse_count(stage->tables);
    for (i = 0; i < count; i ++) {
        ecs_table_t *table = ecs_sparse_get(stage->tables, ecs_table_t, i);
        ecs_table_fini(world, table);
    }
}

/* -- Private functions -- */

void ecs_stage_init(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    bool is_main_stage = stage == &world->main_stage;
    bool is_temp_stage = stage == &world->temp_stage;

    memset(stage, 0, sizeof(ecs_stage_t));

    /* Tables are shared between temp stage and main stage */
    if (is_temp_stage) {
        stage->tables = world->main_stage.tables;
        stage->table_root = world->main_stage.table_root;
    } else {
        stage->tables = ecs_sparse_new(ecs_table_t, 0);
    }

    /* These data structures are only used when not in the main stage */
    if (!is_main_stage) {
        stage->entity_index = ecs_map_new(ecs_record_t, 0);
        stage->dirty_tables = ecs_vector_new(ecs_table_t*, 0);
    }

    stage->range_check_enabled = true;
}

void ecs_stage_fini(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    bool is_main_stage = stage == &world->main_stage;
    bool is_temp_stage = stage == &world->temp_stage;

    /* Don't clean tables from temp stage, as they are shared with main stage */
    if (!is_temp_stage) {
        clean_tables(world, stage);        
        ecs_sparse_free(stage->tables);
    }

    /* These data structures are only used when not in the main stage */
    if (!is_main_stage) {
        ecs_map_free(stage->entity_index);
        ecs_vector_free(stage->dirty_tables);
    }
}

void ecs_stage_merge(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    merge_data(world, stage);
}
