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
        ecs_column_t *main_columns = ecs_table_get_columns(world, &world->main_stage, table);
        ecs_column_t *columns = ecs_table_get_columns(world, stage, table);
    }
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
