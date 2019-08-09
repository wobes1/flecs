#include "flecs_private.h"

static
void merge_families(
    ecs_world_t *world,
    ecs_stage_t *stage)
{

}

static
void notify_new_tables(
    ecs_world_t *world, 
    uint32_t old_table_count, 
    uint32_t new_table_count) 
{

}

static
void merge_commits(
    ecs_world_t *world,
    ecs_stage_t *stage)
{  

}

static
void clean_types(
    ecs_stage_t *stage)
{

}

static
void clean_tables(
    ecs_world_t *world,
    ecs_stage_t *stage)
{

}

/* -- Private functions -- */

void ecs_stage_init(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    bool is_main_stage = stage == &world->main_stage;

    memset(stage, 0, sizeof(ecs_stage_t));

    if (!is_main_stage) {
        stage->entity_index = ecs_map_new(ecs_record_t, 0);
        stage->data_stage = ecs_map_new(ecs_column_t*, 0);
        stage->remove_merge = ecs_map_new(ecs_type_t, 0);
    }

    stage->range_check_enabled = true;
}

void ecs_stage_fini(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    bool is_main_stage = stage == &world->main_stage;
    bool is_temp_stage = stage == &world->temp_stage;

    ecs_map_free(stage->entity_index);

    clean_tables(world, stage);

    if (!is_temp_stage) {
        clean_types(stage);
    }

    if (!is_main_stage) {
        ecs_map_free(stage->data_stage);
        ecs_map_free(stage->remove_merge);
    }
}

void ecs_stage_merge(
    ecs_world_t *world,
    ecs_stage_t *stage)
{

}
