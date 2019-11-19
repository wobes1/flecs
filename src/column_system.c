#include "flecs_private.h"

ecs_entity_t ecs_col_system_new(
    ecs_world_t *world,
    const char *id,
    ecs_system_kind_t kind,
    ecs_signature_t *sig,
    ecs_system_action_t action)
{
    uint32_t count = ecs_signature_columns_count(sig);

    ecs_assert(count != 0, ECS_INVALID_PARAMETER, NULL);

    ecs_entity_t result = _ecs_new(
        world, world->t_col_system->type);

    EcsId *id_data = ecs_get_ptr(world, result, EcsId);
    *id_data = id;

    EcsColSystem *system_data = ecs_get_ptr(world, result, EcsColSystem);
    memset(system_data, 0, sizeof(EcsColSystem));
    system_data->base.action = action;
    system_data->base.enabled = true;
    system_data->base.time_spent = 0;
    system_data->base.kind = kind;
    system_data->query = ecs_new_query(world, sig);

    system_data->column_size = sizeof(int32_t) * (count);
    system_data->ref_size = sizeof(ecs_reference_t) * count;
    system_data->component_size = sizeof(ecs_entity_t) * count;
    system_data->period = 0;
    system_data->entity = result;

    ecs_entity_t *elem = NULL;

    if (kind == EcsManual) {
        elem = ecs_vector_add(&world->on_demand_systems, ecs_entity_t);
    } else if (kind == EcsOnUpdate) {
        elem = ecs_vector_add(&world->on_update_systems, ecs_entity_t);
    } else if (kind == EcsOnValidate) {
        elem = ecs_vector_add(&world->on_validate_systems, ecs_entity_t);            
    } else if (kind == EcsPreUpdate) {
        elem = ecs_vector_add(&world->pre_update_systems, ecs_entity_t);
    } else if (kind == EcsPostUpdate) {
        elem = ecs_vector_add(&world->post_update_systems, ecs_entity_t);
    } else if (kind == EcsOnLoad) {
        elem = ecs_vector_add(&world->on_load_systems, ecs_entity_t);
    } else if (kind == EcsPostLoad) {
        elem = ecs_vector_add(&world->post_load_systems, ecs_entity_t);            
    } else if (kind == EcsPreStore) {
        elem = ecs_vector_add(&world->pre_store_systems, ecs_entity_t);
    } else if (kind == EcsOnStore) {
        elem = ecs_vector_add(&world->on_store_systems, ecs_entity_t);
    }

    *elem = result;

    return result;
}

void ecs_col_system_free(
    EcsColSystem *system_data)
{
    ecs_query_free(system_data->query);
    ecs_vector_free(system_data->jobs);
}

/* -- Public API -- */

static
bool should_run(
    EcsColSystem *system_data,
    float period,
    float delta_time)
{
    float time_passed = system_data->time_passed + delta_time;

    if (time_passed >= period) {
        time_passed -= period;
        if (time_passed > period) {
            time_passed = 0;
        }

        system_data->time_passed = time_passed;
    } else {
        system_data->time_passed = time_passed;
        return false;
    }

    return true;
}

ecs_entity_t ecs_run(
    ecs_world_t *world,
    ecs_entity_t system,
    float delta_time,
    uint32_t offset,
    uint32_t limit,
    void *param)
{
    ecs_world_t *real_world = world;

    if (world->magic == ECS_THREAD_MAGIC) {
        real_world = ((ecs_thread_t*)world)->world; /* dispel the magic */
    }

    EcsColSystem *system_data = ecs_get_ptr_intern(
        real_world, &real_world->main_stage, system, EEcsColSystem, false, false);
    assert(system_data != NULL);

    if (!system_data->base.enabled) {
        return 0;
    }

    ecs_get_stage(&real_world);

    float period = system_data->period;
    bool measure_time = real_world->measure_system_time;

    if (period) {
        if (!should_run(system_data, period, delta_time)) {
            return 0;
        }
    }

    ecs_time_t time_start;
    if (measure_time) {
        ecs_os_get_time(&time_start);
    }

    ecs_system_action_t action = system_data->base.action;
    
    ecs_query_iter_t qiter = ecs_query_iter(system_data->query, offset, limit);
    qiter.rows.world = world;
    qiter.rows.system = system;
    qiter.rows.param = param;
    qiter.rows.delta_time = delta_time + system_data->time_passed;
    qiter.rows.world_time = world->world_time;
    qiter.rows.frame_offset = offset;

    while (ecs_query_next(&qiter)) {
        action(&qiter.rows);
    }

    if (measure_time) {
        system_data->base.time_spent += ecs_time_measure(&time_start);
    }

    return qiter.rows.interrupted_by;
}
