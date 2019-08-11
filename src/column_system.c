#include "flecs_private.h"

const ecs_vector_params_t matched_table_params = {
    .element_size = sizeof(ecs_matched_table_t)
};

const ecs_vector_params_t system_column_params = {
    .element_size = sizeof(ecs_signature_column_t)
};

const ecs_vector_params_t reference_params = {
    .element_size = sizeof(ecs_reference_t)
};

const ecs_vector_params_t matched_column_params = {
    .element_size = sizeof(uint32_t)
};

ecs_entity_t ecs_col_system_new(
    ecs_world_t *world,
    const char *id,
    EcsSystemKind kind,
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

    system_data->column_params.element_size = sizeof(int32_t) * (count);
    system_data->ref_params.element_size = sizeof(ecs_reference_t) * count;
    system_data->component_params.element_size = sizeof(ecs_entity_t) * count;
    system_data->period = 0;
    system_data->entity = result;

    ecs_entity_t *elem = NULL;

    if (kind == EcsManual) {
        elem = ecs_vector_add(&world->on_demand_systems, &handle_arr_params);
    } else if (kind == EcsOnUpdate) {
        elem = ecs_vector_add(&world->on_update_systems, &handle_arr_params);
    } else if (kind == EcsOnValidate) {
        elem = ecs_vector_add(&world->on_validate_systems, &handle_arr_params);            
    } else if (kind == EcsPreUpdate) {
        elem = ecs_vector_add(&world->pre_update_systems, &handle_arr_params);
    } else if (kind == EcsPostUpdate) {
        elem = ecs_vector_add(&world->post_update_systems, &handle_arr_params);
    } else if (kind == EcsOnLoad) {
        elem = ecs_vector_add(&world->on_load_systems, &handle_arr_params);
    } else if (kind == EcsPostLoad) {
        elem = ecs_vector_add(&world->post_load_systems, &handle_arr_params);            
    } else if (kind == EcsPreStore) {
        elem = ecs_vector_add(&world->pre_store_systems, &handle_arr_params);
    } else if (kind == EcsOnStore) {
        elem = ecs_vector_add(&world->on_store_systems, &handle_arr_params);
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

ecs_entity_t _ecs_run_w_filter(
    ecs_world_t *world,
    ecs_entity_t system,
    float delta_time,
    uint32_t offset,
    uint32_t limit,
    ecs_type_t filter,
    void *param)
{
    ecs_world_t *real_world = world;

    if (world->magic == ECS_THREAD_MAGIC) {
        real_world = ((ecs_thread_t*)world)->world; /* dispel the magic */
    }

    ecs_entity_info_t sys_info = {.entity = system};
    EcsColSystem *system_data = ecs_get_ptr_intern(real_world, &real_world->main_stage, 
        &sys_info, EEcsColSystem, false, false);
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

    ecs_rows_t *ptr;

    while ((ptr = ecs_query_next(&qiter))) {
        action(ptr);
    }

    if (measure_time) {
        system_data->base.time_spent += ecs_time_measure(&time_start);
    }

    return qiter.rows.interrupted_by;
}

ecs_entity_t ecs_run(
    ecs_world_t *world,
    ecs_entity_t system,
    float delta_time,
    void *param)
{
    return ecs_run_w_filter(world, system, delta_time, 0, 0, 0, param);
}
