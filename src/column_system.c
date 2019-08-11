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

    float system_delta_time = delta_time + system_data->time_passed;
    float period = system_data->period;
    bool measure_time = real_world->measure_system_time;

    ecs_matched_table_t *tables = ecs_vector_first(system_data->query->tables);
    uint32_t i, table_count = ecs_vector_count(system_data->query->tables);

    if (!table_count) {
        return 0;
    }

    if (period) {
        if (!should_run(system_data, period, delta_time)) {
            return 0;
        }
    }

    ecs_time_t time_start;
    if (measure_time) {
        ecs_os_get_time(&time_start);
    }

    uint32_t column_count = ecs_vector_count(system_data->query->sig.columns);
    ecs_entity_t interrupted_by = 0;
    ecs_system_action_t action = system_data->base.action;
    bool offset_limit = (offset | limit) != 0;
    bool limit_set = limit != 0;

    ecs_rows_t info = {
        .world = world,
        .system = system,
        .param = param,
        .column_count = column_count,
        .delta_time = system_delta_time,
        .world_time = world->world_time,
        .frame_offset = offset
    };

    for (i = 0; i < table_count; i ++) {
        ecs_matched_table_t *table = &tables[i];

        ecs_table_t *world_table = table->table;
        ecs_column_t *table_data = world_table->columns;
        uint32_t first = 0, count = ecs_column_count(world_table->columns);

        if (filter) {
            if (!ecs_type_contains(
                real_world, world_table->type, filter, true, true))
            {
                continue;
            }
        }

        if (offset_limit) {
            if (offset) {
                if (offset > count) {
                    offset -= count;
                    continue;
                } else {
                    first += offset;
                    count -= offset;
                    offset = 0;
                }
            }

            if (limit) {
                if (limit > count) {
                    limit -= count;
                } else {
                    count = limit;
                    limit = 0;
                }
            } else if (limit_set) {
                break;
            }
        }

        if (!count) {
            continue;
        }

        if (table->references) {
            info.references = ecs_vector_first(table->references);
        } else {
            info.references = NULL;
        }

        info.columns =  table->columns;
        info.table = world_table;
        info.table_columns = table_data;
        info.components = table->components;
        info.offset = first;
        info.count = count;

        ecs_entity_t *entity_buffer = 
                ecs_vector_first(((ecs_column_t*)info.table_columns)[0].data);
        info.entities = &entity_buffer[first];
        
        action(&info);

        info.frame_offset += count;

        if (info.interrupted_by) {
            interrupted_by = info.interrupted_by;
            break;
        }
    }

    if (measure_time) {
        system_data->base.time_spent += ecs_time_measure(&time_start);
    }

    return interrupted_by;
}

ecs_entity_t ecs_run(
    ecs_world_t *world,
    ecs_entity_t system,
    float delta_time,
    void *param)
{
    return ecs_run_w_filter(world, system, delta_time, 0, 0, 0, param);
}
