
#include "flecs_private.h"

static
void* get_row_ptr(
    ecs_type_t type,
    ecs_columns_t *columns,
    int32_t row,
    ecs_entity_t component)
{
    ecs_assert(row >= 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(ecs_vector_count(type) < ECS_MAX_ENTITIES_IN_TYPE, ECS_TYPE_TOO_LARGE, NULL);
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_column_t *components = columns->components;
    ecs_entity_t *array = ecs_vector_first(type);
    int32_t i, count = ecs_vector_count(type);

    for (i = 0; i < count; i ++) {
        if (array[i] == component) {
            ecs_column_t *component = &components[i];
            uint32_t size = component->size; 
            void *data = ecs_vector_first(component->data);
            ecs_assert(data != NULL, ECS_INTERNAL_ERROR, NULL);            
            return ECS_OFFSET(data, row * size);
        }
    }
    
    return NULL;
}

ecs_record_t* ecs_get_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity)
{
    if (!stage || stage == &world->main_stage) {
        if (entity == ECS_SINGLETON) {
            return &world->singleton;
        } else {
            return ecs_sparse_get_sparse(world->entity_index, ecs_record_t, entity);
        }
    } else {
        return ecs_map_get(stage->entity_index, ecs_record_t, entity);
    }
}

ecs_record_t* ecs_register_entity(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    return ecs_sparse_get_or_set_sparse(
        world->entity_index, ecs_record_t, entity, NULL);
}

void ecs_set_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t *record)
{
    ecs_assert(world != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(stage != NULL, ECS_INTERNAL_ERROR, NULL);

    if (stage == &world->main_stage) {
        if (entity == ECS_SINGLETON) {
            world->singleton = *record;
        } else {
            ecs_record_t *dst_record = ecs_register_entity(world, entity);
            *dst_record = *record;
        }

        ecs_assert(
            ecs_vector_count(record->table->columns[0]->entities) >= record->row, 
            ECS_INTERNAL_ERROR, 
            NULL);        
    } else {
        ecs_map_set(stage->entity_index, entity, record);
    }
}

void ecs_delete_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity)
{
    if (!stage || stage == &world->main_stage) {
        if (entity == ECS_SINGLETON) {
            world->singleton = (ecs_record_t){0, 0};
        } else {
            _ecs_sparse_remove(world->entity_index, 0, entity);
        }
    } else {
        ecs_map_set(stage->entity_index, entity, &((ecs_record_t){0, 0}));
    }
}

void ecs_grow_entities(
    ecs_world_t *world,
    ecs_stage_t *stage,
    uint32_t count)
{
    if (!stage || stage == &world->main_stage) {
        ecs_sparse_grow(world->entity_index, count);
    } else {
        ecs_map_grow(stage->entity_index, count);
    }
}

uint32_t ecs_count_entities(
    ecs_world_t *world,
    ecs_stage_t *stage)
{
    if (!stage || stage == &world->main_stage) {
        return ecs_sparse_count(world->entity_index);
    } else {
        return ecs_map_count(stage->entity_index);
    }
}

static
bool update_info(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t *record,
    ecs_entity_info_t *info)
{
    /* If index is negative, this entity is being watched */
    int32_t row = record->row;
    bool is_watched = row < 0;
    row = row * -(is_watched * 2 - 1) - 1 * (row != 0);
    
    info->is_watched = is_watched;
    info->row = row;

    /* Retrieve table, type and data columns */
    ecs_table_t *table;
    if ((table = record->table)) {
        ecs_type_t type = table->type;
        ecs_columns_t *columns = ecs_table_get_columns(world, stage, table);
        ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(ecs_columns_count(columns) != 0, ECS_INTERNAL_ERROR, NULL);

        ecs_record_t **record_ptrs = ecs_vector_first(columns->record_ptrs);
        ecs_assert(record_ptrs != NULL, ECS_INTERNAL_ERROR, NULL);

        info->table = table;
        info->type = type;
        info->columns = columns;
        info->record = record_ptrs[row];

        ecs_assert(type != NULL, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(info->columns != NULL, ECS_INTERNAL_ERROR, NULL);
        
        return true;
    } else {
        return false;
    }

    ecs_assert(info->row >= 0, ECS_INTERNAL_ERROR, NULL);
}

static
bool populate_info(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info)
{
    ecs_entity_t entity = info->entity;

    /* Lookup entity in index */
    ecs_record_t *record = ecs_get_entity(world, stage, entity);
    if (record) {
        update_info(world, stage, entity, record, info);
        return true;
    } else {
        return false;
    }
}

static
ecs_type_t instantiate_prefab(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    bool is_prefab,
    ecs_entity_info_t *prefab_info,
    uint32_t limit,
    ecs_type_t modified)
{
    ecs_type_t prefab_type = prefab_info->type;
    ecs_columns_t *prefab_columns = prefab_info->columns;

    EcsPrefabBuilder *builder = get_row_ptr(prefab_type, 
        prefab_columns, prefab_info->row, EEcsPrefabBuilder);

    /* If the current entity is not a prefab itself, and the prefab
     * has children, add the children to the entity. */
    if (!is_prefab) {
        if (builder && builder->ops) {
            int32_t i, count = ecs_vector_count(builder->ops);
            ecs_builder_op_t *ops = ecs_vector_first(builder->ops);

            for (i = 0; i < count; i ++) {
                ecs_builder_op_t *op = &ops[i];
                
                ecs_entity_t child = _ecs_new_w_count(
                    world, op->type, limit);

                uint32_t j;
                for (j = 0; j < limit; j ++) {
                    ecs_adopt(world, child + j, entity + j);
                    ecs_set(world, child + j, EcsId, {op->id});
                }
            }
        }

        /* Keep track of components shared from new prefabs */
        modified = ecs_type_merge_intern(
            world, stage, modified, prefab_info->type, 0, NULL, NULL);

    /* If the current entity is also prefab, do not add children to
     * it. Instead, add children (if any) of its base to its ops */
    } else if (builder) {
        ecs_entity_info_t info = {.entity = entity};
        EcsPrefabBuilder *entity_builder = ecs_get_ptr_intern(world, 
            stage, &info, EEcsPrefabBuilder, false, false);

        if (!entity_builder) {
            ecs_add(world, entity, EcsPrefabBuilder);
            entity_builder = ecs_get_ptr(world, entity, EcsPrefabBuilder);
            entity_builder->ops = NULL;
        }

        uint32_t count = ecs_vector_count(builder->ops);
        void *new_ops = ecs_vector_addn(
            &entity_builder->ops, ecs_builder_op_t, count);
        
        memcpy(new_ops, ecs_vector_first(builder->ops), 
            sizeof(ecs_builder_op_t) * count);
    }

    return modified;
}

int32_t ecs_type_get_prefab(
    ecs_type_t type,
    int32_t n)
{
    int32_t i, count = ecs_vector_count(type);
    ecs_entity_t *buffer = ecs_vector_first(type);

    for (i = n + 1; i < count; i ++) {
        ecs_entity_t e = buffer[i];
        if (e & ECS_INSTANCEOF) {
            return i;
        }
    }

    return -1;
}

static
void override_component(
    ecs_world_t *world,
    ecs_entity_t component,
    ecs_type_t type,
    ecs_column_t *column,
    uint32_t row,
    uint32_t count);

static
void override_from_base(
    ecs_world_t *world,
    ecs_entity_t base,
    ecs_entity_t component,
    ecs_column_t *column,
    uint32_t row,
    uint32_t count)
{
    ecs_record_t *base_record = ecs_get_entity(world, &world->main_stage, base);
    ecs_assert(base_record != NULL, ECS_INTERNAL_ERROR, NULL);

    int32_t base_row = base_record->row;
    if (!base_row) {
        return;
    }

    int8_t is_monitored = 1 - (base_row < 0) * 2;
    base_row = (base_row * is_monitored) - 1;
    ecs_table_t *base_table = base_record->table;
    ecs_type_t base_type = base_table->type;

    void *base_ptr = get_row_ptr(
        base_type, base_table->columns[0], base_row, component);
    
    if (base_ptr) {
        uint32_t data_size = column->size;
        void *data_array = ecs_vector_first(column->data);
        void *data_ptr = ECS_OFFSET(data_array, data_size * row);

        uint32_t i;
        for (i = 0; i < count; i ++) {
            memcpy(data_ptr, base_ptr, data_size);
            data_ptr = ECS_OFFSET(data_ptr, data_size);
        }
    } else {
        /* If component not found on base, check if base itself inherits */
        override_component(world, component, base_type, column, row, count);
    }
}

static
void override_component(
    ecs_world_t *world,
    ecs_entity_t component,
    ecs_type_t type,
    ecs_column_t *column,
    uint32_t row,
    uint32_t count)
{
    ecs_entity_t *type_array = ecs_vector_first(type);
    int32_t i, type_count = ecs_vector_count(type);

    /* Walk prefabs */
    i = type_count - 1;
    do {
        ecs_entity_t e = type_array[i];

        if (e < ECS_ENTITY_FLAGS_START) {
            break;
        }

        if (e & ECS_INSTANCEOF) {
            override_from_base(
                world, e & ECS_ENTITY_MASK, component, column, row, count);
        }
    } while (--i >= 0);
}

static
void run_row_systems(
    ecs_world_t *world,
    ecs_vector_t *systems,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t row,
    uint32_t count)
{
    uint32_t sys_count = ecs_vector_count(systems);
    ecs_entity_t *sys_array = ecs_vector_first(systems);

    int s;
    for (s = 0; s < sys_count; s++) {
        ecs_run_row_system(
            world, sys_array[s], table, columns, row, count);
    }
}

static
ecs_component_data_t *get_component_data(
    ecs_world_t *world,
    ecs_entity_t component)
{
    ecs_assert(component < ECS_MAX_COMPONENTS, ECS_INTERNAL_ERROR, NULL);
    ecs_component_data_t* cdata_array = ecs_vector_first(world->component_data);
    return &cdata_array[component];
}

static
void run_component_set_action(
    ecs_world_t *world,
    ecs_component_data_t* cdata,
    ecs_entity_t component,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t row,
    uint32_t count)
{
    ecs_vector_t *systems = cdata->on_set;   
    if (systems) {
        run_row_systems(world, systems, table, columns, row, count);
    }
}

static
void run_component_actions(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t row,
    uint32_t count,
    ecs_entity_array_t components,
    bool is_init)
{
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);

    /* Array that contains component callbacks & systems */
    ecs_component_data_t* cdata_array = ecs_vector_first(world->component_data);
    ecs_column_t *component_columns = columns->components;
    ecs_type_t type = NULL;
    uint32_t type_count;
    ecs_entity_t *type_array;
    bool has_base = table->has_base;

    int i, cur = 0;
    for (i = 0; i < components.count; i ++) {
        /* Retrieve component callbacks & systems for component */
        ecs_entity_t component = components.array[i];


        if (component >= ECS_MAX_COMPONENTS) {
            continue;
        }

        ecs_component_data_t *cdata = &cdata_array[component];
        ecs_vector_t *systems;
        ecs_init_t init = NULL;
        ecs_init_t fini = NULL;
        void *ctx;
        
        if (is_init) {
            init = cdata->init;
            systems = cdata->on_add;
            if (!init && !systems && !has_base) {
                continue;
            }
            ctx = cdata->ctx;
        } else {
            fini = cdata->fini;
            systems = cdata->on_remove;
            if (!fini && !systems) {
                continue;
            }
            ctx = cdata->ctx;
        }

        if (!type) {
            type = table->type;
            type_count = ecs_vector_count(type);
            type_array = ecs_vector_first(type);
        }

        /* Find column index of current component */
        while (type_array[cur] != component) {
            cur ++;
        }

        /* Removed components should always be in the old type */
        ecs_assert(cur <= type_count, ECS_INTERNAL_ERROR, NULL);

        /* Get column and pointer to data */
        ecs_column_t *column = &component_columns[cur];
        void *array = ecs_vector_first(column->data);
        void *ptr = ECS_OFFSET(array, column->size * row);

        if (is_init) {
            if (init) {
                init(ptr, ctx);
            }

            if (has_base) {
                override_component(world, component, type, column, row, count);
                run_component_set_action(
                    world, cdata, component, table, columns, row, count);
            }
        }

        if (systems) {
            run_row_systems(world, systems, table, columns, row, count);

            if (is_init) {
                run_component_set_action(
                    world, cdata, component, table, columns, row, count);
            }
        }

        if (fini) {
            fini(ptr, ctx);
        }
    }
}

static
uint32_t new_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_entity_info_t *info,
    ecs_table_t *new_table,
    ecs_entity_array_t *added)
{
    ecs_record_t *record = info->record;
    ecs_columns_t *dst_columns = ecs_table_get_columns(world, stage, new_table);
    uint32_t new_row;

    if (stage == &world->main_stage) {
        if (!record) {
            record = ecs_register_entity(world, entity);
        }

        new_row = ecs_columns_insert(
            world, stage, new_table, dst_columns, entity, record);

        record->table = new_table;
        record->row = new_row + 1;
    } else {
        new_row = ecs_columns_insert(
            world, stage, new_table, dst_columns, entity, NULL);  

        ecs_set_entity(world, stage, entity, &(ecs_record_t){
            .table = new_table,
            .row = new_row + 1
        });
    }

    ecs_assert(
        ecs_vector_count(dst_columns[0].entities) > new_row, 
        ECS_INTERNAL_ERROR, NULL);

    run_component_actions(
        world, stage, new_table, dst_columns, new_row, 1, *added, true);

    ecs_vector_t *new_systems = new_table->on_new;
    if (new_systems) {
        int32_t i, count = ecs_vector_count(new_systems);
        ecs_entity_t *sys_array = ecs_vector_first(new_systems);

        for (i = 0; i < count; i ++) {
            ecs_run_row_system(
                world, sys_array[i], new_table, dst_columns, new_row, 1);
        }
    }

    info->columns = dst_columns;

    return new_row;
}

static
uint32_t move_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_entity_info_t *info,
    ecs_table_t *src_table,
    ecs_columns_t *src_columns,
    int32_t src_row,
    ecs_table_t *dst_table,
    ecs_entity_array_t *added,
    ecs_entity_array_t *removed)
{
    ecs_assert(src_table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(src_columns != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(src_row >= 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(ecs_vector_count(src_columns[0].entities) > src_row, ECS_INTERNAL_ERROR, NULL);
    
    ecs_record_t *record = info->record;
    ecs_columns_t *dst_columns = ecs_table_get_columns(world, stage, dst_table);
    int32_t dst_row = ecs_columns_insert(world, stage, dst_table, dst_columns, entity, record);
    ecs_assert(ecs_vector_count(dst_columns[0].entities) > dst_row, ECS_INTERNAL_ERROR, NULL);

    ecs_columns_move(dst_table->type, dst_columns, dst_row, 
             src_table->type, src_columns, src_row);

    if (removed) {
        run_component_actions(
            world, stage, src_table, src_columns, src_row, 1, *removed, false);
    }

    ecs_columns_delete(world, stage, src_table, src_columns, src_row);

    if (stage == &world->main_stage) {
        record->table = dst_table;
        record->row = dst_row + 1;
    } else {
        ecs_set_entity(world, stage, entity, &(ecs_record_t){
            .table = dst_table,
            .row = dst_row + 1
        });
    }

    if (added) {
        run_component_actions(
            world, stage, dst_table, dst_columns, dst_row, 1, *added, true);
    }

    info->columns = dst_columns;

    return dst_row;
}

static
void delete_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_table_t *src_table,
    ecs_columns_t *src_columns,
    int32_t src_row,
    ecs_entity_array_t *removed)
{
    if (removed) {
        run_component_actions(
            world, stage, src_table, src_columns, src_row, 1, *removed, false);
    }

    ecs_columns_delete(world, stage, src_table, src_columns, src_row);
    ecs_delete_entity(world, stage, entity);
}

/** Commit an entity with a specified type to a table (probably the most 
 * important function in flecs). */
static
bool commit(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info,
    ecs_entity_array_t *to_add,
    ecs_entity_array_t *to_remove,
    bool do_set)
{
    ecs_table_t *dst_table = NULL, *src_table;
    int32_t dst_row = 0;
    bool in_progress = world->in_progress;
    ecs_entity_t entity = info->entity;

    /* Keep track of components that are actually added/removed */
    ecs_entity_array_t added, removed;
    ecs_entity_array_t *to_add_ptr = NULL, *to_remove_ptr = NULL;
    ecs_entity_array_t *added_ptr = NULL, *removed_ptr = NULL;

    if (to_add) {
        to_add_ptr = to_add;
        added_ptr = &added;
        added.array = ecs_os_alloca(ecs_entity_t, to_add->count);
        added.count = 0;
    }

    /* Find the new table */
    src_table = info->table;
    if (src_table) {
        if (to_remove) {
            to_remove_ptr = to_remove;
            removed_ptr = &removed;            

            removed.array = ecs_os_alloca(ecs_entity_t, to_remove->count);
            removed.count = 0;

            dst_table = ecs_table_traverse(
                world, stage, src_table, to_add_ptr, to_remove_ptr, &added, &removed);
        } else {
            dst_table = ecs_table_traverse(
                world, stage, src_table, to_add_ptr, NULL, &added, NULL);
        }

        if (dst_table) {
            if (dst_table != src_table) {
                dst_row = move_entity(
                    world, stage, entity, info, src_table, info->columns, info->row, 
                    dst_table, added_ptr, removed_ptr);
            } else {
                return false;
            }

            info->table = dst_table;
            info->type = dst_table->type;
            info->row = dst_row;            
        } else {
            delete_entity(
                world, stage, entity, src_table, info->columns, info->row, 
                removed_ptr);
        }
    } else {
        dst_table = ecs_table_traverse(
            world, stage, &world->main_stage.table_root, to_add_ptr, NULL, &added, NULL);
        
        if (dst_table) {
            dst_row = new_entity(world, stage, entity, info, dst_table, &added);

            info->table = dst_table;
            info->type = dst_table->type;
            info->row = dst_row;            
        }        
    }

    if (!in_progress) {
        /* Entity ranges are only checked when not iterating. It is allowed to
         * modify entities that existed before setting the range, and thus the
         * range checks are only applied if the src_table is NULL, meaning the
         * entity did not yet exist/was empty. When iterating, src_table refers
         * to a table in the data stage, not to the table in the main stage.
         * Therefore it is not possible to check while in progress if the entity
         * already existed. Instead, the check will be applied when the entity
         * is merged, which will invoke commit again. */
        if (stage->range_check_enabled) {
            ecs_assert(!world->max_handle || entity <= world->max_handle, ECS_OUT_OF_RANGE, 0);
            ecs_assert(entity >= world->min_handle, ECS_OUT_OF_RANGE, 0);
        }
    }

    /* If the entity is being watched, it is being monitored for changes and
     * requires rematching systems when components are added or removed. This
     * ensures that systems that rely on components from containers or prefabs
     * update the matched tables when the application adds or removes a 
     * component from, for example, a container. */
    if (info->is_watched) {
        world->should_match = true;
    }

    return true;
}

static
void* get_ptr_from_prefab(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info,
    ecs_entity_t previous,
    ecs_entity_t component)
{
    ecs_type_t type = info->type;
    ecs_entity_t *type_buffer = ecs_vector_first(type);
    int32_t p = -1;
    void *ptr = NULL;

    while (!ptr && (p = ecs_type_get_prefab(type, p)) != -1) {
        ecs_entity_t prefab = type_buffer[p] & ECS_ENTITY_MASK;

        /* Detect cycles with two entities */
        if (prefab == previous) {
            continue;
        }

        ecs_entity_info_t prefab_info = {.entity = prefab};
        if (populate_info(world, &world->main_stage, &prefab_info)) {
            ptr = get_row_ptr(prefab_info.type, prefab_info.columns, 
                prefab_info.row, component);
            
            if (!ptr) {
                ptr = get_ptr_from_prefab(
                    world, stage, &prefab_info, info->entity, component);
            }
        }
    }

    return ptr;
}

/* -- Private functions -- */

void* ecs_get_ptr_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info,
    ecs_entity_t component,
    bool staged_only,
    bool search_prefab)
{
    ecs_entity_info_t main_info = {0}, staged_info = {0};
    void *ptr = NULL;

    ecs_assert(world->magic == ECS_WORLD_MAGIC, ECS_INTERNAL_ERROR, NULL);

    if (world->in_progress && stage != &world->main_stage) {
        if (populate_info(world, stage, info)) {
            ptr = get_row_ptr(info->type, info->columns, info->row, component);
        }

        if (!ptr && search_prefab) {
            /* Store staged info for when looking up data from prefab */
            staged_info = *info;
        }
    }

    if (!ptr && (!world->in_progress || !staged_only)) {
        if (populate_info(world, &world->main_stage, info)) {
            ptr = get_row_ptr(
                info->type, info->columns, info->row, component);
            if (!ptr && search_prefab) {
                main_info = *info;
            }                
        }
    }

    if (ptr) return ptr;

    if (search_prefab && component != EEcsId && component != EEcsPrefab) {
        if (main_info.table) {
            ptr = get_ptr_from_prefab(world, stage, &main_info, 0, component);
        }

        if (ptr) return ptr;

        if (staged_info.table) {
            ptr = get_ptr_from_prefab(world, stage, &staged_info, 0, component);
        }
    }

    return ptr;
}

void ecs_set_watch(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity)
{    
    ecs_record_t *row = ecs_get_entity(world, stage, entity);

    if (row) {
        if (row->row > 0) {
            row->row *= -1;
        } else if (row->row == 0) {
            /* If entity is empty, there is no index to change the sign of. In
                * this case, set the index to -1, and assign an empty type. */
            row->row = -1;
            row->table = NULL;
        }
    } else {
        ecs_set_entity(world, stage, entity, &((ecs_record_t){.row = -1}));
    }
}

bool ecs_components_contains_component(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_entity_t component,
    ecs_entity_t flags,
    ecs_entity_t *entity_out)
{
    uint32_t i, count = ecs_vector_count(type);
    ecs_entity_t *type_buffer = ecs_vector_first(type);

    for (i = 0; i < count; i ++) {
        if (flags) {
            if ((type_buffer[i] & flags) != flags) {
                continue;
            }
        }

        ecs_entity_t e = type_buffer[i] & ECS_ENTITY_MASK;
        ecs_record_t *row = ecs_get_entity(world, NULL, e);

        if (row && row->table) {
            bool result = ecs_type_has_entity_intern(
                world, row->table->type, component, true);
            if (result) {
                if (entity_out) *entity_out = e;
                return true;
            }
        }
    }

    return false;
}

static
bool get_info(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info)
{
    if (stage == &world->main_stage) {
        ecs_entity_t entity = info->entity;
        ecs_record_t *r;

        bool is_new = false;

        if (entity == ECS_SINGLETON) {
            r = &world->singleton;
        } else {
            r = ecs_sparse_get_or_set_sparse(
                world->entity_index, ecs_record_t, entity, &is_new);
            if (is_new) {
                r->table = NULL;
                r->row = 0;
            }
        }

        info->record = r;
        return update_info(world, stage, entity, r, info);
    } else {
        return populate_info(world, stage, info);
    }
}

void ecs_add_remove_intern(
    ecs_world_t *world,
    ecs_entity_info_t *info,
    ecs_entity_array_t *to_add,
    ecs_entity_array_t *to_remove,
    bool do_set)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);
    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);

    get_info(world, stage, info);

    commit(world, stage, info, to_add, to_remove, do_set);
}

void ecs_delete_w_filter(
    ecs_world_t *world,
    ecs_type_filter_t *filter)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_assert(stage == &world->main_stage, ECS_UNSUPPORTED, 
        "delete_w_filter currently only supported on main stage");

    uint32_t i, count = ecs_sparse_count(world->main_stage.tables);

    for (i = 0; i < count; i ++) {
        ecs_table_t *table = ecs_sparse_get(world->main_stage.tables, ecs_table_t, i);
        ecs_type_t type = table->type;

        if (!ecs_type_match_w_filter(world, type, filter)) {
            continue;
        }

        /* Both filters passed, clear table */
        ecs_table_clear(world, table);
    }
}

void _ecs_add_remove_w_filter(
    ecs_world_t *world,
    ecs_type_t to_add,
    ecs_type_t to_remove,
    ecs_type_filter_t *filter)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_assert(stage == &world->main_stage, ECS_UNSUPPORTED, 
        "remove_w_filter currently only supported on main stage");

    uint32_t i, count = ecs_sparse_count(world->main_stage.tables);

    for (i = 0; i < count; i ++) {
        ecs_table_t *table = ecs_sparse_get(world->main_stage.tables, ecs_table_t, i);
        ecs_type_t type = table->type;

        /* Skip if the type contains none of the components in to_remove */
        if (to_remove) {
            if (!ecs_type_contains(world, type, to_remove, false, false, NULL)) {
                continue;
            }
        }

        /* Skip if the type already contains all of the components in to_add */
        if (to_add) {
            if (ecs_type_contains(world, type, to_add, true, false, NULL)) {
                continue;
            }            
        }

        if (!ecs_type_match_w_filter(world, type, filter)) {
            continue;
        }

        /* Component(s) must be removed, find table */
        ecs_type_t dst_type = ecs_type_merge(world, type, to_add, to_remove);
        if (!dst_type) {
            /* If this removes all components, clear table */
            ecs_columns_merge(world, NULL, table);
        } else {
            ecs_entity_array_t entities = {
                .array = ecs_vector_first(dst_type),
                .count = ecs_vector_count(dst_type)
            };
            ecs_table_t *dst_table = ecs_table_find_or_create(world, stage, &entities);
            ecs_assert(dst_table != NULL, ECS_INTERNAL_ERROR, NULL);

            /* Merge table into dst_table */
            ecs_columns_merge(world, dst_table, table);
        }
    }    
}

/* -- Public functions -- */

static
ecs_entity_t new_entity_handle(
    ecs_world_t *world)
{
    ecs_entity_t entity = ++ world->last_handle;
    ecs_assert(!world->max_handle || entity <= world->max_handle, 
        ECS_OUT_OF_RANGE, NULL);
    return entity;
}

static
void new_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_entity_array_t *to_add)
{
    ecs_entity_array_t added = {
        .array = ecs_os_alloca(ecs_entity_t, to_add->count)
    };
    ecs_entity_info_t info = {0};
    ecs_table_t *table = ecs_table_traverse(
        world, stage, &world->main_stage.table_root, to_add, NULL, &added, NULL);
    new_entity(world, stage, entity, &info, table, &added);
}

ecs_entity_t _ecs_new(
    ecs_world_t *world,
    ecs_type_t type)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);    
    ecs_entity_t entity = new_entity_handle(world);

    if (type) {
        ecs_entity_array_t to_add = {
            .array = ecs_vector_first(type),
            .count = ecs_vector_count(type)
        };

        new_intern(world, stage, entity, &to_add);
    }

    return entity;
}

/* Test if any components are provided that do not have a corresponding data
 * array */
static
bool has_unset_columns(
    ecs_type_t type,
    ecs_columns_t *columns,
    ecs_table_data_t *data)
{
    if (!data->columns) {
        return true;
    }


    ecs_column_t *components = columns->components;
    uint32_t i;
    for (i = 0; i < data->column_count; i ++) {
        /* If no column is provided for component, skip it */
        ecs_entity_t component = data->components[i];
        if (component & ECS_ENTITY_FLAGS_MASK) {
            /* If this is a base or parent, don't copy anything */
            continue;
        }

        int32_t column = ecs_type_index_of(type, component);
        ecs_assert(column >= 0, ECS_INTERNAL_ERROR, NULL);
        
        uint32_t size = components[column].size;
        if (size) { 
            if (!data->columns[i]) {
                return true;
            }
        }
    }    

    return false;
}

static
void copy_column_data(
    ecs_type_t type,
    ecs_columns_t *columns,
    uint32_t start_row,
    ecs_table_data_t *data)
{    
    ecs_column_t *components = columns->components;
    uint32_t i;
    for (i = 0; i < data->column_count; i ++) {
        /* If no column is provided for component, skip it */
        if (!data->columns[i]) {
            continue;
        }

        ecs_entity_t component = data->components[i];
        if (component & ECS_ENTITY_FLAGS_MASK) {
            /* If this is a base or parent, don't copy anything */
            continue;
        }

        int32_t column = ecs_type_index_of(type, component);
        ecs_assert(column >= 0, ECS_INTERNAL_ERROR, NULL);

        uint32_t size = components[column].size;
        if (size) { 
            void *column_data = ecs_vector_first(components[column].data);

            memcpy(
                ECS_OFFSET(column_data, (start_row) * size),
                data->columns[i],
                data->row_count * size
            );
        }
    }
}

static
void invoke_reactive_systems(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t src_type,
    ecs_type_t dst_type,
    ecs_table_t *src_table,
    ecs_columns_t *src_columns,
    ecs_table_t *dst_table,
    ecs_columns_t *dst_columns,
    uint32_t src_index,
    uint32_t dst_index,
    uint32_t count,
    bool do_set)
{
    ecs_type_t to_remove = NULL; 
    ecs_type_t to_add = NULL;
    
    if (src_type) {
        to_remove = ecs_type_merge_intern(world, stage, src_type, NULL, dst_type, NULL, NULL);
    } else {
        to_add = dst_type;
    }

    if (dst_type) {
        to_add = ecs_type_merge_intern(world, stage, dst_type, NULL, src_type, NULL, NULL);
    } else {
        to_remove = src_type;
    }

    /* Invoke OnRemove systems */
    if (to_remove) {
        ecs_entity_array_t to_remove_array = {
            .array = ecs_vector_first(to_remove),
            .count = ecs_vector_count(to_remove)
        };

        run_component_actions(
            world,
            stage,
            src_table,
            src_columns,
            src_index,
            count,
            to_remove_array,
            false
        );
    }

    if (src_type && src_type != dst_type) {
        /* Delete column from old table. Delete in reverse, as entity indexes of
         * entities after the deletion point change as a result of the delete. */
         
        uint32_t i;
        for (i = 0; i < count; i ++) {
            ecs_columns_delete(
                world, stage, src_table, src_columns, src_index + count - i - 1);
        }
    }

    /* Invoke OnAdd systems */
    if (to_add) {
        ecs_entity_array_t to_add_array = {
            .array = ecs_vector_first(to_add),
            .count = ecs_vector_count(to_add)
        };

        run_component_actions(
            world,
            stage,
            dst_table,
            dst_columns,
            dst_index,
            count,
            to_add_array,
            true
        );
    }
}

static
uint32_t update_entity_index(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t type,
    ecs_table_t *table,
    ecs_columns_t *columns,
    ecs_entity_t start_entity,
    ecs_table_data_t *data)
{
    bool has_unset = false, tested_for_unset = false;
    uint32_t i, start_row = 0, dst_start_row = 0;
    uint32_t count = data->row_count;
    ecs_entity_t *entities = ecs_vector_first(columns->entities);
    ecs_record_t **record_ptrs = ecs_vector_first(columns->record_ptrs);
    uint32_t row_count = ecs_columns_count(columns);

    /* If 'entities' is set, 'record_ptrs' should also be set */
    ecs_assert(record_ptrs != NULL || !entities, ECS_INTERNAL_ERROR, NULL);

    /* While we're updating the entity index we may need to invoke reactive
     * systems (OnRemove, OnAdd) in case the origin of the entities is not the
     * same. The only moment in time we know the previous type and the new type
     * of the entity is in this function, which is why those systems need to be
     * invoked here.
     *
     * These variables help track the largest contiguous subsets of different
     * origins, so that reactive systems can be inovoked on arrays of entities
     * vs individual entities as much as possible. */
    bool same_origin = true;
    ecs_table_t *src_table = NULL, *prev_src_table = NULL;
    ecs_type_t src_type = NULL, prev_src_type = NULL;
    int32_t src_row = 0, prev_src_row = 0, dst_first_contiguous_row = start_row;
    int32_t src_first_contiguous_row = 0;
    ecs_entity_t e;

    /* We need to commit each entity individually in order to populate
     * the entity index */
    for (i = 0; i < count; i ++) {
        int32_t dst_cur_row = dst_start_row + i;

        /* If existing array with entities was provided, use entity ids from
         * that array. Otherwise use new entity id */
        if(data->entities) {
            e = data->entities[i];

            /* If this is not the first entity, check if the next entity in
             * the table is the next entity to set. If so, there is no need 
             * to update the entity index. This is the fast path that is
             * taken if all entities in the table are in the same order as
             * provided in the data argument. */
            if (i) {
                if (entities) {
                    /* Don't read beyond the size of the entity array */
                    if (dst_cur_row < row_count) {
                        if (entities[dst_cur_row] == e) {
                            continue;
                        }
                    }
                }
            }
            
            /* Ensure that the last issued handle will always be ahead of the
             * entities created by this operation */
            if (e > world->last_handle) {
                world->last_handle = e + 1;
            }                            
        } else {
            e = i + start_entity;
        }

        ecs_record_t *record = ecs_get_entity(world, stage, e);
        if (record) {
            src_row = record->row;
            int8_t is_monitored = 1 - (src_row < 0) * 2;
            src_row = (src_row * is_monitored) - 1;

            ecs_assert(src_row >= 0, ECS_INTERNAL_ERROR, NULL);

            prev_src_table = src_table;
            src_table = record->table;
            src_type = src_table->type;

            /* Keep track of whether all entities come from the same origin or
             * not. If they come from the same origin, reactive system(s)
             * can be invoked with all of the entities at the same time. If the
             * existing entities are of different origins however, we try to do
             * the best we can, which is find the largest contiguous subset(s) 
             * of entities from the same origin, and invoke the reactive systems
             * on those subsets. */
            if (!i) {
                /* If this is the first entity, initialize the prev_src_type */
                prev_src_type = src_type;
            }

            /* If this is the first entity and the source type is equal to the
             * destination type, initialize the dst row to the src row. */
            if (!i && src_type == type) {
                dst_start_row = src_row;
                dst_first_contiguous_row = src_row;
            } else {
                /* If the entity exists but it is not the first entity, 
                 * check if it is in the same table. If not, delete the 
                 * entity from the other table */     
                if (src_type != type) {
                    ecs_columns_t *src_columns = ecs_table_get_columns(
                        world, stage, src_table);

                    /* Insert new row into destination table */
                    int32_t dst_row = ecs_columns_insert(
                        world, stage, table, columns, e, record);

                    ecs_assert(dst_row >= 0, ECS_INTERNAL_ERROR, NULL);

                    if (!i) {
                        dst_start_row = dst_row;
                        dst_first_contiguous_row = dst_row;
                        src_first_contiguous_row = src_row;
                    }

                    /* If the data structure has columns that are unset, data
                     * must be copied from the old table to the new table */
                    if (!tested_for_unset) {
                        has_unset = has_unset_columns(
                            type, columns, data);
                    }

                    if (has_unset) {
                        ecs_columns_move(type, columns, dst_row, 
                            src_type, src_columns, record->row - 1);
                    }

                    /* Actual deletion of the entity from the source table
                     * happens after the OnRemove systems are invoked */
                } else {
                    /* If entity exists in the same table but not on the right
                    * index (if so, we would've found out by now) we need to
                    * move things around. This will ensure that entities are
                    * ordered in exactly the same way as they are provided in
                    * the data argument, so that subsequent invocations of this
                    * operation with entities in the same order, 
                    * insertion can be much faster. This also allows the data
                    * from columns in data to be inserted with a single
                    * memcpy per column. */

                    /* If we're not at the top of the table, simply swap the
                     * next entity with the one that we want at this row. */
                    if (row_count > dst_cur_row) {
                        ecs_columns_swap(world, stage, table, columns, 
                            src_row, dst_cur_row, record, NULL);

                    /* We are at the top of the table and the entity is in
                     * the table. This scenario is a bit nasty, since we
                     * need to now move the added entities back one position
                     * and swap the entity preceding them with the current
                     * entity. This should only happen in rare cases, as any
                     * subsequent calls with the same set of entities should
                     * find the entities in the table to be in the right 
                     * order. 
                     * We could just have swapped the order in which the
                     * entities are inserted, but then subsequent calls
                     * would still be expensive, and we couldn't just memcpy
                     * the component data into the columns. */
                    } else {
                        /* First, swap the entity preceding the start of the
                         * added entities with the entity that we want at
                         * the end of the block */
                        ecs_columns_swap(world, stage, table, columns, 
                            src_row, dst_start_row - 1, record, NULL);

                        /* Now move back the whole block back one position, 
                         * while moving the entity before the start to the 
                         * row right after the block */
                        ecs_columns_move_back_and_swap(
                            world, stage, table, columns, dst_start_row, i);

                        dst_start_row --;
                        dst_first_contiguous_row --;

                        ecs_assert(dst_start_row >= 0, ECS_INTERNAL_ERROR, NULL);
                    }
                }
            }

            /* Update entity index with the new table / row */
            record->table = table;
            
            /* Don't use dst_cur_row, as dst_start_row has been updated */
            record->row = dst_start_row + i + 1; 
            record->row *= is_monitored;
        } else {
            ecs_record_t *record = NULL;

            if (stage == &world->main_stage) {
                record = ecs_register_entity(world, e);
                record->table = table,
                record->row = dst_cur_row + 1;
            } else {
                ecs_record_t dst_row = (ecs_record_t){
                    .table = table, .row = dst_cur_row + 1
                };
                ecs_set_entity(world, stage, e, &dst_row);
            }

            if (entities) {
                entities[dst_cur_row] = e;
                record_ptrs[dst_cur_row] = record;
            } else {
                int32_t row = ecs_columns_insert(
                    world, stage, table, columns, e, record);

                ecs_assert(row == dst_cur_row, ECS_INTERNAL_ERROR, NULL);
            }
        }

        /* Now that the entity index is updated for this entity, check if we
         * need to invoke reactive systems for the current set of contiguous
         * entities. */
        if (i) {
            ecs_assert(dst_first_contiguous_row >= dst_start_row, ECS_INTERNAL_ERROR, NULL);

            if (prev_src_type != src_type || (src_row != prev_src_row && prev_src_row != (src_row - 1))) {
                /* If either the previous type is different from the current
                 * type, or the previous index is not one before the current,
                 * entities are not from the same origin or they are not stored
                 * in a contiguous way. If this happens, invoke reactive systems
                 * up to this point. */
                ecs_columns_t *src_columns = NULL;
                
                if (prev_src_table) {
                    src_columns = ecs_table_get_columns(world, stage, prev_src_table);
                }

                invoke_reactive_systems(
                    world,
                    stage,
                    prev_src_type,
                    type,
                    src_table,
                    src_columns,
                    table,
                    columns,
                    src_first_contiguous_row,
                    dst_first_contiguous_row,
                    i - dst_start_row - dst_first_contiguous_row,
                    data->columns == NULL);

                /* Start a new contiguous set */
                dst_first_contiguous_row = dst_start_row + i;
                src_first_contiguous_row = src_row;
                prev_src_type = src_type;
                same_origin = false;
            }            
        }

        prev_src_row = src_row;
    }

    /* Invoke reactive systems on the entities in the last contiguous set. If 
     * all entities are from the same origin, this will cover all entities. */
    ecs_columns_t *src_columns = NULL;
    uint32_t contiguous_count = 0;

    if (same_origin) {
        contiguous_count = count;
    } else {
        contiguous_count = src_row - src_first_contiguous_row + 1;
    }

    if (src_table) {
        src_columns = ecs_table_get_columns(world, stage, src_table);
    }

    invoke_reactive_systems(
        world,
        stage,
        prev_src_type,
        type,
        src_table,
        src_columns,
        table,
        columns,
        src_first_contiguous_row,
        dst_first_contiguous_row,
        contiguous_count,
        data->columns == NULL);

    ecs_assert(dst_start_row >= 0, ECS_INTERNAL_ERROR, NULL);

    return dst_start_row;
}

static
ecs_entity_t set_w_data_intern(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_type_t type,
    ecs_table_data_t *data)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_world_t *world_arg = world;
    ecs_stage_t *stage = ecs_get_stage(&world);
    ecs_entity_t result;
    uint32_t count = data->row_count;
    ecs_entity_t *data_entities = data->entities;

    if (data_entities) {
        /* Start with first provided entity id */
        result = data_entities[0];
    } else {
        /* Start with next available entity handle */
        result = world->last_handle + 1;
        world->last_handle += count;
    }

    ecs_assert(!world->max_handle || world->last_handle <= world->max_handle, 
        ECS_OUT_OF_RANGE, NULL);
    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);

    if (type) {
        ecs_columns_t *columns = ecs_table_get_columns(world, stage, table);
        ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, 0);

        if (!data_entities) {
            /* If no entity identifiers were provided, this operation will
             * create data->row_count new entities. Make space by growing the
             * table and entity index by data->row_count */
            ecs_columns_grow(world, table, columns, data->row_count, result);
            ecs_grow_entities(world, stage, data->row_count);
        }

        /* This is the most complex part of the set_w_data. We need to go from
         * a potentially chaotic state (entities can be anywhere) to a state
         * where all entities are in the same table, in the order specified by
         * the arguments of this function.
         *
         * This function addresses the following cases:
         * - generate new entity ids (when data->entities = NULL)
         * - entities do not yet exist
         * - entities exist, are in the same table, in the same order
         * - entities exist, are in the same table, in a different order
         * - entities exist, are in a different table, in the same order
         * - entities exist, are in a different table, in a different order
         * - entities may exist, and may be in different tables
         *
         * For each of these cases, the proper sequence of OnAdd / OnRemove 
         * systems must be executed.
         */
        int32_t start_row = update_entity_index(
            world, stage, type, table, columns, result, data);    

        /* If columns were provided, copy data from columns into table. This is
         * where a lot of the performance benefits can be achieved: now that all
         * entities are nicely ordered in the destination table, we can copy the
         * data into each column with a single memcpy. */
        if (data->columns) {
            copy_column_data(type, columns, start_row, data);

            uint32_t i, count = data->column_count;
            for (i = 0; i < count; i ++) {
                ecs_entity_t c = data->components[i];
                if (c < ECS_MAX_COMPONENTS) {
                    ecs_component_data_t *cdata = get_component_data(world, c);
                    run_component_set_action(
                        world, cdata, c, table, columns, start_row, 
                        data->row_count);
                }
            }            
        }
    } else {
        /* If not type is provided, nothing needs to be done except for updating
         * the last_handle field */
    }

    return result;
}

ecs_entity_t _ecs_new_w_count(
    ecs_world_t *world,
    ecs_type_t type,
    uint32_t count)
{
    ecs_table_data_t table_data = {
        .row_count = count
    };

    ecs_table_t *table = ecs_type_find_table(world, NULL, type);

    return set_w_data_intern(world, table, type, &table_data);
}

ecs_entity_t ecs_set_w_data(
    ecs_world_t *world,
    ecs_table_data_t *data)
{
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_entity_array_t entities = {
        .array = data->components,
        .count = data->column_count
    };

    ecs_table_t *table = ecs_table_find_or_create(world, stage, &entities);

    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);

    return set_w_data_intern(world, table, table->type, data);
}

ecs_entity_t _ecs_new_child(
    ecs_world_t *world,
    ecs_entity_t parent,
    ecs_type_t type)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);    
    ecs_entity_t entity = new_entity_handle(world);
    ecs_table_t *table = NULL;;
    ecs_entity_array_t entities = {
        .array = ecs_vector_first(type),
        .count = ecs_vector_count(type)
    };    

    if (type) {
        table = ecs_table_find_or_create(world, stage, &entities);
    }

    if (parent) {
        ecs_entity_t parent_mask = parent | ECS_CHILDOF;
        ecs_entity_array_t entities = {
            .array = &parent_mask,
            .count = 1
        };

        table = ecs_table_traverse(
            world, stage, table, &entities, NULL, NULL, NULL);
    }

    if (table) {
        ecs_entity_info_t info = {0};
        new_entity(world, stage, entity, &info, table, &entities);
    }

    return entity;
}

ecs_entity_t _ecs_new_child_w_count(
    ecs_world_t *world,
    ecs_entity_t parent,
    ecs_type_t type,
    uint32_t count)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_type_t full_type = type;
    
    if (parent) {
        full_type = ecs_type_add(world, full_type, parent | ECS_CHILDOF);
    }

    return _ecs_new_w_count(world, full_type, count);
}

ecs_entity_t _ecs_new_instance(
    ecs_world_t *world,
    ecs_entity_t base,
    ecs_type_t type)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_type_t full_type = type;
    
    if (base) {
        full_type = ecs_type_add(world, full_type, base | ECS_INSTANCEOF);
    }

    return _ecs_new(world, full_type);
}

ecs_entity_t _ecs_new_instance_w_count(
    ecs_world_t *world,
    ecs_entity_t base,
    ecs_type_t type,
    uint32_t count)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_type_t full_type = type;
    
    if (base) {
        full_type = ecs_type_add(world, full_type, base | ECS_INSTANCEOF);
    }

    return _ecs_new_w_count(world, full_type, count);
}

void ecs_delete(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(entity != 0, ECS_INVALID_PARAMETER, NULL);

    ecs_record_t *row;
    ecs_stage_t *stage = ecs_get_stage(&world);
    bool in_progress = world->in_progress;

    if (!in_progress) {
        if ((row = ecs_get_entity(world, NULL, entity))) {
            ecs_table_t *table = row->table;
            ecs_entity_array_t removed = {
                .array = ecs_vector_first(table->type),
                .count = ecs_vector_count(table->type)
            };

            delete_entity(world, stage, entity, table, table->columns[0], 
                row->row - 1, &removed);
        }
    } else {
        /* Remove the entity from the staged index. Any added components while
         * in progress will be discarded as a result. */
        ecs_delete_entity(world, stage, entity);
    }
}

static
ecs_entity_t copy_from_stage(
    ecs_world_t *world,
    ecs_stage_t *src_stage,
    ecs_entity_t src_entity,
    ecs_entity_t dst_entity,
    bool copy_value)
{
    if (!src_entity) {
        return 0;
    }

    ecs_world_t *world_arg = world;
    ecs_stage_t *stage = ecs_get_stage(&world);    

    ecs_entity_info_t src_info = {.entity = src_entity};

    if (populate_info(world, src_stage, &src_info)) {
        ecs_assert(!dst_entity, ECS_INTERNAL_ERROR, NULL);

        dst_entity = ++ world->last_handle;

        ecs_entity_info_t info = {
            .entity = dst_entity
        };

        ecs_entity_array_t to_add = {
            .array = ecs_vector_first(src_info.type),
            .count = ecs_vector_count(src_info.type)
        };

        commit(world, stage, &info, &to_add, NULL, false);

        if (copy_value) {
            ecs_columns_move(info.type, info.columns, info.row,
                src_info.type, src_info.columns, src_info.row);
        }
    }

    return dst_entity;
}

ecs_entity_t ecs_clone(
    ecs_world_t *world,
    ecs_entity_t entity,
    bool copy_value)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);

    ecs_entity_t result = copy_from_stage(world, &world->main_stage, entity, 0, copy_value);

    if (stage != &world->main_stage) {
        result = copy_from_stage(world, stage, entity, result, copy_value);
    }

    if (!result) {
        result = ++ world->last_handle;
    }

    return result;
}

void _ecs_add(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    ecs_entity_info_t info = {.entity = entity};
    ecs_entity_array_t to_add = {
        .array = ecs_vector_first(type),
        .count = ecs_vector_count(type)
    };
    ecs_add_remove_intern(world, &info, &to_add, NULL, true);
}

void _ecs_remove(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    ecs_entity_info_t info = {.entity = entity};
    ecs_entity_array_t to_remove = {
        .array = ecs_vector_first(type),
        .count = ecs_vector_count(type)
    };
    ecs_add_remove_intern(world, &info, NULL, &to_remove, false);
}

void _ecs_add_remove(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t add_type,
    ecs_type_t remove_type)
{
    ecs_entity_info_t info = {.entity = entity};
    ecs_entity_array_t to_add = {
        .array = ecs_vector_first(add_type),
        .count = ecs_vector_count(add_type)
    };      
    ecs_entity_array_t to_remove = {
        .array = ecs_vector_first(remove_type),
        .count = ecs_vector_count(remove_type)
    };      
    ecs_add_remove_intern(world, &info, &to_add, &to_remove, false);
}

void ecs_adopt(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t parent)
{    
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);
    
    ecs_type_t add_type = ecs_type_find(
        world, 
        &(ecs_entity_t){parent | ECS_CHILDOF},
        1);

    _ecs_add_remove(world, entity, add_type, 0);
}

void ecs_orphan(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t parent)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_type_t remove_type = ecs_type_find(
        world, 
        &(ecs_entity_t){parent | ECS_CHILDOF},
        1);

    _ecs_add_remove(world, entity, 0, remove_type);
}

void ecs_inherit(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t base)
{    
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);
    
    ecs_type_t add_type = ecs_type_find(
        world, 
        &(ecs_entity_t){base | ECS_INSTANCEOF},
        1);

    _ecs_add_remove(world, entity, add_type, 0);
}

void ecs_disinherit(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t base)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_type_t remove_type = ecs_type_find(
        world, 
        &(ecs_entity_t){base | ECS_INSTANCEOF},
        1);

    _ecs_add_remove(world, entity, 0, remove_type);
}

void* _ecs_get_ptr(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_world_t *world_arg = world;
    ecs_stage_t *stage = ecs_get_stage(&world);

    /* Get only accepts types that hold a single component */
    ecs_entity_t component = ecs_type_to_entity(world_arg, type);

    ecs_entity_info_t info = {.entity = entity};
    return ecs_get_ptr_intern(world, stage, &info, component, false, true);
}

ecs_entity_t _ecs_set_ptr(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t component,
    size_t size,
    void *ptr)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(component != 0, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(!size || ptr != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_world_t *world_arg = world;
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_entity_info_t info = {.entity = entity};

    /* If no entity is specified, create one */
    if (!entity) {
        ecs_entity_array_t to_add = {
            .array = (ecs_entity_t[]){component},
            .count = 1
        };

        entity = new_entity_handle(world);
        new_intern(world, stage, entity, &to_add);
        info.entity = entity;
    }
    
    void *dst = NULL;
    if (get_info(world, stage, &info)) {
        dst = get_row_ptr(info.type, info.columns, info.row, component);
    }

    if (!dst) {
        ecs_entity_array_t to_add = {
            .array = (ecs_entity_t[]){component},
            .count = 1
        };
            
        commit(world, stage, &info, &to_add, NULL, false);

        dst = get_row_ptr(info.type, info.columns, info.row, component);
        if (!dst) {
            return entity;
        }
    }

    if (ptr) {
        memcpy(dst, ptr, size);
    } else {
        memset(dst, 0, size);
    }

    ecs_component_data_t *cdata = get_component_data(world, component);    
    run_component_set_action(world, cdata, component, 
        info.table, info.columns, info.row, 1);

    return entity;
}

ecs_entity_t _ecs_set_singleton_ptr(
    ecs_world_t *world,
    ecs_entity_t component,
    size_t size,
    void *ptr)
{
    return _ecs_set_ptr(world, ECS_SINGLETON, component, size, ptr);
}

static
bool ecs_has_intern(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type,
    bool match_any,
    bool match_prefabs)    
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    if (!entity) {
        return false;
    }

    if (!type) {
        return true;
    }

    ecs_world_t *world_arg = world;
    ecs_type_t entity_type = ecs_get_type(world_arg, entity);
    return ecs_type_contains(world, entity_type, type, match_any, match_prefabs, NULL);
}

bool _ecs_has(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    return ecs_has_intern(world, entity, type, true, true) != 0;
}

bool _ecs_has_owned(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    return ecs_has_intern(world, entity, type, true, false) != 0;
}

bool _ecs_has_any(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    return ecs_has_intern(world, entity, type, false, true);
}

bool _ecs_has_any_owned(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    return ecs_has_intern(world, entity, type, false, false);
} 

bool ecs_has_entity(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t component)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    if (!entity) {
        return false;
    }

    if (!component) {
        return true;
    }

    ecs_world_t *world_arg = world;
    ecs_type_t entity_type = ecs_get_type(world_arg, entity);
    return ecs_type_has_entity(world, entity_type, component);
}

bool ecs_contains(
    ecs_world_t *world,
    ecs_entity_t parent,
    ecs_entity_t child)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    if (!parent || !child) {
        return false;
    }

    ecs_type_t child_type = ecs_get_type(world, child);

    return ecs_type_has_entity_intern(
        world, child_type, parent | ECS_CHILDOF, false);
}

ecs_entity_t _ecs_get_parent(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t component)
{
    ecs_entity_t parent = 0;
    ecs_type_t type = ecs_get_type(world, entity);
    
    ecs_components_contains_component(
        world, type, component, 0, &parent);

    return parent;
}

const char* ecs_get_id(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    if (entity == ECS_SINGLETON) {
        return "$";
    }

    EcsId *id = ecs_get_ptr(world, entity, EcsId);
    if (id) {
        return *id;
    } else {
        return NULL;
    }
}

bool ecs_is_empty(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    return ecs_get_type(world, entity) == NULL;
}

ecs_type_t ecs_type_from_entity(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    if (entity == 0) {
        return NULL;
    }

    ecs_stage_t *stage = ecs_get_stage(&world);   
    ecs_record_t *row = ecs_get_entity(world, NULL, entity);

    if (!row || !row->row) {
        if (world->in_progress) {
            row = ecs_get_entity(world, stage, entity);
        }
    }

    uint32_t index;
    ecs_type_t type = 0;
    ecs_entity_t component = 0;
    ecs_columns_t *columns = NULL;

    if (row && (index = row->row)) {
        index --;

        ecs_table_t *table = row->table;

        if (table) {
            ecs_entity_t *components = ecs_vector_first(table->type);
            columns = ecs_table_get_columns(world, stage, table);
            component = components[0];
        }
    }

    if (component == EEcsTypeComponent) {
        EcsTypeComponent *fe = ecs_vector_get(columns->components[0].data, EcsTypeComponent, index);
        type = fe->normalized;
    } else {
        ecs_table_t *table = ecs_table_find_or_create(world, NULL, 
            &(ecs_entity_array_t){
                .array = &entity,
                .count = 1
            }
        );

        ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);

        type = table->type;
    }

    return type;
}

ecs_entity_t ecs_type_to_entity(
    ecs_world_t *world, 
    ecs_type_t type)
{
    (void)world;

    if (!type) {
        return 0;
    }
    
    /* If array contains n entities, it cannot be reduced to a single entity */
    if (ecs_vector_count(type) != 1) {
        ecs_abort(ECS_TYPE_NOT_AN_ENTITY, NULL);
    }

    return ((ecs_entity_t*)ecs_vector_first(type))[0];
}

ecs_type_t ecs_get_type(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_type_t result = NULL;
    ecs_record_t *row = ecs_get_entity(world, stage, entity);

    if (row) {
        ecs_table_t *table = row->table;
        if (table) {
            result = table->type;
        }
    }

    if (world->in_progress) {
        ecs_record_t *main_row = ecs_get_entity(world, NULL, entity);

        if (main_row && main_row->table) {
            result = ecs_type_merge_intern(
                world, stage, main_row->table->type, result, 
                NULL, NULL, NULL);
        }
    }
    
    return result;
}

uint32_t ecs_count_w_filter(
    ecs_world_t *world,
    ecs_type_filter_t *filter)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_sparse_t *tables = world->main_stage.tables;
    uint32_t i, count = ecs_sparse_count(tables);
    uint32_t result = 0;

    for (i = 0; i < count; i ++) {
        ecs_table_t *table = ecs_sparse_get(tables, ecs_table_t, i);

        if (ecs_type_match_w_filter(world, table->type, filter)) {
            ecs_columns_t *columns = table->columns[0];
            if (columns) {
                result += ecs_columns_count(columns);
            }
        }
    }
    
    return result; 
}

uint32_t _ecs_count(
    ecs_world_t *world,
    ecs_type_t type)
{
    return ecs_count_w_filter(world, &(ecs_type_filter_t){.include = type});
}

ecs_entity_t ecs_new_component(
    ecs_world_t *world,
    const char *id,
    size_t size)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    assert(world->magic == ECS_WORLD_MAGIC);

    ecs_entity_t result = ecs_lookup(world, id);
    if (result) {
        return result;
    }

    result = world->last_component ++;
    
    _ecs_add(world, result, world->t_component->type);
    ecs_set(world, result, EcsComponent, {.size = size});
    ecs_set(world, result, EcsId, {id});

    return result;
}
