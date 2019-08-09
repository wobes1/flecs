
#include "flecs_private.h"

static
void copy_column(
    ecs_column_t *new_column,
    int32_t new_index,
    ecs_column_t *old_column,
    int32_t old_index)
{
    ecs_assert(new_index > 0, ECS_INTERNAL_ERROR, NULL);

    uint32_t size = new_column->size;

    if (size) {
        ecs_vector_params_t param = {.element_size = new_column->size};

        if (old_index < 0) old_index *= -1;
        
        void *dst = ecs_vector_get(new_column->data, &param, new_index - 1);
        void *src = ecs_vector_get(old_column->data, &param, old_index - 1);
            
        ecs_assert(dst != NULL, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);

        memcpy(dst, src, param.element_size);
    }
}

static
void copy_row(
    ecs_type_t new_type,
    ecs_column_t *new_columns,
    int32_t new_index,
    ecs_type_t old_type,
    ecs_column_t *old_columns,
    int32_t old_index)
{
    uint16_t i_new, new_component_count = ecs_vector_count(new_type);
    uint16_t i_old = 0, old_component_count = ecs_vector_count(old_type);
    ecs_entity_t *new_components = ecs_vector_first(new_type);
    ecs_entity_t *old_components = ecs_vector_first(old_type);

    ecs_assert(new_index >= 0, ECS_INTERNAL_ERROR, NULL);

    ecs_assert(old_columns->data != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(new_columns->data != NULL, ECS_INTERNAL_ERROR, NULL);

    for (i_new = 0; i_new < new_component_count; ) {
        if (i_old == old_component_count) {
            break;
        }

        ecs_entity_t new_component = new_components[i_new];
        ecs_entity_t old_component = old_components[i_old];

        if ((new_component & ECS_ENTITY_FLAGS_MASK) || 
            (old_component & ECS_ENTITY_FLAGS_MASK)) 
        {
            break;
        }

        if (new_component == old_component) {
            copy_column(&new_columns[i_new + 1], new_index, &old_columns[i_old + 1], old_index);
            i_new ++;
            i_old ++;
        } else if (new_component < old_component) {
            i_new ++;
        } else if (new_component > old_component) {
            i_old ++;
        }
    }
}

static
void* get_row_ptr(
    ecs_type_t type,
    ecs_column_t *columns,
    int32_t index,
    ecs_entity_t component)
{
    ecs_assert(ecs_vector_count(type) < ECS_MAX_ENTITIES_IN_TYPE, ECS_TYPE_TOO_LARGE, NULL);

    int16_t column_index = ecs_type_index_of(type, component);
    if (column_index == -1) {
        return NULL;
    }

    ecs_assert(index >= 0, ECS_INTERNAL_ERROR, NULL);

    ecs_column_t *column = &columns[column_index + 1];
    ecs_vector_params_t param = {.element_size = column->size};

    if (param.element_size) {
        ecs_assert(column->data != NULL, ECS_INTERNAL_ERROR, NULL);

        void *ptr = ecs_vector_get(column->data, &param, index - 1);
        return ptr;
    } else {
        return NULL;
    }
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

void ecs_set_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t *row)
{
    ecs_assert(world != NULL, ECS_INTERNAL_ERROR, NULL);

    if (!stage || stage == &world->main_stage) {
        if (entity == ECS_SINGLETON) {
            world->singleton = *row;
        } else {
            ecs_record_t *new_row = ecs_sparse_get_or_set_sparse(
                world->entity_index, ecs_record_t, entity, NULL);
            *new_row = *row;
        }
    } else {
        ecs_map_set(stage->entity_index, entity, row);
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

static
ecs_column_t* get_columns(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table)
{
    if (!stage || stage == &world->main_stage) {
        ecs_column_t *result = table->columns;
        if (!result) {
            result = table->columns = ecs_columns_new(world, NULL, table);
        }
        return result;
    } else {
        return ecs_map_get_ptr(stage->data_stage, ecs_column_t*, (uintptr_t)table->type);
    }    
}

static
void update_info(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t *record,
    ecs_entity_info_t *info)
{
    /* Retrieve table, type and data columns */
    ecs_table_t *table;
    if ((table = record->table)) {
        ecs_type_t type = table->type;

        info->table = table;
        info->type = type;

        if (stage == &world->main_stage) {
            /* Store record so entity can be updated efficiently later. This can
            * only be done for records in the main stage, as they use a sparse
            * set. Staged records are in a map, and pointers to map elements
            * are not stable. */
            info->record = record;
            info->columns = table->columns;
        } else {
            info->columns = ecs_map_get_ptr(
                stage->data_stage, ecs_column_t*, (uintptr_t)type);
        }

        ecs_assert(type != NULL, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(info->columns != NULL, ECS_INTERNAL_ERROR, NULL);
    }

    /* If index is negative, this entity is being watched */
    int32_t row = record->row;
    if (row > 0) {
        info->row = row;
        info->is_watched = false;
    } else {
        info->row = -row;
        info->is_watched = true;
    }
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
    ecs_column_t *prefab_columns = prefab_info->columns;

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
            &entity_builder->ops, &builder_params, count);
        
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
ecs_type_t copy_from_prefab(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *prefab_info,
    ecs_entity_info_t *info,
    uint32_t offset,
    uint32_t limit,
    ecs_type_t to_add,
    ecs_type_t modified)
{
    ecs_type_t prefab_type = prefab_info->type;
    ecs_column_t *prefab_columns = prefab_info->columns;
    ecs_entity_t prefab = prefab_info->entity;
    int32_t prefab_index = prefab_info->row;

    ecs_assert(prefab_index != -1, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(info->row != -1, ECS_INTERNAL_ERROR, NULL);

    uint32_t e, add_count = ecs_vector_count(to_add);
    uint32_t p = 0, prefab_count = ecs_vector_count(prefab_type);
    ecs_entity_t *to_add_buffer = ecs_vector_first(to_add);
    ecs_entity_t *prefab_type_buffer = ecs_vector_first(prefab_type);

    bool is_prefab = false;
    ecs_column_t *columns = info->columns;

    for (e = 0; e < add_count; e ++) {
        ecs_entity_t pe = 0, ee = to_add_buffer[e] & ECS_ENTITY_MASK;

        /* Keep track of whether this entity became a prefab */
        if (ee == EEcsPrefab) {
            is_prefab = true;
            continue;
        }

        /* Never copy EcsId and EcsPrefabBuilder components from base */
        if (ee == EEcsId || ee == EEcsPrefabBuilder) {
            continue;
        }

        /* If added entity is an instance of base, instantiate it */
        if (ee == prefab) {
            modified = instantiate_prefab(world, stage, info->entity, is_prefab, 
                prefab_info, limit, modified);
            continue;
        }

        /* Find the corresponding component in the base type */
        while ((p < prefab_count) && (pe = prefab_type_buffer[p]) < ee) {
            p ++;
        }

        /* If base hasn't been found, continue */
        if (ee != pe) {
            continue;
        }

        ecs_column_t *src_column = &prefab_columns[p + 1];
        uint32_t size = src_column->size;

        if (size) {
            void *src_column_data = ecs_vector_first(src_column->data);
            void *src_ptr = ECS_OFFSET(
                src_column_data, size * (prefab_index - 1));

            uint32_t dst_col_index;
            if (info->type == to_add) {
                dst_col_index = e;
            } else {
                dst_col_index = ecs_type_index_of(info->type, ee);
            }
            
            ecs_column_t *dst_column = &columns[dst_col_index + 1];
            void *dst_column_data = ecs_vector_first(dst_column->data);
            void *dst_ptr = ECS_OFFSET(
                dst_column_data, size * (info->row - 1 + offset));

            uint32_t i;
            for (i = 0; i < limit; i ++) {
                memcpy(dst_ptr, src_ptr, size);
                dst_ptr = ECS_OFFSET(dst_ptr, size);
            }
        }
    }

    if (modified) {
        /* Always strip EcsPrefab, as an entity will never inherit the EcsPrefab
         * component from a prefab. Same for EcsId. */
        modified = ecs_type_merge_intern(world, stage, modified, 0, ecs_type(EcsPrefab), NULL, NULL);
        modified = ecs_type_merge_intern(world, stage, modified, 0, ecs_type(EcsId), NULL, NULL);
    }    

    return modified;
}

static
ecs_type_t copy_from_prefabs(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info,
    uint32_t offset,
    uint32_t limit,
    ecs_type_t to_add,
    ecs_type_t modified)
{
    /* Use get_type, so we have the combined staged/unstaged entity type */
    ecs_type_t type = ecs_get_type(world, info->entity);
    ecs_entity_t *type_buffer = ecs_vector_first(type);
    int32_t i = -1;

    while ((i = ecs_type_get_prefab(type, i)) != -1) {
        ecs_entity_t prefab = type_buffer[i] & ECS_ENTITY_MASK;
        ecs_entity_info_t prefab_info = {.entity = prefab};

        if (populate_info(world, &world->main_stage, &prefab_info)) {
            modified = copy_from_prefab(
                world, stage, &prefab_info, info, offset, limit, to_add, 
                modified);                
        }
    }

    return modified;
}

static
void run_component_actions(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_column_t *columns,
    uint32_t row,
    ecs_entity_array_t components,
    bool is_init)
{
    ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, NULL);

    /* Array that contains component callbacks & systems */
    ecs_component_data_t* cdata_array = ecs_vector_first(world->component_data);
    ecs_type_t type = NULL;
    uint32_t type_count;
    ecs_entity_t *type_array;

    int i, cur = 0;
    for (i = 0; i < components.count; i ++) {
        /* Retrieve component callbacks & systems for component */
        ecs_entity_t component = components.array[i];
        ecs_component_data_t *cdata = &cdata_array[component];
        ecs_vector_t *systems;
        ecs_init_t init = NULL;
        ecs_init_t fini = NULL;
        void *ctx = cdata->ctx;
        
        if (is_init) {
            init = cdata->init;
            systems = cdata->on_add;
            ecs_assert(init || !systems, ECS_UNINITIALIZED_READ, NULL);
            if (!init && !systems) {
                continue;
            }
        } else {
            fini = cdata->fini;
            systems = cdata->on_remove;
            if (!fini && !systems) {
                continue;
            }
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
        ecs_column_t *column = &columns[cur + 1];
        void *array = ecs_vector_first(column->data);
        void *ptr = ECS_OFFSET(array, column->size * row);

        if (init) {
            init(ptr, ctx);
        }

        /* Run systems */
        if (systems) {
            uint32_t sys_count = ecs_vector_count(systems);
            ecs_entity_t *sys_array = ecs_vector_first(systems);

            int s;
            for (s = 0; s < sys_count; s++) {
                /*ecs_run_row_system(
                    world, stage, sys_array[s], column, row, 1);*/
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
    ecs_record_t *record,
    ecs_table_t *new_table,
    ecs_entity_array_t *added)
{
    ecs_column_t *new_columns = get_columns(world, stage, new_table);
    int32_t new_row = ecs_columns_insert(world, new_table, new_columns, entity);

    if (record) {
        record->table = new_table;
        record->row = new_row;
    } else {
        ecs_set_entity(world, stage, entity, &(ecs_record_t){
            .table = new_table,
            .row = new_row
        });
    }

    if (added) {
        run_component_actions(
            world, stage, new_table, new_columns, new_row, *added, true);
    }

    return new_row;
}

static
uint32_t move_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t *record,
    ecs_table_t *old_table,
    ecs_column_t *old_columns,
    int32_t old_row,
    ecs_table_t *new_table,
    ecs_entity_array_t *added,
    ecs_entity_array_t *removed)
{
    ecs_column_t *new_columns = get_columns(world, stage, new_table);
    int32_t new_row = ecs_columns_insert(world, new_table, new_columns, entity);

    copy_row(new_table->type, new_columns, new_row, 
        old_table->type, old_columns, old_row);

    if (removed) {
        run_component_actions(
            world, stage, old_table, old_columns, old_row, *removed, false);
    }

    ecs_columns_delete(world, stage, old_table, old_columns, old_row);

    if (record) {
        record->table = new_table;
        record->row = new_row;
    } else {
        ecs_set_entity(world, stage, entity, &(ecs_record_t){
            .table = new_table,
            .row = new_row
        });
    }

    if (added) {
        run_component_actions(
            world, stage, new_table, new_columns, new_row, *added, true);
    }

    return new_row;
}

static
void delete_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_table_t *old_table,
    ecs_column_t *old_columns,
    int32_t old_row,
    ecs_entity_array_t *removed)
{
    if (removed) {
        run_component_actions(
            world, stage, old_table, old_columns, old_row, *removed, false);
    }

    ecs_columns_delete(world, stage, old_table, old_columns, old_row);
    ecs_delete_entity(world, stage, entity);
}

/** Commit an entity with a specified type to a table (probably the most 
 * important function in flecs). */
static
uint32_t commit(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_info_t *info,
    ecs_type_t to_add,
    ecs_type_t to_remove,
    bool do_set)
{
    ecs_table_t *new_table = NULL, *old_table;
    int32_t new_row = 0;
    bool in_progress = world->in_progress;
    ecs_entity_t entity = info->entity;
    ecs_type_t last_remove_type = NULL;

    if (in_progress) {
        ecs_map_t *remove_merge_map = stage->remove_merge;

        ecs_type_t *rm_type_ptr = ecs_map_get(
                remove_merge_map, ecs_type_t, entity);
        last_remove_type = rm_type_ptr ? *rm_type_ptr : NULL;
        
        ecs_type_t remove_merge = ecs_type_merge(
            world, last_remove_type, to_remove, to_add);
        
        if (!remove_merge && rm_type_ptr) {
            ecs_map_remove(remove_merge_map, entity);
        } else if (rm_type_ptr) {
            *rm_type_ptr = remove_merge;
        } else {
            ecs_map_set(remove_merge_map, entity, &remove_merge);
        }
    }

    /* Keep track of components that are actually added/removed */
    ecs_entity_array_t to_add_array, to_remove_array, added, removed;
    ecs_entity_array_t *to_add_ptr = NULL, *to_remove_ptr = NULL;
    ecs_entity_array_t *added_ptr = NULL, *removed_ptr = NULL;

    if (to_add) {
        int32_t to_add_count = ecs_vector_count(to_add);

        to_add_array = (ecs_entity_array_t){
            .array = ecs_vector_first(to_add),
            .count = to_add_count
        };

        to_add_ptr = &to_add_array;
        added_ptr = &added;

        added.array = ecs_os_alloca(ecs_entity_t, to_add_count);
        added.count = 0;
    }

    /* Find the new table */
    old_table = info->table;
    if (old_table) {
        if (to_remove) {
            int32_t to_remove_count = ecs_vector_count(to_remove);

            to_remove_array = (ecs_entity_array_t){
                .array = ecs_vector_first(to_remove),
                .count = to_remove_count
            };

            to_remove_ptr = &to_remove_array;
            removed_ptr = &removed;            

            removed.array = ecs_os_alloca(ecs_entity_t, to_remove_count);
            removed.count = 0;

            new_table = ecs_table_traverse(
                world, stage, old_table, to_add_ptr, to_remove_ptr, &added, &removed);
        } else {
            new_table = ecs_table_traverse(
                world, stage, old_table, to_add_ptr, NULL, &added, NULL);
        }

        if (new_table) {
            new_row = move_entity(
                world, stage, entity, info->record, old_table, info->columns, info->row, 
                new_table, added_ptr, removed_ptr);
        } else {
            delete_entity(
                world, stage, entity, old_table, info->columns, info->row, 
                removed_ptr);
        }            
    } else {
        new_table = ecs_table_find_or_create(world, stage, to_add_ptr);

        if (new_table) {
            new_row = new_entity(world, stage, entity, info->record, new_table, added_ptr);
        }        
    }

    if (!in_progress) {
        /* Entity ranges are only checked when not iterating. It is allowed to
         * modify entities that existed before setting the range, and thus the
         * range checks are only applied if the old_table is NULL, meaning the
         * entity did not yet exist/was empty. When iterating, old_table refers
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

    return new_row;
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
    ecs_entity_t entity = info->entity;
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

    if (ptr && world->in_progress) {
        ecs_type_t to_remove;
        if ((to_remove = ecs_map_get_ptr(stage->remove_merge, ecs_type_t, entity))) {
            if (ecs_type_has_entity_intern(
                world, to_remove, component, false)) 
            {
                ptr = NULL;
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

ecs_type_t ecs_notify(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_map_t *index,
    ecs_type_t type,
    ecs_table_t *table,
    ecs_column_t *table_columns,
    int32_t offset,
    int32_t limit)
{
    ecs_vector_t *systems;
    ecs_type_t modified = 0;

    if ((systems = ecs_map_get_ptr(index, ecs_vector_t*, (uintptr_t)type))) {
        ecs_entity_t *buffer = ecs_vector_first(systems);
        uint32_t i, count = ecs_vector_count(systems);

        for (i = 0; i < count; i ++) {
            ecs_type_t m = ecs_notify_row_system(
                world, buffer[i], table->type, table, table_columns, offset, 
                limit);
            
            if (i) {
                modified = ecs_type_merge_intern(world, stage, modified, m, 0, NULL, NULL);
            } else {
                modified = m;
            }
        }
    }

    return modified;
}

void ecs_merge_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t staged_row)
{

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

void ecs_add_remove_intern(
    ecs_world_t *world,
    ecs_entity_info_t *info,
    ecs_type_t to_add,
    ecs_type_t to_remove,
    bool do_set)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);
    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);

    if (stage == &world->main_stage) {
        ecs_entity_t entity = info->entity;

        bool is_new = false;
        ecs_record_t *r = ecs_sparse_get_or_set_sparse(
            world->entity_index, ecs_record_t, entity, &is_new);
        if (is_new) {
            r->table = NULL;
            r->row = 0;
        }

        update_info(world, stage, entity, r, info);
        info->record = r;
    } else {
        populate_info(world, stage, info);
    }

    commit(world, stage, info, to_add, to_remove, do_set);
}

/* -- Public functions -- */

ecs_entity_t _ecs_new(
    ecs_world_t *world,
    ecs_type_t type)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_entity_t entity = ++ world->last_handle;
    ecs_assert(!world->max_handle || entity <= world->max_handle, 
        ECS_OUT_OF_RANGE, NULL);

    if (type) {
        ecs_entity_array_t entities = {
            .array = ecs_vector_first(type),
            .count = ecs_vector_count(type)
        };

        ecs_table_t *table = ecs_table_find_or_create(world, stage, &entities);

        new_entity(world, stage, entity, NULL, table, &entities);
    }

    return entity;
}

ecs_entity_t _ecs_new_w_count(
    ecs_world_t *world,
    ecs_type_t type,
    uint32_t count)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_world_t *world_arg = world;
    ecs_stage_t *stage = ecs_get_stage(&world);
    ecs_entity_t result = world->last_handle + 1;
    
    world->last_handle += count;

    ecs_assert(!world->max_handle || world->last_handle <= world->max_handle, 
        ECS_OUT_OF_RANGE, NULL);
    ecs_assert(!world->is_merging, ECS_INVALID_WHILE_MERGING, NULL);

    if (type) {
        ecs_entity_array_t entities = {
            .array = ecs_vector_first(type),
            .count = ecs_vector_count(type)
        };
        ecs_table_t *table = ecs_table_find_or_create(world, stage, &entities);
        ecs_column_t *columns = get_columns(world, stage, table);
        ecs_assert(columns != NULL, ECS_INTERNAL_ERROR, 0);

        uint32_t row = ecs_columns_grow(world, table, columns, count, result);
        ecs_grow_entities(world, stage, count);

        uint64_t i, cur_row = row;
        for (i = result; i < (result + count); i ++) {
            /* We need to commit each entity individually in order to populate
             * the entity index */

            ecs_record_t new_row = (ecs_record_t){.table = table, .row = cur_row};
            ecs_set_entity(world, stage, i, &new_row);

            cur_row ++;
        }
    }

    return result;
}

ecs_entity_t _ecs_new_child(
    ecs_world_t *world,
    ecs_entity_t parent,
    ecs_type_t type)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_type_t full_type = type;
    
    if (parent) {
        full_type = ecs_type_add(world, full_type, parent | ECS_CHILDOF);
    }

    return _ecs_new(world, full_type);
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

            delete_entity(world, stage, entity, table, table->columns, row->row, 
                &removed);
        }
    } else {
        /* Mark components of the entity in the main stage as removed. This will
         * ensure that subsequent calls to ecs_has, ecs_get and ecs_is_empty will
         * behave consistently with the delete. */
        if ((row = ecs_get_entity(world, NULL, entity))) {
            ecs_table_t *table = row->table;
            if (table) {
                ecs_map_set(stage->remove_merge, entity, &table->type);
            }
        }

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

        commit(world, stage, &info, src_info.type, 0, false);

        if (copy_value) {
            copy_row(info.type, info.columns, info.row,
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
    ecs_add_remove_intern(world, &info, type, 0, true);
}

void _ecs_remove(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type)
{
    ecs_entity_info_t info = {.entity = entity};
    ecs_add_remove_intern(world, &info, 0, type, false);
}

void _ecs_add_remove(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t add_type,
    ecs_type_t remove_type)
{
    ecs_entity_info_t info = {.entity = entity};
    ecs_add_remove_intern(world, &info, add_type, remove_type, false);
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

static
ecs_entity_t _ecs_set_ptr_intern(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type,
    size_t size,
    void *ptr)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(type != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(!size || ptr != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_world_t *world_arg = world;
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_entity_info_t info = {.entity = entity};

    /* Set only accepts types that hold a single component */
    ecs_entity_t component = ecs_type_to_entity(world_arg, type);

    /* If component hasn't been added to entity yet, add it */
    int *dst = ecs_get_ptr_intern(world, stage, &info, component, true, false);
    if (!dst) {
        ecs_add_remove_intern(world_arg, &info, type, 0, false);

        dst = ecs_get_ptr_intern(world, stage, &info, component, true, false);
        if (!dst) {
            /* It is possible that an OnAdd system removed the component before
             * it could have been set */
            return entity;
        }
    }

#ifndef NDEBUG
    ecs_entity_info_t cinfo = {.entity = component};
    EcsComponent *cdata = ecs_get_ptr_intern(
        world, stage, &cinfo, EEcsComponent, false, false);
    ecs_assert(cdata->size == size, ECS_INVALID_COMPONENT_SIZE, NULL);
#endif

    if (dst != ptr) {
        memcpy(dst, ptr, size);
    }

    return entity;
}

ecs_entity_t _ecs_set_ptr(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type,
    size_t size,
    void *ptr)
{
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    /* If no entity is specified, create one */
    if (!entity) {
        entity = _ecs_new(world, type);
    }

    return _ecs_set_ptr_intern(world, entity, type, size, ptr);
}

ecs_entity_t _ecs_set_singleton_ptr(
    ecs_world_t *world,
    ecs_type_t type,
    size_t size,
    void *ptr)
{
    return _ecs_set_ptr_intern(world, ECS_SINGLETON, type, size, ptr);
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
    return ecs_type_contains(world, entity_type, type, match_any, match_prefabs) != 0;
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
    ecs_column_t *columns = NULL;

    if (row && (index = row->row)) {
        index --;

        ecs_table_t *table = row->table;

        if (table) {
            ecs_entity_t *components = ecs_vector_first(table->type);
            columns = table->columns;
            component = components[0];
        }
    }

    if (component == EEcsTypeComponent) {
        ecs_vector_params_t params = {.element_size = sizeof(EcsTypeComponent)};
        EcsTypeComponent *fe = ecs_vector_get(columns[1].data, &params, index);
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
        ecs_type_t remove_type = ecs_map_get_ptr(
                stage->remove_merge, ecs_type_t, entity);
        
        ecs_record_t *main_row = ecs_get_entity(world, NULL, entity);

        if (main_row && main_row->table) {
            result = ecs_type_merge_intern(
                world, stage, main_row->table->type, result, 
                remove_type, NULL, NULL);
        }
    }
    
    return result;
}

uint32_t _ecs_count(
    ecs_world_t *world,
    ecs_type_t type)
{
    // if (!type) {
    //     return 0;
    // }

    // ecs_sparse_t *tables = world->main_stage.tables;
    // uint32_t i, count = ecs_sparse_count(tables);
    // uint32_t result = 0;

    // for (i = 0; i < count; i ++) {
    //     ecs_table_t *table = ecs_sparse_get(tables, ecs_table_t, i);
    //     if (ecs_type_contains(world, table->type, type, true, true)) {
    //         result += ecs_vector_count(table->columns[0].data);
    //     }
    // }
    
    // return result;
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
