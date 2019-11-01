#include "flecs_private.h"

const ecs_vector_params_t char_arr_params = {
    .element_size = sizeof(char)
};

/** Parse callback that adds type to type identifier for ecs_new_type */
static
int parse_type_action(
    ecs_world_t *world,
    ecs_signature_from_kind_t elem_kind,
    ecs_signature_op_kind_t oper_kind,
    const char *entity_id,
    const char *source_id,
    void *data)
{
    ecs_vector_t **array = data;
    (void)source_id;

    if (strcmp(entity_id, "0")) {
        ecs_entity_t entity = 0;

        if (elem_kind != EcsFromSelf) {
            return ECS_INVALID_TYPE_EXPRESSION;
        }

        if (!strcmp(entity_id, "INSTANCEOF")) {
            entity = ECS_INSTANCEOF;
        } else if (!strcmp(entity_id, "CHILDOF")) {
            entity = ECS_CHILDOF;
        } else {
            entity = ecs_lookup(world, entity_id);
        }
        
        if (!entity) {
            ecs_os_err("%s not found", entity_id);
            return ECS_INVALID_TYPE_EXPRESSION;
        }

        if (oper_kind == EcsOperAnd) {
            ecs_entity_t* e_ptr = ecs_vector_add(array, &handle_arr_params);
            *e_ptr = entity;
        } else if (oper_kind == EcsOperOr) {
            ecs_entity_t *e_ptr = ecs_vector_last(*array, &handle_arr_params);

            /* If using an OR operator, the array should at least have one 
             * element. */
            ecs_assert(e_ptr != NULL, ECS_INTERNAL_ERROR, NULL);

            if (*e_ptr & ECS_ENTITY_MASK) {
                if (entity & ECS_ENTITY_MASK) {
                    /* An expression should not OR entity ids, only entities +
                     * entity flags. */
                    return ECS_INVALID_TYPE_EXPRESSION;
                }
            }

            *e_ptr |= entity;
        } else {
            /* Only AND and OR operators are supported for type expressions */
            return ECS_INVALID_TYPE_EXPRESSION;
        }
    }

    return 0;
}

static
ecs_entity_t split_entity_id(
    ecs_entity_t id,
    ecs_entity_t *entity)
{
    *entity = (id & ECS_ENTITY_MASK);
    return id;
}

ecs_entity_t ecs_find_entity_in_prefabs(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type,
    ecs_entity_t component,
    ecs_entity_t previous)
{
    int32_t i, count = ecs_vector_count(type);
    ecs_entity_t *array = ecs_vector_first(type);

    /* Walk from back to front, as prefabs are always located
     * at the end of the type. */
    for (i = count - 1; i >= 0; i --) {
        ecs_entity_t e = array[i];

        if (e & ECS_INSTANCEOF) {
            ecs_entity_t prefab = e & ECS_ENTITY_MASK;
            ecs_type_t prefab_type = ecs_get_type(world, prefab);

            if (prefab == previous) {
                continue;
            }

            if (ecs_type_has_entity_intern(
                world, prefab_type, component, false)) 
            {
                return prefab;
            } else {
                prefab = ecs_find_entity_in_prefabs(
                    world, prefab, prefab_type, component, entity);
                if (prefab) {
                    return prefab;
                }
            }
        } else {
            /* If this is not a prefab, the following entities won't
                * be prefabs either because the array is sorted, and
                * the prefab bit is 2^63 which ensures that prefabs are
                * guaranteed to be the last entities in the type */
            break;
        }
    }

    return 0;
}

/* -- Private functions -- */

ecs_table_t* ecs_type_find_table(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t type)
{
    ecs_entity_array_t entities = {
        .array = ecs_vector_first(type),
        .count = ecs_vector_count(type)
    };

    return ecs_table_find_or_create(world, stage, &entities);
}

/** Extend existing type with additional entity */
ecs_type_t ecs_type_add_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t type,
    ecs_entity_t e)
{
    ecs_table_t *table = ecs_type_find_table(world, stage, type);

    ecs_entity_array_t entities = {
        .array = &e,
        .count = 1
    };

    table = ecs_table_traverse(world, stage, table, &entities, NULL, NULL, NULL);

    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);

    return table->type;
}

/** Return merged type */
ecs_type_t ecs_type_merge_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t cur,
    ecs_type_t to_add,
    ecs_type_t to_remove,
    ecs_entity_array_t *to_add_except,
    ecs_entity_array_t *to_remove_intersect)
{
    ecs_table_t *table = ecs_type_find_table(world, stage, cur);

    ecs_entity_array_t add_array = {
        .array = ecs_vector_first(to_add),
        .count = ecs_vector_count(to_add)
    };

    ecs_entity_array_t remove_array = {
        .array = ecs_vector_first(to_remove),
        .count = ecs_vector_count(to_remove)
    };

    table = ecs_table_traverse(
        world, stage, table, &add_array, &remove_array, to_add_except, 
        to_remove_intersect);

    if (!table) {
        return NULL;
    } else {
        return table->type;
    }
}

/* O(n) algorithm to check whether type 1 is equal or superset of type 2 */
bool ecs_type_contains(
    ecs_world_t *world,
    ecs_type_t type_1,
    ecs_type_t type_2,
    bool match_all,
    bool match_prefab,
    ecs_entity_t *found)
{
    if (!type_1) {
        return false;
    }

    ecs_assert(type_2 != NULL, ECS_INTERNAL_ERROR, NULL);

    if (type_1 == type_2) {
        return *(ecs_entity_t*)ecs_vector_first(type_1);
    }

    uint32_t i_2, i_1 = 0;
    ecs_entity_t e1 = 0;
    ecs_entity_t *t1_array = ecs_vector_first(type_1);
    ecs_entity_t *t2_array = ecs_vector_first(type_2);
    uint32_t t1_count = ecs_vector_count(type_1);
    uint32_t t2_count = ecs_vector_count(type_2);

    for (i_2 = 0; i_2 < t2_count; i_2 ++) {
        ecs_entity_t e2 = t2_array[i_2] & ECS_ENTITY_MASK;

        if (i_1 >= t1_count) {
            return false;
        }

        e1 = t1_array[i_1] & ECS_ENTITY_MASK;

        if (e2 > e1) {
            do {
                i_1 ++;
                if (i_1 >= t1_count) {
                    return false;
                }
                e1 = t1_array[i_1] & ECS_ENTITY_MASK;
            } while (e2 > e1);
        }

        if (e1 != e2) {
            if (match_prefab && e2 != EEcsId && e2 != EEcsPrefab && e2 != EEcsDisabled) {
                if (ecs_find_entity_in_prefabs(world, 0, type_1, e2, 0)) {
                    e1 = e2;
                }
            }

            if (e1 != e2) {
                if (match_all) {
                    return false;
                }
            } else if (!match_all) {
                if (found) *found = e1;
                return true;
            }
        } else {
            if (!match_all) {
                if (found) *found = e1;
                return true;
            }
            i_1 ++;
            if (i_1 < t1_count) {
                e1 = t1_array[i_1] & ECS_ENTITY_MASK;
            }
        }
    }

    if (match_all) {
        if (found) *found = e1;
        return true;
    } else {
        return false;
    }
}

bool ecs_type_has_entity_intern(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_entity_t entity,
    bool match_prefab)
{
    ecs_entity_t *array = ecs_vector_first(type);
    uint32_t i, count = ecs_vector_count(type);

    for (i = 0; i < count; i++) {
        ecs_entity_t e = array[i];
        if ((e == entity) || (e & ECS_ENTITY_MASK) == entity) {
            return true;
        }
    }

    if (match_prefab) {
        if (ecs_find_entity_in_prefabs(world, 0, type, entity, 0)) {
            return true;
        }
    }

    return false;
}

int32_t ecs_type_container_depth(
   ecs_world_t *world,
   ecs_type_t type,
   ecs_entity_t component)     
{
    int result = 0;

    int32_t i, count = ecs_vector_count(type);
    ecs_entity_t *array = ecs_vector_first(type);

    for (i = count - 1; i >= 0; i --) {
        if (array[i] & ECS_CHILDOF) {
            ecs_type_t c_type = ecs_get_type(world, array[i] & ECS_ENTITY_MASK);
            int32_t j, c_count = ecs_vector_count(c_type);
            ecs_entity_t *c_array = ecs_vector_first(c_type);

            for (j = 0; j < c_count; j ++) {
                if (c_array[j] == component) {
                    result ++;
                    result += ecs_type_container_depth(
                        world, c_type, component);
                    break;
                }
            }

            if (j != c_count) {
                break;
            }
        } else if (!(array[i] & ECS_ENTITY_FLAGS_MASK)) {
            /* No more parents after this */
            break;
        }
    }

    return result;
}

/* This function is used to derive a type from a vector with arbitrarily
 * ordered entities, such as the one that is returned when parsing a type
 * expression. */
static
EcsTypeComponent type_from_vec(
    ecs_world_t *world,
    ecs_vector_t *vec)
{
    EcsTypeComponent result = {0, 0};

    int32_t count = ecs_vector_count(vec);
    if (!count) {
        return result;
    }

    /* Determining the type is simple, just find the table with the specified
     * entities. */
    ecs_table_t *table = ecs_table_find_or_create(world, NULL, 
    &(ecs_entity_array_t){
        .array = ecs_vector_first(vec),
        .count = count
    });
    
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    result.type = table->type;

    /* To find the actual (resolved) type, we need to call the 
     * ecs_type_from_entity function to obtain the actual type for each entity.
     * When the entity contains an EcsTypeComponent, the resulting type can have
     * multiple entities. */
    ecs_entity_t *array = ecs_vector_first(vec);
    uint32_t i;
    
    table = NULL; /* Start from root */

    for (i = 0; i < count; i ++) {
        ecs_entity_t entity = array[i];
        ecs_type_t actual = ecs_type_from_entity(world, entity);

        ecs_entity_array_t entities = {
            .array = ecs_vector_first(actual),
            .count = ecs_vector_count(actual)
        };

        table = ecs_table_traverse(
            world, NULL, table, &entities, NULL, NULL, NULL);
    }

    result.normalized = table->type;

    return result;
}

/* Translate a type expression to a type. Return an EcsTypeComponent, so the
 * caller can use the normalized type as well as the non-normalized type. */
static
EcsTypeComponent type_from_expr(
    ecs_world_t *world,
    const char *expr)
{
    ecs_vector_t *vec = ecs_vector_new(&handle_arr_params, 1);
    ecs_parse_component_expr(world, expr, parse_type_action, &vec);
    EcsTypeComponent result = type_from_vec(world, vec);
    ecs_vector_free(vec);
    return result;
}

/* -- Public API -- */

ecs_entity_t ecs_new_type(
    ecs_world_t *world,
    const char *id,
    const char *expr)
{
    assert(world->magic == ECS_WORLD_MAGIC);  

    EcsTypeComponent type = type_from_expr(world, expr);
    ecs_entity_t result = ecs_lookup(world, id);

    if (result) {
        EcsTypeComponent *type_ptr = ecs_get_ptr(world, result, EcsTypeComponent);
        if (type_ptr) {
            if (type_ptr->type != type.type || 
                type_ptr->normalized != type.normalized) 
            {
                ecs_abort(ECS_ALREADY_DEFINED, id);
            }
        } else {
            ecs_abort(ECS_ALREADY_DEFINED, id);
        }
    } else if (!result) {
        result = _ecs_new(world, world->t_type->type);
        ecs_set(world, result, EcsId, {id});
        ecs_set(world, result, EcsTypeComponent, {
            .type = type.type, .normalized = type.normalized
        });

        /* Register named types with world, so applications can automatically
         * detect features (amongst others). */
        ecs_map_set(world->type_handles, (uintptr_t)type.type, &result);
    }

    return result;
}

ecs_entity_t ecs_new_prefab(
    ecs_world_t *world,
    const char *id,
    const char *expr)
{
    ecs_assert(world->magic == ECS_WORLD_MAGIC, ECS_INVALID_PARAMETER, NULL);
   
    EcsTypeComponent type = type_from_expr(world, expr);
    type.normalized = ecs_type_merge(world, world->t_prefab->type, type.normalized, 0);

    ecs_entity_t result = ecs_lookup(world, id);
    if (result) {
        if (ecs_get_type(world, result) != type.normalized) {
            ecs_abort(ECS_ALREADY_DEFINED, id);
        }
    } else {
        result = _ecs_new(world, type.normalized);
        ecs_set(world, result, EcsId, {id});
    }

    return result;
}

ecs_entity_t ecs_new_entity(
    ecs_world_t *world,
    const char *id,
    const char *expr)
{
    ecs_assert(world->magic == ECS_WORLD_MAGIC, ECS_INVALID_PARAMETER, NULL);
   
    EcsTypeComponent type = type_from_expr(world, expr);

    ecs_entity_t result = ecs_lookup(world, id);
    if (result) {
        if (ecs_get_type(world, result) != type.normalized) {
            ecs_abort(ECS_ALREADY_DEFINED, id);
        }
    } else {
        result = _ecs_new(world, type.normalized);
        ecs_set(world, result, EcsId, {id});
    }

    return result;
}

int16_t ecs_type_index_of(
    ecs_type_t type,
    ecs_entity_t entity)
{
    ecs_entity_t *buf = ecs_vector_first(type);
    int i, count = ecs_vector_count(type);
    
    for (i = 0; i < count; i ++) {
        if ((buf[i] & ECS_ENTITY_MASK) == entity) {
            return i; 
        }
    }

    return -1;
}

ecs_type_t ecs_type_merge(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_type_t type_add,
    ecs_type_t type_remove)
{
    ecs_stage_t *stage = ecs_get_stage(&world);

    return ecs_type_merge_intern(world, stage, type, type_add, type_remove, 
                NULL, NULL);
}

ecs_type_t ecs_type_find(
    ecs_world_t *world,
    ecs_entity_t *array,
    uint32_t count)
{
    ecs_stage_t *stage = ecs_get_stage(&world);

    ecs_table_t *table = ecs_table_find_or_create(world, stage, 
        &(ecs_entity_array_t) {
            .array = array,
            .count = count
        }
    );

    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);

    return table->type;
}

ecs_entity_t ecs_type_get_entity(
    ecs_world_t *world,
    ecs_type_t type,
    uint32_t index)
{
    (void)world;
    ecs_assert(world != NULL, ECS_INVALID_PARAMETER, NULL);

    if (!type) {
        return 0;
    }
    
    ecs_entity_t *array = ecs_vector_first(type);

    if (ecs_vector_count(type) > index) {
        return array[index];
    } else {
        return 0;
    }
}

bool ecs_type_has_entity(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_entity_t entity)
{
    return ecs_type_has_entity_intern(world, type, entity, false);
}

ecs_type_t ecs_expr_to_type(
    ecs_world_t *world,
    const char *expr)
{
    EcsTypeComponent type = type_from_expr(world, expr);
    return type.normalized;
}

ecs_type_t ecs_type_add(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_entity_t e)
{
    ecs_stage_t *stage = ecs_get_stage(&world);
    return ecs_type_add_intern(world, stage, type, e);
}

char* ecs_type_to_expr(
    ecs_world_t *world,
    ecs_type_t type)
{
    ecs_vector_t *chbuf = ecs_vector_new(&char_arr_params, 32);
    char *dst;
    uint32_t len;
    char buf[15];

    ecs_entity_t *handles = ecs_vector_first(type);
    uint32_t i, count = ecs_vector_count(type);

    for (i = 0; i < count; i ++) {
        ecs_entity_t h;
        ecs_entity_t flags = split_entity_id(handles[i], &h) & ECS_ENTITY_FLAGS_MASK;

        if (i) {
            *(char*)ecs_vector_add(&chbuf, &char_arr_params) = ',';
        }

        if (flags & ECS_INSTANCEOF) {
            int len = sizeof("INSTANCEOF|") - 1;
            dst = ecs_vector_addn(&chbuf, &char_arr_params, len);
            memcpy(dst, "INSTANCEOF|", len);
        }

        if (flags & ECS_CHILDOF) {
            int len = sizeof("CHILDOF|") - 1;
            dst = ecs_vector_addn(&chbuf, &char_arr_params, len);
            memcpy(dst, "CHILDOF|", len);
        }

        const char *str = NULL;
        EcsId *id = ecs_get_ptr(world, h, EcsId);
        if (id) {
            str = *id;
        } else {
            int h_int = h;
            sprintf(buf, "%u", h_int);
            str = buf;
        }
        len = strlen(str);
        dst = ecs_vector_addn(&chbuf, &char_arr_params, len);
        memcpy(dst, str, len);
    }

    *(char*)ecs_vector_add(&chbuf, &char_arr_params) = '\0';

    char *result = strdup(ecs_vector_first(chbuf));
    ecs_vector_free(chbuf);

    return result;
}

bool ecs_type_match_w_filter(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_type_filter_t *filter)
{
    if (!filter) {
        return true;
    }
    
    if (filter->include) {
        /* If filter kind is exact, types must be the same */
        if (filter->include_kind == EcsMatchExact) {
            if (type != filter->include) {
                return false;
            }

        /* Default for include_kind is MatchAll */
        } else if (!ecs_type_contains(world, type, filter->include, 
            filter->include_kind != EcsMatchAny, false, NULL)) 
        {
            return false;
        }
    
    /* If no include filter is specified, make sure that builtin components
        * aren't matched by default. */
    } else {
        if (ecs_type_contains(
            world, type, world->t_builtins, false, false, NULL))
        {
            /* Continue if table contains any of the builtin components */
            return false;
        }
    }

    if (filter->exclude) {
        /* If filter kind is exact, types must be the same */
        if (filter->exclude_kind == EcsMatchExact) {
            if (type == filter->exclude) {
                return false;
            }
        
        /* Default for exclude_kind is MatchAny */                
        } else if (ecs_type_contains(world, type, filter->exclude, 
            filter->exclude_kind == EcsMatchAll, false, NULL))
        {
            return false;
        }
    }

    return true;
}
