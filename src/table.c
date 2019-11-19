#include "flecs_private.h"

static
void match_queries(
    ecs_world_t *world,
    ecs_table_t *table)
{
    uint32_t i, count = ecs_sparse_count(world->queries);

    for (i = 0; i < count; i ++) {
        ecs_query_t *query = ecs_sparse_get(world->queries, ecs_query_t, i);
        ecs_query_match_table(world, query, table);
    }
}

static
ecs_type_t entities_to_type(
    ecs_entity_array_t *entities)
{
    if (entities->count) {
        ecs_vector_t *result = NULL;
        ecs_vector_set_count(&result, ecs_entity_t, entities->count);
        ecs_entity_t *array = ecs_vector_first(result);
        memcpy(array, entities->array, sizeof(ecs_entity_t) * entities->count);
        return result;
    } else {
        return NULL;
    }
}

static
void init_edges(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_entity_t *entities = ecs_vector_first(table->type);
    uint32_t count = ecs_vector_count(table->type);

    table->edges = ecs_os_calloc(sizeof(ecs_edge_t), ECS_MAX_COMPONENTS);
    table->hi_edges = ecs_map_new(ecs_edge_t, 0);
    
    /* Make add edges to own components point to self */
    int i;
    for (i = 0; i < count; i ++) {
        ecs_entity_t e = entities[i];

        if (e >= ECS_MAX_COMPONENTS) {
            ecs_edge_t edge = { .add = table };

            if (count == 1) {
                edge.remove = &world->main_stage.table_root;
            }

            ecs_map_set(table->hi_edges, e, &edge);

            if (e & ECS_INSTANCEOF) {
                table->has_base = true;
            }
            if (e & ECS_CHILDOF) {
                table->has_parent = true;
            }
        } else if (e == ecs_entity(EcsPrefab)) {
            table->is_prefab = true;
        } else {
            table->edges[e].add = table;

            if (count == 1) {
                table->edges[e].remove = &world->main_stage.table_root;
            } else {
                table->edges[e].remove = NULL;
            }
        }
    }

    table->parent_edge.add = NULL;
    table->parent_edge.remove = NULL;
}

static
void init_table(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_entity_array_t *entities)
{
    table->type = entities_to_type(entities);
    memset(table->columns, 0, sizeof(table->columns));
    table->dst_rows = NULL;
    table->has_base = 0;
    table->has_parent = 0;
    table->is_prefab = 0;
    
    init_edges(world, table);

    table->queries = NULL;
    table->on_new = NULL;
}

static
ecs_table_t *create_table(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_array_t *entities)
{
    ecs_table_t *result = ecs_sparse_add(stage->tables, ecs_table_t);

    init_table(world, result, entities);

    match_queries(world, result);

    return result;
}

static
void add_entity_to_type(
    ecs_type_t type,
    ecs_entity_t add,
    ecs_entity_array_t *out)
{
    uint32_t count = ecs_vector_count(type);
    ecs_entity_t *array = ecs_vector_first(type);    
    bool added = false;

    int i, el = 0;
    for (i = 0; i < count; i ++) {
        ecs_entity_t e = array[i];
        if (e > add && !added) {
            out->array[el ++] = add;
            added = true;
        }
        
        out->array[el ++] = e;

        ecs_assert(el <= out->count, ECS_INTERNAL_ERROR, NULL);
    }

    if (!added) {
        out->array[el] = add;
    }
}

static
void remove_entity_from_type(
    ecs_type_t type,
    ecs_entity_t remove,
    ecs_entity_array_t *out)
{
    uint32_t count = ecs_vector_count(type);
    ecs_entity_t *array = ecs_vector_first(type);

    int i, el = 0;
    for (i = 0; i < count; i ++) {
        ecs_entity_t e = array[i];
        if (e != remove) {
            out->array[el ++] = e;
            ecs_assert(el < count, ECS_INTERNAL_ERROR, NULL);
        }
    }
}

static
ecs_edge_t* get_edge(
    ecs_table_t *node,
    ecs_entity_t e)
{
    ecs_edge_t *edge;

    if (e < ECS_MAX_COMPONENTS) {
        edge = &node->edges[e];
    } else if (e & ECS_CHILDOF) {
        edge = &node->parent_edge;
    } else {
        ecs_edge_t new_edge = {0};
        ecs_map_set(node->hi_edges, e, &new_edge);
        edge = ecs_map_get(node->hi_edges, ecs_edge_t, e);        
    }

    return edge;
}

static
void create_backlink_after_add(
    ecs_table_t *next,
    ecs_table_t *prev,
    ecs_entity_t add)
{
    ecs_edge_t *edge = get_edge(next, add);
    edge->add = NULL;
    edge->remove = prev;
}

static
void create_backlink_after_remove(
    ecs_table_t *next,
    ecs_table_t *prev,
    ecs_entity_t add)
{
    ecs_edge_t *edge = get_edge(next, add);
    edge->add = prev;
    edge->remove = NULL;
}

static
ecs_table_t *find_or_create_table_include(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *node,
    ecs_entity_t add)
{
    ecs_type_t type = node->type;
    uint32_t count = ecs_vector_count(type);

    ecs_entity_array_t entities = {
        .array = ecs_os_alloca(sizeof(ecs_entity_t), count + 1),
        .count = count + 1
    };

    add_entity_to_type(type, add, &entities);

    ecs_table_t *result = ecs_table_find_or_create(world, stage, &entities);

    create_backlink_after_add(result, node, add);

    return result;
}

static
ecs_table_t *find_or_create_table_exclude(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *node,
    ecs_entity_t remove)
{
    ecs_type_t type = node->type;
    uint32_t count = ecs_vector_count(type);

    ecs_entity_array_t entities = {
        .array = ecs_os_alloca(sizeof(ecs_entity_t), count - 1),
        .count = count - 1
    };

    remove_entity_from_type(type, remove, &entities);

    ecs_table_t *result = ecs_table_find_or_create(world, stage, &entities);
    if (!result) {
        return NULL;
    }

    create_backlink_after_remove(result, node, remove);

    return result;    
}

static
ecs_table_t* traverse_remove_hi_edges(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *node,
    ecs_entity_t e,
    uint32_t i,
    ecs_entity_array_t *to_remove,
    ecs_entity_array_t *removed)
{
    uint32_t count = to_remove->count;
    ecs_entity_t *entities = to_remove->array;

    for (; i < count; i ++) {
        ecs_entity_t e = entities[i];
        ecs_entity_t next_e = e;
        ecs_table_t *next;
        ecs_edge_t *edge;

        if (e & ECS_CHILDOF) {
            edge = &node->parent_edge;
            next_e = ECS_CHILDOF;
        } else {
            edge = get_edge(node, e);
        }

        next = edge->remove;

        if (!next) {
            next = edge->remove = find_or_create_table_exclude(
                world, stage, node, next_e);
            if (!next) {
                return NULL;
            }

            edge->remove = next;
        }

        /* Hi edges should are not removed to the removed array. This array is used
         * to determine if component actions need to be executed, and entities
         * that are not a component (>ECS_MAX_COMPONENT) there is never any
         * component action.
         *
         if (removed && node != next) removed->array[removed->count ++] = e;
         */

        node = next;        
    }

    return node;
}

static
ecs_table_t* traverse_remove(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *node,
    ecs_entity_array_t *to_remove,
    ecs_entity_array_t *removed)
{
    uint32_t i, count = to_remove->count;
    ecs_entity_t *entities = to_remove->array;

    for (i = 0; i < count; i ++) {
        ecs_entity_t e = entities[i];

        /* If the array is not a simple component array, use a function that
         * handles all cases, but is slower */
        if (e >= ECS_MAX_COMPONENTS) {
            return traverse_remove_hi_edges(world, stage, node, e, i, to_remove, removed);
        }

        ecs_edge_t *edge = &node->edges[e];
        ecs_table_t *next = edge->remove;

        if (!next) {
            if (edge->add == node) {
                /* Find table with all components of node except 'e' */
                next = find_or_create_table_exclude(world, stage, node, e);
                if (!next) {
                    return NULL;
                }

                edge->remove = next;
            } else {
                /* If the add edge does not point to self, the table
                    * does not have the entity in to_remove. */
                continue;
            }
        }

        if (removed) removed->array[removed->count ++] = e;

        node = next;
    }    

    return node;
}

static
ecs_table_t* traverse_add_hi_edges(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *node,
    ecs_entity_t e,
    uint32_t i,
    ecs_entity_array_t *to_add,
    ecs_entity_array_t *added)
{
    uint32_t count = to_add->count;
    ecs_entity_t *entities = to_add->array;

    for (; i < count; i ++) {
        ecs_entity_t e = entities[i];
        ecs_entity_t next_e = e;
        ecs_table_t *next;
        ecs_edge_t *edge;

        if (e & ECS_CHILDOF) {
            edge = &node->parent_edge;
            next_e = ECS_CHILDOF;
        } else {
            edge = get_edge(node, e);
        }

        next = edge->add;

        if (!next) {
            next = edge->add = find_or_create_table_include(
                world, stage, node, next_e);
            
            ecs_assert(next != NULL, ECS_INTERNAL_ERROR, NULL);                    
        }

        /* Hi edges should are not added to the added array. This array is used
         * to determine if component actions need to be executed, and entities
         * that are not a component (>ECS_MAX_COMPONENT) there is never any
         * component action.
         *
         if (added && node != next) added->array[added->count ++] = e;
         */

        node = next;        
    }

    return node;
}

static
ecs_table_t* traverse_add(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *node,
    ecs_entity_array_t *to_add,
    ecs_entity_array_t *added)    
{
    uint32_t i, count = to_add->count;
    ecs_entity_t *entities = to_add->array;

    for (i = 0; i < count; i ++) {
        ecs_entity_t e = entities[i];
        ecs_table_t *next;

        /* If the array is not a simple component array, use a function that
         * handles all cases, but is slower */
        if (e >= ECS_MAX_COMPONENTS) {
            return traverse_add_hi_edges(world, stage, node, e, i, to_add, added);
        }

        /* There should always be an edge for adding */
        ecs_edge_t *edge = &node->edges[e];
        next = edge->add;

        if (!next) {
            next = edge->add = find_or_create_table_include(world, stage, node, e);
            ecs_assert(next != NULL, ECS_INTERNAL_ERROR, NULL);
        }

        if (added && node != next) added->array[added->count ++] = e;

        node = next;
    }

    return node;
}

void ecs_init_root_table(
    ecs_world_t *world)
{
    ecs_entity_array_t entities = {
        .array = NULL,
        .count = 0
    };

    init_table(world, &world->main_stage.table_root, &entities);
}

ecs_columns_t* ecs_table_get_columns(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table)
{
    ecs_columns_t *result = table->columns[stage->id];

    if (!result) {
        result = table->columns[stage->id] = ecs_columns_new(world, table);

        if (stage != &world->main_stage) {
            /* If this is not the main stage, keep track of the tables for
             * which data was created in the stage */
            ecs_table_t **elem = ecs_vector_add(
                &stage->dirty_tables, ecs_table_t*);

            *elem = table;
        }
    }

    return result;
}

void ecs_table_fini(
    ecs_world_t *world,
    ecs_table_t *table)
{
    int i;
    for (i = 0; i < ECS_MAX_STAGES; i ++) {
        ecs_columns_t *columns = table->columns[i];
        if (columns) {
            ecs_columns_free(world, table, columns);
        }
    }

    ecs_vector_free((ecs_vector_t*)table->type);
    free(table->edges);
    ecs_vector_free(table->queries);
    ecs_vector_free(table->on_new);
}

void ecs_table_clear(
    ecs_world_t *world,
    ecs_table_t *table)
{
    /* This only clears the columns in the main stage! */
    if (table->columns[0]) {
        ecs_columns_clear(table, table->columns[0]);
    }
}

static
int ecs_entity_compare(
    const void *e1,
    const void *e2)
{
    const ecs_entity_t v1 = *(ecs_entity_t*)e1;
    const ecs_entity_t v2 = *(ecs_entity_t*)e2;
    return v1 - v2;
}

static
bool ecs_entity_array_is_ordered(
    ecs_entity_array_t *entities)
{
    ecs_entity_t prev = 0;
    ecs_entity_t *array = entities->array;
    uint32_t i, count = entities->count;

    for (i = 0; i < count; i ++) {
        if (array[i] <= prev) {
            return false;
        }
        prev = array[i];
    }    

    return true;
}

static
void ecs_entity_array_dedup(
    ecs_entity_array_t *entities)
{
    ecs_entity_t *array = entities->array;
    uint32_t j, k, count = entities->count;
    ecs_entity_t prev = array[0];

    for (k = j = 1; k < count; j ++, k++) {
        ecs_entity_t e = array[k];
        if (e == prev) {
            k ++;
        }

        array[j] = e;
        prev = e;
    }

    entities->count -= (k - j);
}

static
ecs_table_t *ecs_table_find_or_create_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *root,
    ecs_entity_array_t *entities)
{    
    uint32_t count;
    if (entities && (count = entities->count)) 
    {
        bool is_ordered = true, order_checked = false;
        ecs_entity_t *ordered_entities = NULL;

        ecs_table_t *table = root;
        ecs_entity_t *array = entities->array;
        uint32_t i;

        if (!stage) {
            stage = &world->main_stage;
        }

        for (i = 0; i < count; i ++) {
            ecs_entity_t e = array[i];
            ecs_edge_t *edge;

            if (e >= ECS_MAX_COMPONENTS) {
                if (e >= ECS_CHILDOF) {
                    edge = &table->parent_edge;
                    table = edge->add;
                } else {
                    edge = ecs_map_get(table->hi_edges, ecs_edge_t, e);
                    if (edge) {
                        table = edge->add;
                    } else {
                        edge = get_edge(table, e);
                        table = NULL;
                    }
                }
            } else {
                ecs_assert(e < ECS_MAX_COMPONENTS, ECS_INTERNAL_ERROR, NULL);
                edge = &table->edges[e];
                table = edge->add;
            }

            if (!table) {
                /* A new table needs to be created. To ensure that one table is
                 * created per combination of components, regardless of in
                 * which order the components are specified, the entity array
                 * with which the table is created must be ordered. */

                /* First, determine of the entities array is ordered. Do this
                 * only once per lookup */
                if (!order_checked) {
                    is_ordered = ecs_entity_array_is_ordered(entities);
                    order_checked = true;
                }

                /* If the array is ordered, we can use it to create the table */
                if (is_ordered) {
                    ecs_entity_array_t table_entities = {
                        .array = array,
                        .count = i + 1
                    };

                    /* If the original array is ordered and the edge was empty, 
                    * the table does not exist, so create it */
                    if (stage != &world->main_stage && stage != &world->temp_stage) {
                        /* If we're in threaded mode and we have been searching
                         * the main stage tables, find or create the table in 
                         * the thread specific staging area.
                         *
                         * This is expensive, but should rarely happen as 
                         * eventually the main stage will have tables for all of
                         * the entity types. */
                        if (root == &world->main_stage.table_root) {
                            return ecs_table_find_or_create_intern(
                                world, stage, &stage->table_root, entities);
                        } else {
                            /* If we are staged and we were looking in the table
                             * root of the stage, the table doesn't exist yet
                             * and we need to create it in the stage. */
                            
                            /* Verify that we weren't accidentally searching the
                             * wrong stage */
                            ecs_assert(&stage->table_root == root, 
                                ECS_INTERNAL_ERROR, NULL);
                            
                            table = create_table(world, stage, &table_entities);
                        }
                    } else {
                        table = create_table(world, stage, &table_entities);
                    }
                } else {
                    uint32_t count_now = i + 1;

                    /* Create an ordered array if we don't have one yet */
                    if (!ordered_entities) {
                        ordered_entities = ecs_os_alloca(ecs_entity_t, count);
                    }
                    
                    memcpy(ordered_entities, array, 
                        count_now * sizeof(ecs_entity_t));

                    qsort(ordered_entities, count_now, 
                        sizeof(ecs_entity_t), ecs_entity_compare);

                    ecs_entity_array_t table_entities = {
                        .array = ordered_entities,
                        .count = count_now
                    };

                    /* Now that the array is sorted, dedup */
                    ecs_entity_array_dedup(&table_entities);

                    /* If the original array is unordered we want to check if an
                    * existing table can be found using the ordered array */
                    table = ecs_table_find_or_create_intern(
                        world, stage, root, &table_entities);                                
                }                    

                ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);

                edge->add = table;
            }
        }

        ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
        
        return table;
    } else {
        return NULL;
    }
}

ecs_table_t *ecs_table_find_or_create(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_array_t *entities)
{
    return ecs_table_find_or_create_intern(
        world, stage, &world->main_stage.table_root, entities);
}

ecs_table_t *ecs_table_traverse(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_entity_array_t *to_add,
    ecs_entity_array_t *to_remove,
    ecs_entity_array_t *added,
    ecs_entity_array_t *removed)
{
    ecs_table_t *node = table;

    if (!node) {
        node = &world->main_stage.table_root;
    }

    /* Do to_remove first, to limit the depth of the graph */
    if (to_remove) {
        node = traverse_remove(world, stage, node, to_remove, removed);
    }

    if (to_add) {
        node = traverse_add(world, stage, node, to_add, added);
    }

    if (node == &world->main_stage.table_root) {
        node = NULL;
    }

    return node;
}
