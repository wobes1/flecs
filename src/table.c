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
        ecs_vector_set_count(&result, &handle_arr_params, entities->count);
        ecs_entity_t *array = ecs_vector_first(result);
        memcpy(array, entities->array, sizeof(ecs_entity_t) * entities->count);
        return result;
    } else {
        return NULL;
    }
}

static
void init_edges(
    ecs_table_t *table)
{
    ecs_entity_t *entities = ecs_vector_first(table->type);
    uint32_t count = ecs_vector_count(table->type);

    table->edges = ecs_os_calloc(sizeof(ecs_edge_t), ECS_MAX_COMPONENTS);
    
    /* Make add edges to own components point to self */
    int i;
    for (i = 0; i < count; i ++) {
        table->edges[entities[i]].add = table;
        table->edges[entities[i]].remove = NULL;
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
    table->columns = NULL;
    
    init_edges(table);

    table->queries = NULL;
    table->on_new = NULL;

    table->flags = 0;
}

static
ecs_table_t *create_table(
    ecs_world_t *world,
    ecs_entity_array_t *entities)
{
    ecs_table_t *result = ecs_sparse_add(world->tables, ecs_table_t);

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
void create_backlink_after_add(
    ecs_table_t *next,
    ecs_table_t *prev,
    ecs_entity_t add)
{
    ecs_edge_t *edge = &next->edges[add];
    edge->add = NULL;
    edge->remove = prev;
}

static
void create_backlink_after_remove(
    ecs_table_t *next,
    ecs_table_t *prev,
    ecs_entity_t add)
{
    ecs_edge_t *edge = &next->edges[add];
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
ecs_table_t* traverse_add_parent(
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

        if (e & ECS_CHILDOF) {
            ecs_edge_t *edge = &node->parent_edge;
            ecs_table_t *next = edge->add;

            if (!next) {
                next = edge->add = find_or_create_table_include(
                    world, stage, node, ECS_CHILDOF);
                
                ecs_assert(next != NULL, ECS_INTERNAL_ERROR, NULL);                    
            }

            if (added) added->array[added->count ++] = e;

            node = next;
        } else {
            /* No more children to add */
            break;
        }
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

        if (e >= ECS_ENTITY_FLAGS_START) {
            /* Continue looping nodes with flags in separate loop to limit
                * the additional performance overhead of comparing flags. */
            return traverse_add_parent(world, stage, node, e, i, to_add, added);
        }

        /* There should always be an edge for adding */
        ecs_edge_t *edge = &node->edges[e];
        next = edge->add;

        if (!next) {
            next = edge->add = find_or_create_table_include(world, stage, node, e);
            ecs_assert(next != NULL, ECS_INTERNAL_ERROR, NULL);
        }

        if (added) added->array[added->count ++] = e;

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

    init_table(world, &world->table_root, &entities);
}

void ecs_table_fini(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_column_free(world, table, table->columns);
    ecs_vector_free((ecs_vector_t*)table->type);
    free(table->edges);
    ecs_vector_free(table->queries);
    ecs_vector_free(table->on_new);
}

ecs_table_t *ecs_table_find_or_create(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_array_t *entities)
{
    uint32_t count = entities->count;
    if (count) {
        ecs_table_t *table = &world->table_root;
        ecs_entity_t *array = entities->array;
        uint32_t i;

        for (i = 0; i < count; i ++) {
            ecs_entity_t e = array[i];
            ecs_edge_t *edge;

            if (e >= ECS_CHILDOF) {
                edge = &table->parent_edge;
            } else {
                ecs_assert(e < ECS_MAX_COMPONENTS, ECS_INTERNAL_ERROR, NULL);
                edge = &table->edges[e];
            }

            table = edge->add;

            if (!table) {
                ecs_entity_array_t entities = {
                    .array = array,
                    .count = i + 1
                };
                
                table = edge->add = create_table(world, &entities);
            }
        }

        ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
        
        return table;
    } else {
        return NULL;
    }
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
        node = &world->table_root;
    }

    /* Do to_remove first, to limit the depth of the graph */
    if (to_remove) {
        node = traverse_remove(world, stage, node, to_remove, removed);
    }

    if (to_add) {
        node = traverse_add(world, stage, node, to_add, added);
    }

    ecs_assert(node != NULL, ECS_INTERNAL_ERROR, NULL);

    if (node == &world->table_root) {
        node = NULL;
    }

    return node;
}
