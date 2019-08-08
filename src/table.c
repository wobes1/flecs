#include "flecs_private.h"

typedef struct ecs_edge_t {
    ecs_table_t *add;
    ecs_table_t *remove;
} ecs_edge_t;

const ecs_vector_params_t edge_params = {
    .element_size = sizeof(ecs_edge_t)
};

static
ecs_type_t entities_to_type(
    ecs_entity_array_t *entities)
{
    ecs_vector_t *result = NULL;
    ecs_vector_set_count(&result, &handle_arr_params, entities->count);
    ecs_entity_t *array = ecs_vector_first(result);
    memcpy(array, entities->array, sizeof(ecs_entity_t) * entities->count);
    return result;
}

static
void init_table(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_entity_array_t *entities)
{
    table->type = entities_to_type(entities);
    table->columns = NULL;
    
    table->edges = ecs_sparse_new(ecs_edge_t, ECS_MAX_COMPONENTS);    
    table->on_frame = NULL;
    table->on_new = NULL;

    table->flags = 0;

    /* Make add edges to own components point to self */
    int i;
    for (i = 0; i < entities->count; i ++) {
        ecs_edge_t *edge = ecs_sparse_get_or_set_sparse(
            table->edges, 
            ecs_edge_t, 
            entities->array[i],
            NULL);
        
        edge->add = table;
        edge->remove = NULL;
    }
}

static
ecs_table_t *create_table(
    ecs_world_t *world,
    ecs_entity_array_t *entities)
{
    ecs_table_t *result = ecs_sparse_add(world->tables, ecs_table_t);

    init_table(world, result, entities);

    return result;
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
    ecs_entity_t *array = ecs_vector_first(type);
    bool added = false;

    ecs_entity_array_t entities = {
        .array = ecs_os_alloca(sizeof(ecs_entity_t), count + 1),
        .count = count + 1
    };

    int i, el = 0;
    for (i = 0; i < count; i ++) {
        ecs_entity_t e = array[i];
        if (e > add && !added) {
            entities.array[el ++] = add;
            added = true;
        }
        
        entities.array[el ++] = e;
    }

    if (!added) {
        entities.array[el] = add;
    }

    ecs_table_t *result = ecs_table_find_or_create(world, stage, &entities);

    /* Create remove edge to previous node */
    ecs_edge_t *edge = ecs_sparse_get_or_set_sparse(
            result->edges, ecs_edge_t, add, NULL);

    edge->add = NULL;
    edge->remove = node;

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
    ecs_entity_t *array = ecs_vector_first(type);

    ecs_entity_array_t entities = {
        .array = ecs_os_alloca(sizeof(ecs_entity_t), count - 1),
        .count = count - 1
    };

    int i, el = 0;
    for (i = 0; i < count; i ++) {
        ecs_entity_t e = array[i];
        if (e != remove) {
            entities.array[el ++] = e;
            ecs_assert(el < count, ECS_INTERNAL_ERROR, NULL);
        }
    }

    ecs_table_t *result = ecs_table_find_or_create(world, stage, &entities);    

    /* Create add edge to previous node */
    ecs_edge_t *edge = ecs_sparse_get_or_set_sparse(
            result->edges, ecs_edge_t, remove, NULL);

    edge->add = node;
    edge->remove = NULL;

    return result;    
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

ecs_table_t *ecs_table_find_or_create(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_array_t *entities)
{
    uint32_t count = entities->count;

    if (count) {
        ecs_table_t *table;

        if (count > 1) {
            ecs_entity_array_t parent_entities = {
                .array = entities->array,
                .count = count - 1
            };

            table = ecs_table_find_or_create(world, stage, &parent_entities);
            ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
        } else {
            table = &world->table_root;
        }

        ecs_entity_t next = entities->array[count - 1];

        bool is_new = false;
        ecs_edge_t *edge = ecs_sparse_get_or_set_sparse(
            table->edges, ecs_edge_t, next, &is_new);

        if (is_new) {
            edge->add = NULL;
            edge->remove = NULL;
        }

        if (!(table = edge->add)) {
            table = edge->add = create_table(world, entities);
        }

        ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
        
        return table;
    } else {
        return &world->table_root;
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
    int i;
    ecs_table_t *next, *node = table;
    ecs_edge_t *edge = NULL;

    if (!node) {
        node = &world->table_root;
    }

    /* Do to_remove first, to limit the depth of the graph */
    if (to_remove) {
        uint32_t count = to_remove->count;
        ecs_entity_t *entities = to_remove->array;

        for (i = 0; i < count; i ++) {
            ecs_entity_t e = entities[i];

            edge = ecs_sparse_get_sparse(node->edges, ecs_edge_t, e);
            if (edge) {
                next = edge->remove;
                if (!next) {
                    if (edge->add == node) {
                        /* Find table with all components of node except 'e' */
                        next = find_or_create_table_exclude(world, stage, node, e);
                        ecs_assert(next != NULL, ECS_INTERNAL_ERROR, NULL);
                        edge->remove = next;
                    } else {
                        /* If the add edge does not point to self, the table
                         * does not have the entity in to_remove. */
                        continue;
                    }
                }

                if (removed) removed->array[removed->count ++] = e;

                node = next;
            } else {
                /* If there is no edge, the table does not have the entity in
                 * to_remove, since all edges for its own components are added
                 * when the table is initialized. */
            }
        }
    }

    if (to_add) {
        uint32_t count = to_add->count;
        ecs_entity_t *entities = to_add->array;

        for (i = 0; i < count; i ++) {
            ecs_entity_t e = entities[i];

            /* There should always be an edge for adding */
            bool is_new = false;
            ecs_edge_t *edge = ecs_sparse_get_or_set_sparse(
                node->edges, ecs_edge_t, e, &is_new);

            ecs_assert(edge != NULL, ECS_INTERNAL_ERROR, NULL);
            ecs_table_t *next = edge->add;

            if (is_new || !next) {
                /* Initialize new element */
                edge->add = NULL; 
                edge->remove = NULL;

                /* Find table with all components of node including 'e' */
                next = find_or_create_table_include(world, stage, node, e);
                edge->add = next;
            } else if (next == node) {
                /* If edge points to self, the current table already has this
                 * component. */
                continue;
            }

            if (added) added->array[added->count ++] = e;

            node = next;
        }
    }

    return node;
}
