#include "flecs_private.h"

/** Skip spaces when parsing signature */
static
const char *skip_space(
    const char *ptr)
{
    while (isspace(*ptr)) {
        ptr ++;
    }
    return ptr;
}

/** Parse element with a dot-separated qualifier ('CONTAINER.Foo') */
static
char* parse_complex_elem(
    char *bptr,
    ecs_signature_from_kind_t *elem_kind,
    ecs_signature_op_kind_t *oper_kind,
    const char * *source)
{
    if (bptr[0] == '!') {
        *oper_kind = EcsOperNot;
        if (!bptr[1]) {
            ecs_abort(ECS_INVALID_EXPRESSION, bptr);
        }
        bptr ++;
    } else if (bptr[0] == '?') {
        *oper_kind = EcsOperOptional;
        if (!bptr[1]) {
            ecs_abort(ECS_INVALID_EXPRESSION, bptr);
        }
        bptr ++;
    }

    *source = NULL;

    char *dot = strchr(bptr, '.');
    if (dot) {
        if (bptr == dot) {
            *elem_kind = EcsFromEmpty;
        } else if (!strncmp(bptr, "CONTAINER", dot - bptr)) {
            *elem_kind = EcsFromContainer;
        } else if (!strncmp(bptr, "SYSTEM", dot - bptr)) {
            *elem_kind = EcsFromSystem;
        } else if (!strncmp(bptr, "SELF", dot - bptr)) {
            /* default */
        } else if (!strncmp(bptr, "OWNED", dot - bptr)) {
            *elem_kind = EcsFromOwned;
        } else if (!strncmp(bptr, "SHARED", dot - bptr)) {
            *elem_kind = EcsFromShared;
        } else if (!strncmp(bptr, "CASCADE", dot - bptr)) {
            *elem_kind = EcsCascade;   
        } else {
            *elem_kind = EcsFromEntity;
            *source = bptr;
        }
        
        bptr = dot + 1;

        if (!bptr[0]) {
            return NULL;
        }
    }

    return bptr;
}

static
bool has_refs(
    ecs_signature_t *sig)
{
    uint32_t i, count = ecs_vector_count(sig->columns);
    ecs_signature_column_t *columns = ecs_vector_first(sig->columns);

    for (i = 0; i < count; i ++) {
        ecs_signature_from_kind_t from = columns[i].from;

        if (columns[i].op == EcsOperNot && from == EcsFromEmpty) {
            /* Special case: if oper kind is Not and the query contained a
             * shared expression, the expression is translated to FromId to
             * prevent resolving the ref */
            return true;

        } else if (from != EcsFromSelf && from != EcsFromEmpty) {
            /* If the component is not from the entity being iterated over, and
             * the column is not just passing an id, it must be a reference to
             * another entity. */
            return true;
        }
    }

    return false;
}

static
void postprocess(
    ecs_world_t *world,
    ecs_signature_t *sig)
{
    int i, count = ecs_vector_count(sig->columns);
    ecs_signature_column_t *columns = ecs_vector_first(sig->columns);

    for (i = 0; i < count; i ++) {
        ecs_signature_column_t *column = &columns[i];
        ecs_signature_op_kind_t op = column->op; 

        if (op == EcsOperOr) {
            /* If the signature explicitly indicates interest in EcsDisabled,
             * signal that disabled entities should be matched. By default,
             * disabled entities are not matched. */
            if (ecs_type_has_entity_intern(
                world, column->is.type, 
                ecs_entity(EcsDisabled), false))
            {
                sig->match_disabled = true;
            }

            /* If the signature explicitly indicates interest in EcsPrefab,
             * signal that disabled entities should be matched. By default,
             * prefab entities are not matched. */
            if (ecs_type_has_entity_intern(
                world, column->is.type, 
                ecs_entity(EcsPrefab), false))
            {
                sig->match_prefab = true;
            }            
        } else if (op == EcsOperAnd || op == EcsOperOptional) {
            if (column->is.component == ecs_entity(EcsDisabled)) {
                sig->match_disabled = true;
            } else if (column->is.component == ecs_entity(EcsPrefab)) {
                sig->match_prefab = true;
            }
        }

        if (sig->match_prefab && sig->match_disabled) {
            break;
        }
    }
}

/* -- Private functions -- */

/* Does expression require that a system matches with tables */
bool ecs_needs_tables(
    ecs_world_t *world,
    ecs_signature_t *sig)
{    
    int i, count = ecs_vector_count(sig->columns);
    ecs_signature_column_t *columns = ecs_vector_first(sig->columns);

    for (i = 0; i < count; i ++) {
        ecs_signature_column_t *elem = &columns[i];
        if (elem->from == EcsFromSelf || elem->from == EcsFromContainer) {
            return true;
        }
    }

    return false;
}

/** Count components in a signature */
uint32_t ecs_signature_columns_count(
    ecs_signature_t *sig)
{
    return ecs_vector_count(sig->columns);
}

/** Parse component expression */
int ecs_parse_component_expr(
    ecs_world_t *world,
    const char *sig,
    ecs_parse_action_t action,
    void *ctx)
{
    size_t len = strlen(sig);
    const char *ptr;
    char ch, *bptr, *buffer = ecs_os_malloc(len + 1);
    ecs_assert(buffer != NULL, ECS_OUT_OF_MEMORY, NULL);

    bool complex_expr = false;
    bool prev_is_0 = false;
    ecs_signature_from_kind_t elem_kind = EcsFromSelf;
    ecs_signature_op_kind_t oper_kind = EcsOperAnd;
    const char *source;

    for (bptr = buffer, ch = sig[0], ptr = sig; ch; ptr++) {
        ptr = skip_space(ptr);
        ch = *ptr;

        if (prev_is_0) {
            /* 0 can only apppear by itself */
            ecs_abort(ECS_INVALID_SIGNATURE, sig);
        }

        if (ch == ',' || ch == '|' || ch == '\0') {
            if (bptr == buffer) {
                ecs_abort(ECS_INVALID_SIGNATURE, sig);
            }

            *bptr = '\0';
            bptr = buffer;

            source = NULL;

            if (complex_expr) {
                ecs_signature_op_kind_t prev_oper_kind = oper_kind;
                bptr = parse_complex_elem(bptr, &elem_kind, &oper_kind, &source);
                if (!bptr) {
                    ecs_abort(ECS_INVALID_EXPRESSION, sig);
                }

                if (oper_kind == EcsOperNot && prev_oper_kind == EcsOperOr) {
                    ecs_abort(ECS_INVALID_EXPRESSION, sig);
                }
            }

           if (oper_kind == EcsOperOr) {
                if (elem_kind == EcsFromEmpty) {
                    /* Cannot OR handles */
                    ecs_abort(ECS_INVALID_EXPRESSION, sig);
                }
            }            

            if (!strcmp(bptr, "0")) {
                if (bptr != buffer) {
                    /* 0 can only appear by itself */
                    ecs_abort(ECS_INVALID_EXPRESSION, sig);
                }

                elem_kind = EcsFromEmpty;
                prev_is_0 = true;
            }

            char *source_id = NULL;
            if (source) {
                char *dot = strchr(source, '.');
                source_id = ecs_os_malloc(dot - source + 1);
                ecs_assert(source_id != NULL, ECS_OUT_OF_MEMORY, NULL);

                strncpy(source_id, source, dot - source);
                source_id[dot - source] = '\0';
            }

            int ret;
            if ((ret = action(
                world, elem_kind, oper_kind, bptr, source_id, ctx))) 
            {
                ecs_abort(ret, sig);
            }

            if (source_id) {
                ecs_os_free(source_id);
            }

            complex_expr = false;
            elem_kind = EcsFromSelf;

            if (ch == '|') {
                oper_kind = EcsOperOr;
            } else {
                oper_kind = EcsOperAnd;
            }

            bptr = buffer;
        } else {
            *bptr = ch;
            bptr ++;

            if (ch == '.' || ch == '!' || ch == '?' || ch == '$') {
                complex_expr = true;
            }
        }
    }

    ecs_os_free(buffer);
    return 0;
}

/** Parse signature expression */
int ecs_new_signature_action(
    ecs_world_t *world,
    ecs_signature_from_kind_t from,
    ecs_signature_op_kind_t op,
    const char *component_id,
    const char *source_id,
    void *data)
{
    ecs_signature_t *sig = data;

    /* Lookup component handly by string identifier */
    ecs_entity_t component = ecs_lookup(world, component_id);
    if (!component) {
        /* "0" is a valid expression used to indicate that a system matches no
         * components */
        if (strcmp(component_id, "0")) {
            ecs_abort(ECS_INVALID_COMPONENT_ID, component_id);
        } else {
            /* Don't add 0 component to signature */
            return 0;
        }
    }

    /* If retrieving a component from a system, only the AND operator is
     * supported. The set of system components is expected to be constant, and
     * thus no conditional operators are needed. */
    if (from == EcsFromSystem && op != EcsOperAnd) {
        return ECS_INVALID_SIGNATURE;
    }

    ecs_signature_column_t *elem;

    /* AND (default) and optional columns are stored the same way */
    if (op == EcsOperAnd || op == EcsOperOptional) {
        elem = ecs_vector_add(&sig->columns, ecs_signature_column_t);
        elem->from = from;
        elem->op = op;          
        elem->is.component = component;

        if (from == EcsFromEntity) {
            elem->source = ecs_lookup(world, source_id);
            if (!elem->source) {
                ecs_abort(ECS_UNRESOLVED_IDENTIFIER, source_id);
            }

            ecs_set_watch(world, &world->main_stage, elem->source);
        }

    /* OR columns store a type id instead of a single component */
    } else if (op == EcsOperOr) {
        elem = ecs_vector_last(sig->columns, ecs_signature_column_t);
        if (elem->op == EcsOperAnd) {
            elem->is.type = ecs_type_add_intern(
                world, NULL, 0, elem->is.component);
        } else {
            if (elem->from != from) {
                /* Cannot mix FromEntity and FromComponent in OR */
                goto error;
            }
        }

        elem->from = from;
        elem->op = op;  
        elem->is.type = ecs_type_add_intern(
            world, NULL, elem->is.type, component);

    /* A system stores two NOT familes; one for entities and one for components.
     * These can be quickly & efficiently used to exclude tables with
     * ecs_type_contains. */
    } else if (op == EcsOperNot) {
        elem = ecs_vector_add(&sig->columns, ecs_signature_column_t);
        elem->is.component = component;
        elem->from = EcsFromEmpty;
        elem->op = op;  
    } else if (from == EcsFromEntity) {
        elem = ecs_vector_add(&sig->columns, ecs_signature_column_t);
        elem->from = from;
        elem->source = ecs_lookup(world, source_id);
        elem->is.component = component;
    }

    return 0;
error:
    return -1;
}

ecs_signature_t ecs_new_signature(
    ecs_world_t *world,
    const char *signature)
{
    ecs_signature_t result = {
        .expr = signature
    };

    ecs_parse_component_expr(
        world, signature, ecs_new_signature_action, &result);

    postprocess(world, &result);

    result.has_refs = has_refs(&result);
    result.owned = true;

    return result;
}

void ecs_signature_free(
    ecs_signature_t *sig)
{
    if (sig->owned) {
        ecs_vector_free(sig->columns);
        sig->columns = NULL;
        sig->owned = false;
    }
}
