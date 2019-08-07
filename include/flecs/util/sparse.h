#ifndef FLECS_CHUNKED_H
#define FLECS_CHUNKED_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ecs_sparse_t ecs_sparse_t;

FLECS_EXPORT
ecs_sparse_t* _ecs_sparse_new(
    uint32_t element_size,
    uint32_t element_count);

#define ecs_sparse_new(type, element_count)\
    _ecs_sparse_new(sizeof(type), element_count)

FLECS_EXPORT
void ecs_sparse_free(
    ecs_sparse_t *chunked);

FLECS_EXPORT
void ecs_sparse_clear(
    ecs_sparse_t *chunked);

FLECS_EXPORT
void* _ecs_sparse_add(
    ecs_sparse_t *chunked,
    uint32_t size);

#define ecs_sparse_add(chunked, type)\
    ((type*)_ecs_sparse_add(chunked, sizeof(type)))

FLECS_EXPORT
void* _ecs_sparse_remove(
    ecs_sparse_t *chunked,
    uint32_t size,
    uint32_t index);

#define ecs_sparse_remove(chunked, type, index)\
    ((type*)_ecs_sparse_remove(chunked, sizeof(type), index))

FLECS_EXPORT
void* _ecs_sparse_get(
    const ecs_sparse_t *chunked,
    uint32_t size,
    uint32_t index);

#define ecs_sparse_get(chunked, type, index)\
    ((type*)_ecs_sparse_get(chunked, sizeof(type), index))

FLECS_EXPORT
uint32_t ecs_sparse_count(
    const ecs_sparse_t *chunked);

FLECS_EXPORT
uint32_t ecs_sparse_size(
    const ecs_sparse_t *chunked);

FLECS_EXPORT
void* _ecs_sparse_get_sparse(
    const ecs_sparse_t *chunked,
    uint32_t size,
    uint32_t index);

#define ecs_sparse_get_sparse(chunked, type, index)\
    ((type*)_ecs_sparse_get_sparse(chunked, sizeof(type), index))

FLECS_EXPORT
void* _ecs_sparse_get_or_set_sparse(
    ecs_sparse_t *chunked,
    uint32_t element_size,
    uint32_t index,
    bool *is_new);

#define ecs_sparse_get_or_set_sparse(chunked, type, index, is_new)\
    ((type*)_ecs_sparse_get_or_set_sparse(chunked, sizeof(type), index, is_new))

FLECS_EXPORT
const uint32_t* ecs_sparse_indices(
    const ecs_sparse_t *chunked);

FLECS_EXPORT
void ecs_sparse_set_size(
    ecs_sparse_t *chunked,
    uint32_t size);

FLECS_EXPORT
void ecs_sparse_grow(
    ecs_sparse_t *chunked,
    uint32_t count);

FLECS_EXPORT
void ecs_sparse_memory(
    ecs_sparse_t *chunked,
    uint32_t *allocd,
    uint32_t *used);

#ifdef __cplusplus
}
#endif

#endif
