#ifndef FLECS_CHUNKED_H
#define FLECS_CHUNKED_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ecs_chunked_t ecs_chunked_t;

FLECS_EXPORT
ecs_chunked_t* _ecs_chunked_new(
    uint32_t element_size,
    uint32_t element_count);

#define ecs_chunked_new(type, element_count)\
    _ecs_chunked_new(sizeof(type), element_count)

FLECS_EXPORT
void ecs_chunked_free(
    ecs_chunked_t *chunked);

FLECS_EXPORT
void ecs_chunked_clear(
    ecs_chunked_t *chunked);

FLECS_EXPORT
void* _ecs_chunked_add(
    ecs_chunked_t *chunked,
    uint32_t size);

#define ecs_chunked_add(chunked, type)\
    ((type*)_ecs_chunked_add(chunked, sizeof(type)))

FLECS_EXPORT
void* _ecs_chunked_remove(
    ecs_chunked_t *chunked,
    uint32_t size,
    uint32_t index);

#define ecs_chunked_remove(chunked, type, index)\
    ((type*)_ecs_chunked_remove(chunked, sizeof(type), index))

FLECS_EXPORT
void* _ecs_chunked_get(
    const ecs_chunked_t *chunked,
    uint32_t size,
    uint32_t index);

#define ecs_chunked_get(chunked, type, index)\
    ((type*)_ecs_chunked_get(chunked, sizeof(type), index))

FLECS_EXPORT
uint32_t ecs_chunked_count(
    const ecs_chunked_t *chunked);

FLECS_EXPORT
uint32_t ecs_chunked_size(
    const ecs_chunked_t *chunked);

FLECS_EXPORT
void* _ecs_chunked_get_sparse(
    const ecs_chunked_t *chunked,
    uint32_t size,
    uint32_t index);

#define ecs_chunked_get_sparse(chunked, type, index)\
    ((type*)_ecs_chunked_get_sparse(chunked, sizeof(type), index))

FLECS_EXPORT
void* _ecs_chunked_get_or_set_sparse(
    ecs_chunked_t *chunked,
    uint32_t element_size,
    uint32_t index,
    bool *is_new);

#define ecs_chunked_get_or_set_sparse(chunked, type, index, is_new)\
    ((type*)_ecs_chunked_get_or_set_sparse(chunked, sizeof(type), index, is_new))

FLECS_EXPORT
const uint32_t* ecs_chunked_indices(
    const ecs_chunked_t *chunked);

FLECS_EXPORT
void ecs_chunked_set_size(
    ecs_chunked_t *chunked,
    uint32_t size);

FLECS_EXPORT
void ecs_chunked_grow(
    ecs_chunked_t *chunked,
    uint32_t count);

FLECS_EXPORT
void ecs_chunked_memory(
    ecs_chunked_t *chunked,
    uint32_t *allocd,
    uint32_t *used);

#ifdef __cplusplus
}
#endif

#endif
