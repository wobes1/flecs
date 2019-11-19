#ifndef FLECS_PRIVATE_H
#define FLECS_PRIVATE_H

/* This file contains declarations to private flecs functions */

#include "types.h"

/* -- Entity API -- */

ecs_record_t* ecs_set_entity_in_main(
    ecs_world_t *world,
    ecs_entity_t entity);

void ecs_set_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t *row);

ecs_record_t* ecs_get_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity);

/* Merge entity with stage */
void ecs_merge_entity(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_record_t staged_row);

/* Get prefab from type, even if type was introduced while in progress */
ecs_entity_t ecs_get_prefab_from_type(
    ecs_world_t *world,
    ecs_stage_t *stage,
    bool is_new_table,
    ecs_entity_t entity,
    ecs_type_t type);

/* Mark an entity as being watched. This is used to trigger automatic rematching
 * when entities used in system expressions change their components. */
void ecs_set_watch(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity);

/* Does one of the entity containers has specified component */
bool ecs_components_contains_component(
    ecs_world_t *world,
    ecs_type_t table_type,
    ecs_entity_t component,
    ecs_entity_t flags,
    ecs_entity_t *entity_out);

/* Get pointer to a component */
void* ecs_get_ptr_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t entity,
    ecs_entity_t component,
    bool staged_only,
    bool search_prefab);

ecs_entity_t ecs_get_entity_for_component(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type,
    ecs_entity_t component);


/* -- World API -- */

/* Notify systems that there is a new table, which triggers matching */
void ecs_notify_systems_of_table(
    ecs_world_t *world,
    ecs_table_t *table);

/* Activate system (move from inactive array to on_update array or vice versa) */
void ecs_world_activate_system(
    ecs_world_t *world,
    ecs_entity_t system,
    ecs_system_kind_t kind,
    bool active);

/* Get current thread-specific stage */
ecs_stage_t *ecs_get_stage(
    ecs_world_t **world_ptr);

/* -- Stage API -- */

/* Initialize stage data structures */
void ecs_stage_init(
    ecs_world_t *world,
    ecs_stage_t *stage);

/* Deinitialize stage */
void ecs_stage_fini(
    ecs_world_t *world,
    ecs_stage_t *stage);

/* Merge stage with main stage */
void ecs_stage_merge(
    ecs_world_t *world,
    ecs_stage_t *stage);

/* -- Type utility API -- */

/* Merge add/remove families */
ecs_type_t ecs_type_merge_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t cur,
    ecs_type_t to_add,
    ecs_type_t to_remove,
    ecs_entity_array_t *to_add_except,
    ecs_entity_array_t *to_remove_intersect);

/* Test if type_1 contains type_2 */
bool ecs_type_contains(
    ecs_world_t *world,
    ecs_type_t type_1,
    ecs_type_t type_2,
    bool match_all,
    bool match_prefab,
    ecs_entity_t *found);

/* Test if type contains component */
bool ecs_type_has_entity_intern(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_entity_t component,
    bool match_prefab);

/* Add component to type */
ecs_type_t ecs_type_add_intern(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t type,
    ecs_entity_t component);

/* Get index for entity in type */
int16_t ecs_type_index_of(
    ecs_type_t type,
    ecs_entity_t component);

/* Get number of containers (parents) for a type */
int32_t ecs_type_container_depth(
   ecs_world_t *world,
   ecs_type_t type,
   ecs_entity_t component);

/** Utility to iterate over prefabs in type */
int32_t ecs_type_get_prefab(
    ecs_type_t type,
    int32_t n);

/* Find entity in prefabs of type */
ecs_entity_t ecs_find_entity_in_prefabs(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_type_t type,
    ecs_entity_t component,
    ecs_entity_t previous);

/* Match type with type filter */
bool ecs_type_match_w_filter(
    ecs_world_t *world,
    ecs_type_t type,
    ecs_type_filter_t *filter);

/*? Find table for type */
ecs_table_t* ecs_type_find_table(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_type_t type);

/* -- Table API -- */

/* Initialize root table node */
void ecs_init_root_table(
    ecs_world_t *world);  

/* Find existing or create new table */
ecs_table_t *ecs_table_find_or_create(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_array_t *entities);  

/* Find or create table, starting from existing table */
ecs_table_t *ecs_table_traverse(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_entity_array_t *to_add,
    ecs_entity_array_t *to_remove,
    ecs_entity_array_t *added,
    ecs_entity_array_t *removed);

/* Cleanup table resources */
void ecs_table_fini(
    ecs_world_t *world,
    ecs_table_t *table);

/* Clear table data */
void ecs_table_clear(
    ecs_world_t *world,
    ecs_table_t *table);

/* Return number of entities in table */
uint64_t ecs_table_count(
    ecs_table_t *table);

/* Return size of table row */
uint32_t ecs_table_row_size(
    ecs_table_t *table);

/* Return size of table row */
uint32_t ecs_table_rows_dimensioned(
    ecs_table_t *table);    

/* Delete row from table */
void ecs_table_delete(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t index);

/* Get row from table (or stage) */
void* ecs_table_get(
    ecs_table_t *table,
    ecs_vector_t *rows,
    uint32_t index);

/* Test if table has component */
bool ecs_table_has_components(
    ecs_table_t *table,
    ecs_vector_t *components);

/* Deinitialize table. This invokes all matching on_remove systems */
void ecs_table_deinit(
    ecs_world_t *world,
    ecs_table_t *table);

/* Get stage-specific columns from table */
ecs_columns_t* ecs_table_get_columns(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table);

/* -- Column API -- */

/* Create columns for table */
ecs_columns_t* ecs_columns_new(
    ecs_world_t *world,
    ecs_table_t *table);

void ecs_columns_free(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns);

/* Free columns */
void ecs_column_free(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns);

/* Insert row into columns */
uint32_t ecs_columns_insert(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    ecs_entity_t entity,
    ecs_record_t *record);

/* Delete entity from columns */
void ecs_columns_delete(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    int32_t sindex);

/* Return number of entities in columns */
uint64_t ecs_columns_count(
    ecs_columns_t *columns);

/* Grow columns with specified size */
uint32_t ecs_columns_grow(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t count,
    ecs_entity_t first_entity);

/* Dimension columns to have n rows (does not add entities) */
int16_t ecs_columns_set_size(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t count);

/* Swap two entities */
void ecs_columns_swap(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    int32_t row_1,
    int32_t row_2,
    ecs_record_t *row_ptr_1,
    ecs_record_t *row_ptr_2);

/* Swap n entities with entity preceding them */
void ecs_columns_copy_back_and_swap(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_table_t *table,
    ecs_columns_t *columns,
    uint32_t row,
    uint32_t count);  

void ecs_columns_merge(
    ecs_world_t *world,
    ecs_table_t *new_table,
    ecs_table_t *old_table);    

/* Move row from one set of columns to (partially) overlapping set of columns */
void ecs_columns_copy(
    ecs_type_t dst_type,
    ecs_columns_t *dst_columns,
    int32_t dst_index,
    ecs_type_t src_type,
    ecs_columns_t *src_columns,
    int32_t src_index);

/* Clear column data */
void ecs_columns_clear(
    ecs_table_t *table,
    ecs_columns_t *columns);      

/* -- System API -- */

/* Create new table system */
ecs_entity_t ecs_col_system_new(
    ecs_world_t *world,
    const char *id,
    ecs_system_kind_t kind,
    ecs_signature_t *sig,
    ecs_system_action_t action);

void ecs_col_system_free(
    EcsColSystem *system_data);

/* Notify row system of a new type, which initiates system-type matching */
void ecs_row_system_notify_of_type(
    ecs_world_t *world,
    ecs_stage_t *stage,
    ecs_entity_t system,
    ecs_type_t type);

/* Activate table for system (happens if table goes from empty to not empty) */
void ecs_system_activate_table(
    ecs_world_t *world,
    ecs_entity_t system,
    ecs_table_t *table,
    bool active);

/* Run a task (periodic system that is not matched against any tables) */
void ecs_run_task(
    ecs_world_t *world,
    ecs_entity_t system);

/* Invoke row system */
void ecs_run_row_system(
    ecs_world_t *world,
    ecs_entity_t system,
    ecs_table_t *table,
    ecs_columns_t *table_columns,
    uint32_t offset,
    uint32_t limit);

/* Callback for parse_component_expr that stores result as ecs_signature_column_t's */
int ecs_new_signature_action(
    ecs_world_t *world,
    ecs_signature_from_kind_t elem_kind,
    ecs_signature_op_kind_t oper_kind,
    const char *component_id,
    const char *source_id,
    void *data);

/* -- Query API -- */

void ecs_query_match_table(
    ecs_world_t *world,
    ecs_query_t *query,
    ecs_table_t *table);

void ecs_query_free(
    ecs_query_t *query);

/* -- Worker API -- */

/* Compute schedule based on current number of entities matching system */
void ecs_schedule_jobs(
    ecs_world_t *world,
    ecs_entity_t system);

/* Prepare jobs */
void ecs_prepare_jobs(
    ecs_world_t *world,
    ecs_entity_t system);

/* Run jobs */
void ecs_run_jobs(
    ecs_world_t *world);

/* -- Os time api -- */

void ecs_os_time_setup(void);
uint64_t ecs_os_time_now(void);
void ecs_os_time_sleep(unsigned int sec, unsigned int nanosec);


/* -- Private utilities -- */

/* Convert 64bit value to ecs_record_t type. ecs_record_t is stored as 64bit int in the
 * entity index */
ecs_record_t ecs_to_row(
    uint64_t value);

/* Get 64bit integer from ecs_record_t */
uint64_t ecs_from_row(
    ecs_record_t row);

/* Utility that parses system signature */
int ecs_parse_component_expr(
    ecs_world_t *world,
    const char *sig,
    ecs_parse_action_t action,
    void *ctx);

/* Test whether signature has columns that must be retrieved from a table */
bool ecs_needs_tables(
    ecs_world_t *world,
    ecs_signature_t *sig);

/* Count number of columns signature */
uint32_t ecs_signature_columns_count(
    ecs_signature_t *sig);

#define assert_func(cond) _assert_func(cond, #cond, __FILE__, __LINE__, __func__)
void _assert_func(
    bool cond,
    const char *cond_str,
    const char *file,
    uint32_t line,
    const char *func);

#endif
