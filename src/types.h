#ifndef FLECS_TYPES_PRIVATE_H
#define FLECS_TYPES_PRIVATE_H

#ifndef __MACH__
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>
#include <math.h>

#ifdef _MSC_VER
//FIXME
#else
#include <sys/param.h>  /* attempt to define endianness */
#endif
#ifdef linux
# include <endian.h>    /* attempt to define endianness */
#endif

#include <flecs.h>

#define ECS_WORLD_INITIAL_TABLE_COUNT (2)
#define ECS_WORLD_INITIAL_ENTITY_COUNT (2)
#define ECS_WORLD_INITIAL_STAGING_COUNT (0)
#define ECS_WORLD_INITIAL_COL_SYSTEM_COUNT (1)
#define ECS_WORLD_INITIAL_OTHER_SYSTEM_COUNT (0)
#define ECS_WORLD_INITIAL_ADD_SYSTEM_COUNT (0)
#define ECS_WORLD_INITIAL_REMOVE_SYSTEM_COUNT (0)
#define ECS_WORLD_INITIAL_SET_SYSTEM_COUNT (0)
#define ECS_WORLD_INITIAL_PREFAB_COUNT (0)
#define ECS_MAP_INITIAL_NODE_COUNT (4)
#define ECS_TABLE_INITIAL_ROW_COUNT (0)
#define ECS_SYSTEM_INITIAL_TABLE_COUNT (0)
#define ECS_MAX_JOBS_PER_WORKER (16)
#define ECS_MAX_COMPONENTS (1024)

/* This is _not_ the max number of entities that can be of a given type. This 
 * constant defines the maximum number of components, prefabs and parents can be
 * in one type. This limit serves two purposes: detect errors earlier (assert on
 * very large types) and allow for more efficient allocation strategies (like
 * using alloca for temporary buffers). */
#define ECS_MAX_ENTITIES_IN_TYPE (256)

#define ECS_WORLD_MAGIC (0x65637377)
#define ECS_THREAD_MAGIC (0x65637374)


/* -- Builtin component types -- */

/** Metadata of an explicitly created type (ECS_TYPE or ecs_new_type) */
typedef struct EcsTypeComponent {
    ecs_type_t type;    /* Preserved nested families */
    ecs_type_t normalized;  /* Resolved nested families */
} EcsTypeComponent;

/* For prefabs with child entities, the parent prefab must be marked so that
 * flecs knows not to share components from it, as adding a prefab as a parent
 * is stored in the same way as adding a prefab for sharing components.
 * There are two mechanisms required to accomplish this. The first one is to set
 * the 'parent' member in the EcsPrefab component, for the child entity of the
 * prefab. This acts as a front-end for another mechanism, that ensures that
 * child entities for different prefab parents are added to different tables. As
 * a result of setting a parent in EcsPrefab, Flecs will:
 * 
 *  - Add the prefab to the entity type
 *  - Find or create a prefab parent flag entity
 *  - Set the EcsPrefabParent component on the prefab parent flag entity
 *  - Add the prefab parent flag entity to the child
 * 
 * The last step ensures that the type of the child entity is associated with at
 * most one prefab parent. If the mechanism would just rely on the EcsPrefab
 * parent field, it would theoretically be possible that childs for different
 * prefab parents end up in the same table.
 */
typedef struct EcsPrefabParent {
    ecs_entity_t parent;
} EcsPrefabParent;

typedef struct ecs_builder_op_t {
    const char *id;
    ecs_type_t type;
} ecs_builder_op_t;

typedef struct EcsPrefabBuilder {
    ecs_vector_t *ops; /* ecs_builder_op_t */
} EcsPrefabBuilder;


/* -- Entity storage -- */

#define EcsTableIsStaged  (1)
#define EcsTableIsPrefab (2)
#define EcsTableHasPrefab (4)

typedef struct ecs_table_t ecs_table_t;

/** A single column in a table */
typedef struct ecs_column_t {
    ecs_vector_t *data;              /* Column data */
    uint16_t size;                   /* Column size (saves component lookups) */
} ecs_column_t;

typedef struct ecs_edge_t {
    ecs_table_t *add;
    ecs_table_t *remove;
} ecs_edge_t;

/* A table stores component data. Tables are stored in a graph that is traversed
 * when adding/removing components. */
struct ecs_table_t {
    ecs_type_t type;                  /* Type containing component ids */
    ecs_column_t *columns;            /* Columns storing components of array */

    ecs_edge_t *edges;                /* Edges to other tables */

    ecs_vector_t *on_frame;           /* Column systems matched with table */
    ecs_vector_t *on_new;             /* Systems executed when new entity is
                                       * created in this table. */

    uint32_t flags;                   /* Flags for testing table properties */
};

/* A record contains the table and row at which the entity is stored. */
typedef struct ecs_record_t {
    ecs_table_t *table;           /* The type of the entity */
    int32_t row;                  /* Row at which the entity is stored */
} ecs_record_t;


/* -- Components -- */

/** Component-specific data */
typedef struct ecs_component_data_t {
    ecs_vector_t *on_add;       /* Systems ran after adding this component */
    ecs_vector_t *on_remove;    /* Systems ran after removing this component */
    ecs_vector_t *on_set;       /* Systems ran after setting this component */

    ecs_init_t init;            /* Invoked for new uninitialized component */
    ecs_init_t fini;            /* Invoked when component is deinitialized */
    ecs_replace_t replace;      /* Invoked when component value is replaced */
    ecs_merge_t merge;          /* Invoked when component value is merged */

    void *ctx;
} ecs_component_data_t;


/* -- Systems -- */

/** Type that is used by systems to indicate where to fetch a component from */
typedef enum ecs_signature_from_kind_t {
    EcsFromSelf,            /* Get component from self (default) */
    EcsFromOwned,           /* Get owned component from self */
    EcsFromShared,          /* Get shared component from self */
    EcsFromContainer,       /* Get component from container */
    EcsFromSystem,          /* Get component from system */
    EcsFromEmpty,           /* Get entity handle by id */
    EcsFromEntity,          /* Get component from other entity */
    EcsCascade              /* Walk component in cascading (hierarchy) order */
} ecs_signature_from_kind_t;

/** Type describing an operator used in an signature of a system signature */
typedef enum ecs_signature_op_kind_t {
    EcsOperAnd = 0,
    EcsOperOr = 1,
    EcsOperNot = 2,
    EcsOperOptional = 3,
    EcsOperLast = 4
} ecs_signature_op_kind_t;

/** Callback used by the system signature expression parser */
typedef int (*ecs_parse_action_t)(
    ecs_world_t *world,
    ecs_signature_from_kind_t elem_kind,
    ecs_signature_op_kind_t oper_kind,
    const char *component,
    const char *source,
    void *ctx);

/** Type that describes a single column in the system signature */
typedef struct ecs_signature_column_t {
    ecs_signature_from_kind_t from;       /* Element kind (Entity, Component) */
    ecs_signature_op_kind_t op;           /* Operator kind (AND, OR, NOT) */
    union {
        ecs_type_t type;             /* Used for OR operator */
        ecs_entity_t component;      /* Used for AND operator */
    } is;
    ecs_entity_t source;             /* Source entity (used with FromEntity) */
} ecs_signature_column_t;

/** Type containing data for a table matched with a system */
typedef struct ecs_matched_table_t {
    ecs_table_t *table;             /* Reference to the table */
    int32_t *columns;               /* Mapping of system columns to table */
    ecs_entity_t *components;       /* Actual components of system columns */
    ecs_vector_t *references;       /* Reference columns and cached pointers */
    int32_t depth;                  /* Depth of table (when using CASCADE) */
} ecs_matched_table_t;

/** Query that is automatically matched against active tables */
struct ecs_query_t {
    /* Signature of query */
    ecs_signature_t sig;

    /* Tables matched with query */
    ecs_vector_t *tables;

    /* Precomputed types for quick comparisons */
    ecs_type_t not_from_self;      /* Exclude components from self */
    ecs_type_t not_from_owned;     /* Exclude components from self only if owned */
    ecs_type_t not_from_shared;    /* Exclude components from self only if shared */
    ecs_type_t not_from_component; /* Exclude components from components */
    ecs_type_t and_from_self;      /* Which components are required from entity */
    ecs_type_t and_from_owned;     /* Which components are required from entity */
    ecs_type_t and_from_shared;    /* Which components are required from entity */
    ecs_type_t and_from_system;    /* Used to auto-add components to system */

    ecs_entity_t system;           /* Handle to system */
};

/** Base type for a system */
typedef struct EcsSystem {
    ecs_system_action_t action;    /* Callback to be invoked for matching rows */
    EcsSystemKind kind;            /* Kind of system */
    float time_spent;              /* Time spent on running system */
    bool enabled;                  /* Is system enabled or not */
} EcsSystem;

/** A column system is a system that is ran periodically (default = every frame)
 * on all entities that match the system signature expression. Column systems
 * are prematched with tables (archetypes) that match the system signature
 * expression. Each time a column system is invoked, it iterates over the 
 * matched list of tables (the 'tables' member). 
 *
 * For each table, the system stores a list of the components that were matched
 * with the system. This list may differ from the component list of the table,
 * when OR expressions or optional expressions are used.
 * 
 * A column system keeps track of tables that are empty. These tables are stored
 * in the 'inactive_tables' array. This prevents the system from iterating over
 * tables in the main loop that have no data.
 * 
 * For each table, a column system stores an index that translates between the
 * a column in the system signature, and the matched table. This information is
 * stored, alongside with an index that identifies the table, in the 'tables'
 * member. This is an array of an array of integers, per table.
 * 
 * Additionally, the 'tables' member contains information on where a component
 * should be fetched from. By default, components are fetched from an entity,
 * but a system may specify that a component must be resolved from a container,
 * or must be fetched from a prefab. In this case, the index to lookup a table
 * column from system column contains a negative number, which identifies an
 * element in the 'refs' array.
 * 
 * The 'refs' array contains elements of type 'EcsRef', and stores references
 * to external entities. References can vary per table, but not per entity/row,
 * as prefabs / containers are part of the entity type, which in turn 
 * identifies the table in which the entity is stored.
 * 
 * The 'period' and 'time_passed' members are used for periodic systems. An
 * application may specify that a system should only run at a specific interval, 
 * like once per second. This interval is stored in the 'period' member. Each
 * time the system is evaluated but not ran, the delta_time is added to the 
 * time_passed member, until it exceeds 'period'. In that case, the system is
 * ran, and 'time_passed' is decreased by 'period'. 
 */
typedef struct EcsColSystem {
    EcsSystem base;
    ecs_entity_t entity;                  /* Entity id of system, used for ordering */
    ecs_query_t *query;                   /* System query */
    ecs_vector_t *jobs;                   /* Jobs for this system */
    ecs_vector_params_t column_params;    /* Parameters for type_columns */
    ecs_vector_params_t component_params; /* Parameters for components */
    ecs_vector_params_t ref_params;       /* Parameters for refs */
    float period;                         /* Minimum period inbetween system invocations */
    float time_passed;                    /* Time passed since last invocation */
} EcsColSystem;

/** A row system is a system that is ran on 1..n entities for which a certain 
 * operation has been invoked. The system kind determines on what kind of
 * operation the row system is invoked. Example operations are ecs_add,
 * ecs_remove and ecs_set. */
typedef struct EcsRowSystem {
    EcsSystem base;
    ecs_signature_t sig;            /* System signature */
    ecs_vector_t *components;       /* Components in order of signature */
} EcsRowSystem;


/* -- Staging -- */

/** A stage is a data structure in which delta's are stored until it is safe to
 * merge those delta's with the main world stage. A stage allows flecs systems
 * to arbitrarily add/remove/set components and create/delete entities while
 * iterating. Additionally, worker threads have their own stage that lets them
 * mutate the state of entities without requiring locks. */
typedef struct ecs_stage_t {
    /* If this is not main stage, 
     * changes to the entity index 
     * are buffered here */
    ecs_map_t *entity_index;       /* Entity lookup table for (table, row) */

    /* These occur only in
     * temporary stages, and
     * not on the main stage */
    ecs_map_t *data_stage;         /* Arrays with staged component values */
    ecs_map_t *remove_merge;       /* All removed components before merge */
    
    /* Is entity range checking enabled? */
    bool range_check_enabled;
} ecs_stage_t;


/* -- Threading -- */

/** A type describing a unit of work to be executed by a worker thread. */ 
typedef struct ecs_job_t {
    ecs_entity_t system;          /* System handle */
    EcsColSystem *system_data;    /* System to run */
    uint32_t offset;              /* Start index in row chunk */
    uint32_t limit;               /* Total number of rows to process */
} ecs_job_t;

/** A type desribing a worker thread. When a system is invoked by a worker
 * thread, it receives a pointer to an ecs_thread_t instead of a pointer to an 
 * ecs_world_t (provided by the ecs_rows_t type). When this ecs_thread_t is passed down
 * into the flecs API, the API functions are able to tell whether this is an
 * ecs_thread_t or an ecs_world_t by looking at the 'magic' number. This allows the
 * API to transparently resolve the stage to which updates should be written,
 * without requiring different API calls when working in multi threaded mode. */
typedef struct ecs_thread_t {
    uint32_t magic;                           /* Magic number to verify thread pointer */
    uint32_t job_count;                       /* Number of jobs scheduled for thread */
    ecs_world_t *world;                       /* Reference to world */
    ecs_job_t *jobs[ECS_MAX_JOBS_PER_WORKER]; /* Array with jobs */
    ecs_stage_t *stage;                       /* Stage for thread */
    ecs_os_thread_t thread;                   /* Thread handle */
    uint16_t index;                           /* Index of thread */
} ecs_thread_t;


/* -- Utility types -- */

/** Supporting type that internal functions pass around to ensure that data
 * related to an entity is only looked up once. */
typedef struct ecs_entity_info_t {
    ecs_entity_t entity;        /* Entity identifier */
    ecs_record_t *record;       /* Record in entity index */
    ecs_table_t *table;         /* Table of entity */
    ecs_column_t *columns;      /* Columns in which entity data is stored */
    ecs_type_t type;            /* Type of entity */
    int32_t row;                /* Row in which the entity is stored */
    bool is_watched;            /* Is entity being watched */
} ecs_entity_info_t;

/** Simple array of entities. Used for passing entity arrays when an ecs_type_t
 * vector would have unnecessary overhead. */
typedef struct ecs_entity_array_t {
    ecs_entity_t *array;
    int32_t count;
} ecs_entity_array_t;


/* -- World -- */

/** The world stores and manages all ECS data. An application can have more than
 * one world, but data is not shared between worlds. */
struct ecs_world {
    uint32_t magic;               /* Magic number to verify world pointer */
    float delta_time;             /* Time passed to or computed by ecs_progress */
    void *context;                /* Application context */

    /* -- Component data */
    ecs_vector_t *component_data;

    /* -- Column systems -- */

    ecs_vector_t *on_load_systems;  
    ecs_vector_t *post_load_systems;  
    ecs_vector_t *pre_update_systems;  
    ecs_vector_t *on_update_systems;   
    ecs_vector_t *on_validate_systems; 
    ecs_vector_t *post_update_systems; 
    ecs_vector_t *pre_store_systems; 
    ecs_vector_t *on_store_systems;   
    ecs_vector_t *on_demand_systems;  
    ecs_vector_t *inactive_systems;   

    ecs_sparse_t *queries;


    /* -- Tasks -- */

    ecs_vector_t *tasks;              /* Periodic actions not invoked on entities */
    ecs_vector_t *fini_tasks;         /* Tasks to execute on ecs_fini */


    /* -- Lookup Indices -- */

    ecs_map_t *prefab_parent_index;   /* Index to find flag for prefab parent */
    ecs_map_t *type_handles;          /* Handles to named families */


    /* -- Entity storage -- */

    ecs_sparse_t *entity_index;      /* Entity index of main stage */
    ecs_record_t singleton;          /* Singleton record is stored separately */
    ecs_table_t table_root;          /* Root to table graph */
    ecs_sparse_t *tables;            /* Iterable storage for tables */


    /* -- Staging -- */

    ecs_stage_t main_stage;          /* Main storage */
    ecs_stage_t temp_stage;          /* Stage for when processing systems */
    ecs_vector_t *worker_stages;     /* Stages for worker threads */


    /* -- Multithreading -- */

    ecs_vector_t *worker_threads;    /* Worker threads */
    ecs_os_cond_t thread_cond;       /* Signal that worker threads can start */
    ecs_os_mutex_t thread_mutex;     /* Mutex for thread condition */
    ecs_os_cond_t job_cond;          /* Signal that worker thread job is done */
    ecs_os_mutex_t job_mutex;        /* Mutex for protecting job counter */
    uint32_t jobs_finished;          /* Number of jobs finished */
    uint32_t threads_running;        /* Number of threads running */

    ecs_entity_t last_handle;        /* Last issued handle */
    ecs_entity_t last_component;     /* Last issued handle for components */
    ecs_entity_t min_handle;         /* First allowed handle */
    ecs_entity_t max_handle;         /* Last allowed handle */


    /* -- Handles to builtin components families -- */

    ecs_table_t *t_component;
    ecs_table_t *t_type;
    ecs_table_t *t_prefab;
    ecs_table_t *t_row_system;
    ecs_table_t *t_col_system;


    /* -- Time management -- */

    uint32_t tick;                /* Number of computed frames by world */
    ecs_time_t frame_start;       /* Starting timestamp of frame */
    float frame_time;             /* Time spent processing a frame */
    float system_time;            /* Time spent processing systems */
    float merge_time;             /* Time spent on merging */
    float target_fps;             /* Target fps */
    float fps_sleep;              /* Sleep time to prevent fps overshoot */
    float world_time;             /* Time since start of simulation */


    /* -- Settings from command line arguments -- */

    int arg_fps;
    int arg_threads;


    /* -- World state -- */

    bool valid_schedule;          /* Is job schedule still valid */
    bool quit_workers;            /* Signals worker threads to quit */
    bool in_progress;             /* Is world being progressed */
    bool is_merging;              /* Is world currently being merged */
    bool auto_merge;              /* Are stages auto-merged by ecs_progress */
    bool measure_frame_time;      /* Time spent on each frame */
    bool measure_system_time;     /* Time spent by each system */
    bool should_quit;             /* Did a system signal that app should quit */
    bool should_match;            /* Should table be rematched */
    bool should_resolve;          /* If a table reallocd, resolve system refs */
}; 


/* Parameters for various array types */
extern const ecs_vector_params_t handle_arr_params;
extern const ecs_vector_params_t stage_arr_params;
extern const ecs_vector_params_t table_arr_params;
extern const ecs_vector_params_t thread_arr_params;
extern const ecs_vector_params_t job_arr_params;
extern const ecs_vector_params_t builder_params;
extern const ecs_vector_params_t system_column_params;
extern const ecs_vector_params_t matched_table_params;
extern const ecs_vector_params_t matched_column_params;
extern const ecs_vector_params_t reference_params;

#endif
