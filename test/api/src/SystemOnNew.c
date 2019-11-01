#include <api.h>

static
void Init(ecs_rows_t *rows) {
    ECS_COLUMN(rows, Position, p, 1);
    
    Velocity *v = NULL;
    if (rows->column_count >= 2) {
        v = ecs_column(rows, Velocity, 2);
    }

    ProbeSystem(rows);

    int i;
    for (i = 0; i < rows->count; i ++) {
        p[i].x = 10;
        p[i].y = 20;

        if (v) {
            v[i].x = 30;
            v[i].y = 40;
        }
    }
}

void SystemOnNew_new_match_1_of_1() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_SYSTEM(world, Init, EcsOnNew, Position);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new(world, Position);
    test_assert(e != 0);

    test_int(ctx.count, 1);
    test_int(ctx.invoked, 1);
    test_int(ctx.system, Init);
    test_int(ctx.column_count, 1);
    test_null(ctx.param);

    test_int(ctx.e[0], e);
    test_int(ctx.c[0][0], ecs_entity(Position));
    test_int(ctx.s[0][0], 0);

    Position *p = ecs_get_ptr(world, e, Position);
    test_int(p->x, 10);
    test_int(p->y, 20);

    ecs_fini(world);
}

void SystemOnNew_new_match_1_of_2() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_TYPE(world, Type, Position, Velocity);
    ECS_SYSTEM(world, Init, EcsOnNew, Position);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new(world, Type);
    test_assert(e != 0);

    test_int(ctx.count, 1);
    test_int(ctx.invoked, 1);
    test_int(ctx.system, Init);
    test_int(ctx.column_count, 1);
    test_null(ctx.param);

    test_int(ctx.e[0], e);
    test_int(ctx.c[0][0], ecs_entity(Position));
    test_int(ctx.s[0][0], 0);

    Position *p = ecs_get_ptr(world, e, Position);
    test_int(p->x, 10);
    test_int(p->y, 20);

    ecs_fini(world);
}

void SystemOnNew_new_no_match_1() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_SYSTEM(world, Init, EcsOnNew, Position);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new(world, Velocity);
    test_assert(e != 0);

    test_int(ctx.count, 0);

    ecs_fini(world);
}

void SystemOnNew_new_no_match_2_of_1() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_SYSTEM(world, Init, EcsOnNew, Position, Velocity);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new(world, Position);
    test_assert(e != 0);

    test_int(ctx.count, 0);

    ecs_fini(world);
}

void SystemOnNew_new_no_match_2_of_3() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_COMPONENT(world, Mass);
    ECS_COMPONENT(world, Rotation);
    ECS_TYPE(world, Type, Position, Velocity, Mass);
    ECS_SYSTEM(world, Init, EcsOnNew, Position, Rotation);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new(world, Type);
    test_assert(e != 0);

    test_int(ctx.count, 0);

    ecs_fini(world);
}

void SystemOnNew_clone_match_1_of_2() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_TYPE(world, Type, Position, Velocity);
    ECS_SYSTEM(world, Init, EcsOnNew, Position);

    ecs_entity_t e_1 = ecs_new(world, Type);
    test_assert(e_1 != 0);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e_2 = ecs_clone(world, e_1, false);

    test_int(ctx.count, 1);
    test_int(ctx.invoked, 1);
    test_int(ctx.system, Init);
    test_int(ctx.column_count, 1);
    test_null(ctx.param);

    test_int(ctx.e[0], e_2);
    test_int(ctx.c[0][0], ecs_entity(Position));
    test_int(ctx.s[0][0], 0);

    Position *p = ecs_get_ptr(world, e_2, Position);
    test_int(p->x, 10);
    test_int(p->y, 20);

    ecs_fini(world);
}

void SystemOnNew_clone_match_2_of_2() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_TYPE(world, Type, Position, Velocity);
    ECS_SYSTEM(world, Init, EcsOnNew, Position, Velocity);

    ecs_entity_t e_1 = ecs_new(world, Type);
    test_assert(e_1 != 0);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e_2 = ecs_clone(world, e_1, false);

    test_int(ctx.count, 1);
    test_int(ctx.invoked, 1);
    test_int(ctx.system, Init);
    test_int(ctx.column_count, 2);
    test_null(ctx.param);

    test_int(ctx.e[0], e_2);
    test_int(ctx.c[0][0], ecs_entity(Position));
    test_int(ctx.s[0][0], 0);
    test_int(ctx.c[0][1], ecs_entity(Velocity));
    test_int(ctx.s[0][1], 0);

    Position *p = ecs_get_ptr(world, e_2, Position);
    test_int(p->x, 10);
    test_int(p->y, 20);

    Velocity *v = ecs_get_ptr(world, e_2, Velocity);
    test_int(v->x, 30);
    test_int(v->y, 40);

    ecs_fini(world);
}

void SystemOnNew_clone_match_2_of_3() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_COMPONENT(world, Velocity);
    ECS_COMPONENT(world, Mass);
    ECS_TYPE(world, Type, Position, Velocity, Mass);
    ECS_SYSTEM(world, Init, EcsOnNew, Position, Velocity);

    ecs_entity_t e_1 = ecs_new(world, Type);
    test_assert(e_1 != 0);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e_2 = ecs_clone(world, e_1, false);

    test_int(ctx.count, 1);
    test_int(ctx.invoked, 1);
    test_int(ctx.system, Init);
    test_int(ctx.column_count, 2);
    test_null(ctx.param);

    test_int(ctx.e[0], e_2);
    test_int(ctx.c[0][0], ecs_entity(Position));
    test_int(ctx.s[0][0], 0);
    test_int(ctx.c[0][1], ecs_entity(Velocity));
    test_int(ctx.s[0][1], 0);

    Position *p = ecs_get_ptr(world, e_2, Position);
    test_int(p->x, 10);
    test_int(p->y, 20);

    Velocity *v = ecs_get_ptr(world, e_2, Velocity);
    test_int(v->x, 30);
    test_int(v->y, 40);

    ecs_fini(world);
}

void SystemOnNew_new_w_count_match_1_of_1() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_SYSTEM(world, Init, EcsOnNew, Position);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new_w_count(world, Position, 3);
    test_assert(e != 0);

    test_int(ctx.count, 3);
    test_int(ctx.invoked, 1);
    test_int(ctx.system, Init);
    test_int(ctx.column_count, 1);
    test_null(ctx.param);

    test_int(ctx.e[0], e);
    test_int(ctx.e[1], e + 1);
    test_int(ctx.e[2], e + 2);
    test_int(ctx.c[0][0], ecs_entity(Position));
    test_int(ctx.s[0][0], 0);

    Position *p = ecs_get_ptr(world, e, Position);
    test_int(p->x, 10);
    test_int(p->y, 20);

    ecs_fini(world);
}

static int is_invoked;

static
void IsInvoked(ecs_rows_t *rows) {
    is_invoked ++;
}

void SystemOnNew_disabled_system() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_SYSTEM(world, IsInvoked, EcsOnNew, Position);

    ecs_enable(world, IsInvoked, false);

    SysTestData ctx = {0};
    ecs_set_context(world, &ctx);

    ecs_entity_t e = ecs_new(world, Position);
    test_assert(e != 0);
    test_assert( ecs_has(world, e, Position));

    test_int(is_invoked, 0);

    ecs_fini(world);
}

static
void SystemA(ecs_rows_t *rows) {
    int i, tag;
    for (i = 0; i < rows->count; i ++) {
        for (tag = 1000; tag < 1100; tag ++) {
            _ecs_add(
                rows->world, 
                rows->entities[i], 
                ecs_type_from_entity(rows->world, tag));
        }
    }
}

static
void SystemB(ecs_rows_t *rows) {
    ECS_COLUMN(rows, Position, p, 1);

    int i;
    for (i = 0; i < rows->count; i ++) {
        ecs_has(rows->world, rows->entities[i], Position);
    }
}

void SystemOnNew_2_systems_w_table_creation() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_SYSTEM(world, SystemA, EcsOnNew, Position);
    ECS_SYSTEM(world, SystemB, EcsOnNew, Position);

    ecs_entity_t e = ecs_new(world, Position);
    test_assert(e != 0);    

    ecs_fini(world);
}

static
void NewWithPosition(ecs_rows_t *rows) {
    ECS_COLUMN(rows, Position, p, 1);

    ecs_entity_t e = ecs_new(rows->world, Position);
    test_assert(e != 0); 
}

void SystemOnNew_2_systems_w_table_creation_in_progress() {
    ecs_world_t *world = ecs_init();

    ECS_COMPONENT(world, Position);
    ECS_SYSTEM(world, SystemA, EcsOnNew, Position);
    ECS_SYSTEM(world, SystemB, EcsOnNew, Position);
    ECS_SYSTEM(world, NewWithPosition, EcsOnUpdate, .Position);

    ecs_progress(world, 1);   

    ecs_fini(world);
}

static
void TestContext(ecs_rows_t *rows) {
    void *world_ctx = ecs_get_context(rows->world);
    test_assert(world_ctx == rows->param);
    uint32_t *param = rows->param;
    (*param) ++;
}


void SystemOnNew_sys_context() {
    ecs_world_t *world = ecs_init();
    uint32_t param = 0;

    ECS_COMPONENT(world, Position);

    ECS_SYSTEM(world, TestContext, EcsOnNew, Position);

    ecs_set_system_context(world, TestContext, &param);

    test_assert(ecs_get_system_context(world, TestContext) == &param);

    ecs_fini(world);
}

void SystemOnNew_get_sys_context_from_param() {
    ecs_world_t *world = ecs_init();
    uint32_t param = 0;

    ECS_COMPONENT(world, Position);

    ECS_SYSTEM(world, TestContext, EcsOnNew, Position);

    /* Set world context so system can compare if pointer is correct */
    ecs_set_context(world, &param);
    ecs_set_system_context(world, TestContext, &param);

    /* Trigger system */
    ecs_new(world, Position);

    test_int(param, 1);

    ecs_fini(world);
}

void SystemOnNew_clone_match_1_of_1() {
    // Implement testcase
}
