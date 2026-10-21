/* Copyright (c) 2026. Released under Apache 2.0 license. */
#include <lean/lean.h>
#include <stdatomic.h>
#include <stdlib.h>

typedef struct { lean_object *region; size_t bytes; } region_owner;
typedef struct { lean_object *data; lean_object *owner; } imported_snapshot;
static _Atomic size_t live_bytes;

extern lean_obj_res lean_compacted_region_free(lean_obj_arg region, lean_obj_arg world);

static void region_finalize(void *data) {
    region_owner *owner = data;
    lean_dec(lean_compacted_region_free(owner->region, lean_box(0)));
    atomic_fetch_sub(&live_bytes, owner->bytes);
    free(owner);
}

static void region_foreach(void *data, b_lean_obj_arg visit) {
    lean_inc(visit);
    lean_object *region = ((region_owner *)data)->region;
    lean_inc(region);
    lean_dec(lean_apply_1(visit, region));
}

static lean_external_class *region_class;
static lean_external_class *snapshot_class;
static atomic_flag region_lock = ATOMIC_FLAG_INIT;

/* Lean destroys constructor fields via a LIFO worklist. Putting a region owner
   beside the state in a Lean constructor can therefore unmap it before queued
   state destructors run. This external object establishes the required order. */
static void snapshot_finalize(void *data) {
    imported_snapshot *snapshot = data;
    lean_dec(snapshot->data);
    lean_dec(snapshot->owner);
    free(snapshot);
}

static void snapshot_foreach(void *data, b_lean_obj_arg visit) {
    imported_snapshot *snapshot = data;
    lean_inc(visit); lean_inc(snapshot->data);
    lean_dec(lean_apply_1(visit, snapshot->data));
    lean_inc(visit); lean_inc(snapshot->owner);
    lean_dec(lean_apply_1(visit, snapshot->owner));
}

static lean_obj_res snapshot_new(lean_obj_arg data, lean_obj_arg owner) {
    imported_snapshot *snapshot = malloc(sizeof(*snapshot));
    if (!snapshot) lean_internal_panic_out_of_memory();
    *snapshot = (imported_snapshot){data, owner};
    return lean_alloc_external(snapshot_class, snapshot);
}

LEAN_EXPORT lean_obj_res lean_beam_imported_snapshot_new(lean_obj_arg data, lean_obj_arg region,
                                                        size_t bytes, lean_obj_arg world) {
    (void)world;
    while (atomic_flag_test_and_set_explicit(&region_lock, memory_order_acquire)) {}
    if (!region_class) {
        region_class = lean_register_external_class(region_finalize, region_foreach);
        snapshot_class = lean_register_external_class(snapshot_finalize, snapshot_foreach);
    }
    atomic_flag_clear_explicit(&region_lock, memory_order_release);
    region_owner *owner = malloc(sizeof(*owner));
    if (!owner) lean_internal_panic_out_of_memory();
    *owner = (region_owner){region, bytes};
    atomic_fetch_add(&live_bytes, bytes);
    return lean_io_result_mk_ok(snapshot_new(data, lean_alloc_external(region_class, owner)));
}

LEAN_EXPORT lean_obj_res lean_beam_imported_snapshot_data(b_lean_obj_arg object) {
    lean_object *data = ((imported_snapshot *)lean_get_external_data(object))->data;
    lean_inc(data);
    return data;
}

LEAN_EXPORT lean_obj_res lean_beam_imported_snapshot_derive(b_lean_obj_arg object,
                                                           lean_obj_arg data, lean_obj_arg world) {
    (void)world;
    lean_object *owner = ((imported_snapshot *)lean_get_external_data(object))->owner;
    lean_inc(owner);
    return lean_io_result_mk_ok(snapshot_new(data, owner));
}

LEAN_EXPORT lean_obj_res lean_beam_region_live_bytes(lean_obj_arg world) {
    (void)world;
    return lean_io_result_mk_ok(lean_box_usize(atomic_load(&live_bytes)));
}

LEAN_EXPORT lean_obj_res lean_beam_region_keep_alive(b_lean_obj_arg owner, lean_obj_arg world) {
    (void)owner; (void)world;
    return lean_io_result_mk_ok(lean_box(0));
}
