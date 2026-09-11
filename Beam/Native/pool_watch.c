/* Copyright (c) 2026 Lean FRO LLC. Released under Apache 2.0 license. */
#include <lean/lean.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static lean_obj_res beam_watch_error(int error) {
    lean_object *name = lean_mk_string("inotify");
    lean_object *result = lean_decode_io_error(error, name);
    lean_dec(name);
    return lean_io_result_mk_error(result);
}

#if defined(__linux__)
#include <pthread.h>
#include <sys/inotify.h>
#include <unistd.h>

typedef struct { int wd; char *relative; } beam_watch_dir;
typedef struct {
    int fd;
    size_t count;
    beam_watch_dir *dirs;
} beam_watch;

static void beam_watch_finalize(void *data) {
    beam_watch *watch = data;
    close(watch->fd);
    for (size_t i = 0; i < watch->count; i++) free(watch->dirs[i].relative);
    free(watch->dirs);
    free(watch);
}

static void beam_watch_foreach(void *data, b_lean_obj_arg visit) { (void)data; (void)visit; }
static lean_external_class *beam_watch_class;
static pthread_once_t beam_watch_once = PTHREAD_ONCE_INIT;
static void beam_watch_register(void) {
    beam_watch_class = lean_register_external_class(beam_watch_finalize, beam_watch_foreach);
}
#endif

LEAN_EXPORT lean_obj_res lean_beam_pool_watch_new(lean_obj_arg world) {
    (void)world;
#if defined(__linux__)
    int fd = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
    if (fd < 0) return beam_watch_error(errno);
    beam_watch *watch = calloc(1, sizeof(*watch));
    if (!watch) { close(fd); return beam_watch_error(ENOMEM); }
    watch->fd = fd;
    pthread_once(&beam_watch_once, beam_watch_register);
    return lean_io_result_mk_ok(lean_alloc_external(beam_watch_class, watch));
#else
    return lean_io_result_mk_error(lean_mk_io_user_error(lean_mk_string("Beam pools require Linux file watching")));
#endif
}

LEAN_EXPORT lean_obj_res lean_beam_pool_watch_add(b_lean_obj_arg object, b_lean_obj_arg path,
                                                  b_lean_obj_arg relative, lean_obj_arg world) {
    (void)world;
#if defined(__linux__)
    beam_watch *watch = lean_get_external_data(object);
    int wd = inotify_add_watch(watch->fd, lean_string_cstr(path), IN_MODIFY | IN_ATTRIB |
        IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE_SELF | IN_MOVE_SELF);
    if (wd < 0) return lean_io_result_mk_error(lean_decode_io_error(errno, path));
    beam_watch_dir *dirs = realloc(watch->dirs, (watch->count + 1) * sizeof(*dirs));
    if (!dirs) return lean_io_result_mk_error(lean_decode_io_error(ENOMEM, path));
    watch->dirs = dirs;
    char *name = strdup(lean_string_cstr(relative));
    if (!name) return lean_io_result_mk_error(lean_decode_io_error(ENOMEM, path));
    watch->dirs[watch->count++] = (beam_watch_dir){wd, name};
#else
    (void)object; (void)path; (void)relative;
#endif
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_beam_pool_watch_read(b_lean_obj_arg object, lean_obj_arg world) {
    (void)world;
    lean_object *result = lean_alloc_array(0, 0);
#if defined(__linux__)
    beam_watch *watch = lean_get_external_data(object);
    union { struct inotify_event alignment; char bytes[32768]; } buffer;
    for (;;) {
        ssize_t count = read(watch->fd, buffer.bytes, sizeof(buffer.bytes));
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            lean_dec(result);
            return beam_watch_error(count < 0 ? errno : EIO);
        }
        for (size_t offset = 0; offset < (size_t)count;) {
            struct inotify_event *event = (void *)(buffer.bytes + offset);
            offset += sizeof(*event) + event->len;
            if (event->mask & (IN_Q_OVERFLOW | IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED)) {
                result = lean_array_push(result, lean_mk_string("*"));
                continue;
            }
            for (size_t i = 0; i < watch->count; i++) if (watch->dirs[i].wd == event->wd) {
                const char *parent = watch->dirs[i].relative;
                size_t size = strlen(parent) + strlen(event->name) + 3;
                char *path = malloc(size);
                if (!path) { lean_dec(result); return beam_watch_error(ENOMEM); }
                snprintf(path, size, "%s%s%s%s", parent, *parent ? "/" : "", event->name,
                         event->mask & IN_ISDIR ? "/" : "");
                result = lean_array_push(result, lean_mk_string(path));
                free(path);
                break;
            }
        }
        /* An overflow forces a complete rescan; bound event memory even during a build. */
        if (lean_array_size(result) > 8192) {
            lean_dec(result);
            result = lean_alloc_array(0, 1);
            result = lean_array_push(result, lean_mk_string("*"));
            break;
        }
    }
#else
    (void)object;
#endif
    return lean_io_result_mk_ok(result);
}
