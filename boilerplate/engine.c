/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 2048
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int stop_requested;

    void *stack_ptr;

    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;

    int container_out_fd;
    int container_in_fd;

    int stream_lock_initialized;
    pthread_mutex_t stream_lock;
    int should_stream;
    int streamfds[2];
    pthread_cond_t state_cv;

    int log_file_fd;
    char log_path[PATH_MAX];

    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    int active_producers;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char is_control;
    union {
        int payload_len;
        int control_signal;
        int exit_signal;
    } info;
} streaming_chunk_t;

typedef struct {
    char rootfs_base[PATH_MAX];
    int server_fd;
    int monitor_fd;
    int shutdown_pipe_fds[2]; // used as a shutdown signal
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *container;
    int log_fd;
} log_capture_thread_args_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} client_handler_thread_args_t;

typedef struct {
    const char *rootfs;
    const char *command;
    const char *hostname;
    int cout_fd[2];
    int cin_fd[2];
    int nice_value;
} child_args_t;

supervisor_ctx_t *supervisor_ctx = NULL;


static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->count == LOG_BUFFER_CAPACITY && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && (buffer->active_producers > 0 || !buffer->shutting_down)) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->active_producers == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t*) arg;

    log_item_t log_item;
    while (1) {
        int rc = bounded_buffer_pop(&ctx->log_buffer, &log_item);
        if (rc != 0) return NULL;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *container = ctx->containers;
        while (container != NULL && strncmp(log_item.container_id, container->id, sizeof(container->id)) != 0) {
            container = container->next;
        }

        if (container == NULL) {
            fprintf(stderr, "Could not find container with id '%s'\n", log_item.container_id);
            pthread_mutex_unlock(&ctx->metadata_lock);
            continue;
        }

        int bytes_written = write(container->log_file_fd, log_item.data, log_item.length);
        if (bytes_written < 0) {
            perror("write");
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

int setup_root(const char *rootfs)
{
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0)
        return -1;

    if (mount(rootfs, rootfs, NULL, MS_BIND | MS_REC, NULL) != 0)
        return -1;

    char old_root[PATH_MAX];
    snprintf(old_root, sizeof(old_root), "%s/.old_root", rootfs);
    mkdir(old_root, 0755);

    if (syscall(SYS_pivot_root, rootfs, old_root) != 0)
        return -1;

    if (chdir("/") != 0)
        return -1;

    if (umount2("/.old_root", MNT_DETACH) != 0)
        return -1;

    rmdir("/.old_root");
    return 0;
}

pid_t child_pid = -1;

void forward_sig(int sig) {
    if (child_pid == -1) return;
    if (kill(child_pid, sig) == -1) {
        perror("kill");
    }
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_args_t *args = (child_args_t*) arg;

    close(args->cout_fd[0]);
    dup2(args->cout_fd[1], STDOUT_FILENO);
    dup2(args->cout_fd[1], STDERR_FILENO);
    close(args->cout_fd[1]);

    close(args->cin_fd[1]);
    dup2(args->cin_fd[0], STDIN_FILENO);
    close(args->cin_fd[0]);

    if (setup_root(args->rootfs) != 0) {
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount");
        return 1;
    }

    if (sethostname(args->hostname, strlen(args->hostname)) == -1) {
        perror("sethostname");
        return 1;
    }

    signal(SIGINT, SIG_IGN);
    signal(SIGTERM, forward_sig);

    if (setpgid(0, 0) != 0) {
        perror("setpgid");
        return 1;
    }

    if (setpriority(PRIO_PROCESS, 0, args->nice_value) != 0) {
        perror("setpriority");
    }

    if (setpriority(PRIO_PGRP, getpgrp(), args->nice_value) != 0) {
        perror("setpriority");
    }

    // Fork because PID 1 ignores SIGTERM
    pid_t pid = fork();
    if (pid == 0) {
        execl("/bin/sh", "sh", "-c", args->command, NULL);
        perror("execl");
        exit(1);
    }
    child_pid = pid;

    int status;
    if (waitpid(pid, &status, 0) == -1) {
        perror("waitpid");
        return 1;
    }
    child_pid = -1;

    if (WIFEXITED(status)) {
        exit(WEXITSTATUS(status));
    } else if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        exit(128 + sig);
    };

    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

void *capture_container_log(void *arg) {
    log_capture_thread_args_t *args = (log_capture_thread_args_t*) arg;
    container_record_t *container = args->container;

    pthread_mutex_lock(&args->ctx->log_buffer.mutex);
    args->ctx->log_buffer.active_producers++;
    pthread_mutex_unlock(&args->ctx->log_buffer.mutex);

    struct pollfd fds[2];
    fds[0].events = POLLIN;
    fds[0].fd = args->ctx->shutdown_pipe_fds[0];
    fds[1].events = POLLIN;
    fds[1].fd = args->log_fd;

    log_item_t log_item;
    strncpy(log_item.container_id, container->id, sizeof(log_item.container_id));

    while (1) {
        int n = poll(fds, 2, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (fds[1].revents & POLLIN) {
            int bytes_read = read(fds[1].fd, log_item.data, sizeof(log_item.data));
            if (bytes_read < 0) {
                perror("read");
                break;
            }

            log_item.length = bytes_read;
            int rc = bounded_buffer_push(&args->ctx->log_buffer, &log_item);
            if (rc != 0) {
                break;
            }

            pthread_mutex_lock(&container->stream_lock);
            if (container->should_stream) {
                int bytes_written = write(container->streamfds[1], log_item.data, bytes_read);
                if (bytes_written == -1) {
                    perror("write");
                }
            }
            pthread_mutex_unlock(&container->stream_lock);
        }

        if (fds[0].revents & POLLIN) {
            char buf[16];
            int n = read(fds[0].fd, buf, sizeof(buf));

            if (n > 0 && strncmp(buf, "stop", 4) == 0) {
                int rc = kill(-(container->host_pid), SIGKILL);
                if (rc == -1) {
                    perror("kill");
                }
                break;
            }
        }

        if (fds[1].revents & POLLHUP) {
            // container exited, supervisor will reap via SIGCHLD
            pthread_mutex_lock(&container->stream_lock);
            if (container->should_stream) {
                close(container->streamfds[1]);
            }
            pthread_mutex_unlock(&container->stream_lock);
            break;
        }
    }

    pthread_mutex_lock(&args->ctx->log_buffer.mutex);
    args->ctx->log_buffer.active_producers--;
    pthread_cond_broadcast(&args->ctx->log_buffer.not_empty); // so any waiting consumers exit
    pthread_mutex_unlock(&args->ctx->log_buffer.mutex);

    close(container->log_file_fd);
    free(args);
    return NULL;
}

int start_container(supervisor_ctx_t *ctx, control_request_t *request, container_record_t **container_out) {
    __label__ cleanup_pipe, cleanup_ctx_containers, cleanup_logger;

    container_record_t *container = malloc(sizeof(container_record_t));
    strncpy(container->id, request->container_id, CONTAINER_ID_LEN);
    strncpy(container->rootfs, request->rootfs, PATH_MAX);
    strncpy(container->command, request->command, CHILD_COMMAND_LEN);
    container->nice_value = request->nice_value;
    container->soft_limit_bytes = request->soft_limit_bytes;
    container->hard_limit_bytes = request->hard_limit_bytes;
    container->state = CONTAINER_STARTING;
    container->stop_requested = 0;

    int cout_fds[2] = {-1};
    int rc = pipe(cout_fds);
    if (rc != 0) {
        perror("rc");
        free(container);
        return -1;
    }
    container->container_out_fd = cout_fds[0];

    struct stat st = {0};
    // Check if container->rootfs exists and is a directory
    if (stat(container->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        int cmd_size = 2 * PATH_MAX + 32;
        char *cmd = malloc(cmd_size);
        snprintf(cmd, cmd_size, "cp -a \"%s\" \"%s\"", ctx->rootfs_base, container->rootfs);

        int ret = system(cmd);
        free(cmd);
        if (ret != 0) {
            fprintf(stderr, "Failed to copy rootfs from %s to %s\n", ctx->rootfs_base, container->rootfs);
            free(container);
            return -1;
        }
    }

    int cin_fds[2] = {-1};
    rc = pipe(cin_fds);
    if (rc != 0) {
        perror("rc");
        free(container);
        return -1;
    }
    container->container_in_fd = cin_fds[1];

    container->should_stream = 0;
    rc = pthread_mutex_init(&container->stream_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        free(container);
        return -1;
    }
    container->stream_lock_initialized = 1;
    pthread_cond_init(&container->state_cv, NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *tmp = ctx->containers;
    container_record_t *prev = NULL;
    while (tmp != NULL) {
        if (tmp->state == CONTAINER_RUNNING && strncmp(tmp->rootfs, request->rootfs, sizeof(tmp->rootfs)) == 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            rc = 2;
            goto cleanup_pipe;
        }

        if (strncmp(tmp->id, container->id, sizeof(container->id)) == 0) {
            if (tmp->state == CONTAINER_RUNNING || tmp->state == CONTAINER_STARTING) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                rc = 1;
                goto cleanup_pipe;
            } else {
                if (tmp->stream_lock_initialized) {
                    pthread_mutex_unlock(&ctx->metadata_lock);
                    rc = 1;
                    goto cleanup_pipe;
                }
                pthread_cond_destroy(&tmp->state_cv);
                if (prev != NULL) {
                    prev->next = tmp->next;
                } else {
                    ctx->containers = tmp->next;
                }
                free(tmp);
                break;
            }
        }
        prev = tmp;
        tmp = tmp->next;
    }
    container->next = ctx->containers;
    ctx->containers = container;
    pthread_mutex_unlock(&ctx->metadata_lock);

    // Create log directory if required
    rc = stat(LOG_DIR, &st);
    if (rc == -1) {
        rc = mkdir(LOG_DIR, 0755);
        if (rc == -1) {
            perror("mkdir");
            rc = -1;
            goto cleanup_ctx_containers;
        }
    }

    snprintf(container->log_path, sizeof(container->log_path), "%s/%s.log", LOG_DIR, container->id);
    container->log_file_fd = open(container->log_path, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (container->log_file_fd < 0) {
        perror("open");
        rc = -1;
        goto cleanup_ctx_containers;
    }

    log_capture_thread_args_t *logger_args = malloc(sizeof(log_capture_thread_args_t));
    logger_args->ctx = ctx;
    logger_args->container = container;
    logger_args->log_fd = cout_fds[0];

    pthread_t container_log_thread;
    rc = pthread_create(&container_log_thread, NULL, capture_container_log, logger_args);
    if (rc != 0) {
        fprintf(stderr, "Failed to create log capture thread for container %s\n", container->id);
        rc = -1;
        goto cleanup_logger;
    }
    pthread_detach(container_log_thread);

    child_args_t *child_args = malloc(sizeof(child_args_t));
    child_args->rootfs = request->rootfs;
    child_args->command = request->command;
    child_args->hostname = container->id;
    child_args->cout_fd[0] = cout_fds[0];
    child_args->cout_fd[1] = cout_fds[1];
    child_args->cin_fd[0] = cin_fds[0];
    child_args->cin_fd[1] = cin_fds[1];
    child_args->nice_value = container->nice_value;

    char *child_stack = malloc(STACK_SIZE);
    if (!child_stack) {
        perror("malloc");
        free(child_args);
        rc = -1;
        goto cleanup_logger;
    }

    pid_t pid = clone(
        child_fn,
        child_stack + STACK_SIZE,
        CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
        child_args
    );

    if (pid > 0) {
        close(cout_fds[1]);
        close(cin_fds[0]);

        pthread_mutex_lock(&ctx->metadata_lock);
        container->host_pid = pid;
        container->started_at = time(NULL);
        container->stack_ptr = child_stack;
        container->state = CONTAINER_RUNNING;
        pthread_mutex_unlock(&ctx->metadata_lock);

        printf("Started container %s with PID %d\n", container->id, pid);
        if (container_out != NULL) {
            *container_out = container;
        }

        rc = register_with_monitor(ctx->monitor_fd, container->id, container->host_pid, container->soft_limit_bytes, container->hard_limit_bytes);
        if (rc != 0) {
            fprintf(stderr, "Failed to register container '%s' with monitor\n", container->id);
        }

        return 0;
    }

    perror("clone");
    rc = -1;

    free(child_stack);
    free(child_args);

cleanup_logger:
    free(logger_args);
    close(container->log_file_fd);

cleanup_ctx_containers:
    // remove container
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *container_tmp = ctx->containers;
    if (container_tmp == container) {
        ctx->containers = NULL;
    } else {
        while (container_tmp->next != NULL && container_tmp->next != container) {
            container_tmp = container_tmp->next;
        }
        if (container_tmp->next == container) {
            container_tmp->next = container->next;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

cleanup_pipe:
    pthread_mutex_destroy(&container->stream_lock);
    close(cout_fds[0]);
    close(cout_fds[1]);
    close(cin_fds[0]);
    close(cin_fds[1]);
    free(container);
    return rc;
}

void get_containers_status(supervisor_ctx_t *ctx, char *buf, int n) {
    int len = 0;
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *container = ctx->containers;
    if (container == NULL) {
        snprintf(buf, n, "No containers yet.\n");
    } else {
        while (container != NULL && len < n) {
            container_state_t state = container->state;
            const char *state_str = state_to_string(state);
            len += snprintf(buf + len, n - len, "%s (%s)", container->id, state_str);
            if (state != CONTAINER_STARTING) {
                char time_str[32];
                struct tm t;
                localtime_r(&container->started_at, &t);
                strftime(time_str, sizeof(time_str), "%d-%m-%Y %H:%M:%S", &t);
                len += snprintf(buf + len, n - len, " - Start Time: %s", time_str);
            }
            if (state == CONTAINER_RUNNING) {
                len += snprintf(buf + len, n - len, ", PID: %d, Nice: %d, Soft Limit: %lu, Hard Limit: %lu", container->host_pid, container->nice_value, container->soft_limit_bytes, container->hard_limit_bytes);
            } else if (state == CONTAINER_EXITED) {
                len += snprintf(buf + len, n - len, ", Exit Code: %d", container->exit_code);
            } else if (state == CONTAINER_STOPPED) {
                len += snprintf(buf + len, n - len, ", Stop signal: %d", container->exit_code);
            }
            len += snprintf(buf + len, n - len, "\n");
            container = container->next;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

int get_container_logs(supervisor_ctx_t *ctx, const control_request_t *request, char *buf, int n) {
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *container = ctx->containers;
    while (container != NULL) {
        if (strncmp(request->container_id, container->id, sizeof(container->id)) == 0) {
            break;
        }
        container = container->next;
    }

    if (container == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    FILE *fp = fopen(container->log_path, "rb");
    if (!fp) return -1;

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return -1;
    }

    long file_size = ftell(fp);
    if (file_size < 0) {
        fclose(fp);
        return -1;
    }

    long start = (file_size > (long)n) ? file_size - (long)n : 0;

    if (fseek(fp, start, SEEK_SET) != 0) {
        fclose(fp);
        return -1;
    }

    size_t to_read = file_size - start;
    size_t read_bytes = fread(buf, 1, to_read, fp);
    fclose(fp);
    if (read_bytes != to_read) {
        return -1;
    }

    return 0;
}

int stop_container(pthread_mutex_t *lock, container_record_t *container, int signal) {
    pthread_mutex_lock(lock);
    if (container->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(lock);
        return 2;
    }
    pthread_mutex_unlock(lock);

    if (signal == SIGTERM) {
        container->stop_requested = 1;
    }
    int rc = kill(-(container->host_pid), signal);
    if (rc == -1) {
        perror("kill");
    }

    return rc;
}

int stop_container_by_id(supervisor_ctx_t *ctx, const char *container_id, int signal) {
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *container = ctx->containers;
    if (container == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return 1;
    } else {
        while (container != NULL) {
            if (strncmp(container_id, container->id, sizeof(container->id)) == 0) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                return stop_container(&ctx->metadata_lock, container, signal);
            }
            container = container->next;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    return 1;
}

int stream_container_stdio(pthread_mutex_t *lock, container_record_t *container, int client_fd) {
    if (!container->stream_lock_initialized) return -1;


    pthread_mutex_lock(&container->stream_lock);
    if (container->should_stream) {
        pthread_mutex_unlock(&container->stream_lock);
        return -1;
    }

    int rc = pipe(container->streamfds);
    if (rc == -1) {
        pthread_mutex_unlock(&container->stream_lock);
        perror("pipe");
        return -1;
    }

    container->should_stream = 1;
    pthread_mutex_unlock(&container->stream_lock);


    struct pollfd pollfds[2];
    pollfds[0].events = POLLIN;
    pollfds[0].fd = container->streamfds[0];

    pollfds[1].events = POLLIN;
    pollfds[1].fd = client_fd;

    streaming_chunk_t chunk;
    char buf[128];
    while (1) {
        rc = poll(pollfds, 2, -1);
        if (rc < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        int container_hup = pollfds[0].revents & POLLHUP;
        int client_hup = pollfds[1].revents & POLLHUP;
        if (container_hup || client_hup) {
            pthread_mutex_lock(&container->stream_lock);
            container->should_stream = 0;
            if (!container_hup) {
                close(container->streamfds[0]);
            }
            close(container->streamfds[1]);
            pthread_mutex_unlock(&container->stream_lock);
            pthread_mutex_destroy(&container->stream_lock);
            container->stream_lock_initialized = 0;

            if (container_hup && !client_hup) {
                pthread_mutex_lock(lock);
                while (container->state == CONTAINER_RUNNING) {
                    pthread_cond_wait(&container->state_cv, lock);
                }

                chunk.is_control = 1;
                chunk.info.exit_signal = container->exit_code;
                if (container->state != CONTAINER_EXITED) chunk.info.exit_signal += 128;
                pthread_mutex_unlock(lock);

                rc = write(client_fd, &chunk, sizeof(chunk));
                if (rc == -1) {
                    perror("write");
                }
            }

            return 0;
        }

        if (pollfds[0].revents & POLLIN) {
            rc = read(pollfds[0].fd, buf, sizeof(buf));
            if (rc == -1) {
                perror("read");
                break;
            }

            chunk.is_control = 0;
            chunk.info.payload_len = rc;
            rc = write(client_fd, &chunk, sizeof(chunk));
            if (rc == -1) {
                perror("write");
                break;
            }

            rc = write(client_fd, buf, chunk.info.payload_len);
            if (rc == -1) {
                perror("write");
                break;
            }
        }

        if (pollfds[1].revents & POLLIN) {
            rc = read(pollfds[1].fd, (void*) &chunk, sizeof(chunk));
            if (rc == -1) {
                perror("read");
                break;
            }

            if (chunk.is_control) {
                printf("Sending signal %d to container '%s'\n", chunk.info.control_signal, container->id);
                rc = stop_container(lock, container, chunk.info.control_signal);
                if (rc != 0) {
                    fprintf(stderr, "Could not send signal\n");
                }
                continue;
            }

            while (chunk.info.payload_len) {
                int recv_len = sizeof(buf);
                if (chunk.info.payload_len < recv_len) {
                    recv_len = chunk.info.payload_len;
                }

                rc = read(pollfds[1].fd, &buf, recv_len);
                if (rc == -1) {
                    perror("read");
                    break;
                }
                if (rc != recv_len) {
                    rc = -1;
                    break;
                }

                rc = write(container->container_in_fd, buf, recv_len);
                if (rc == -1) {
                    perror("write");
                    break;
                }
                chunk.info.payload_len -= recv_len;
            }

            if (rc == -1) break;
        }
    }

    return rc;
}

void *handle_client(void *arg) {
    client_handler_thread_args_t *args = (client_handler_thread_args_t*) arg;
    int client_fd = args->client_fd;

    control_request_t request;
    int rc = read(client_fd, (void*) &request, sizeof(request));
    if (rc == -1) {
        perror("read");
        close(client_fd);
        free(args);
        return NULL;
    }

    int send_response = 0;
    control_response_t response;
    memset(&response, 0, sizeof(response));

    container_record_t *container;

    switch (request.kind) {
        case CMD_SUPERVISOR:
            fprintf(stderr, "Received request of type 'CMD_SUPERVISOR'\n");
            break;
        case CMD_START:
            rc = start_container(args->ctx, &request, NULL);
            send_response = 1;
            if (rc == 0) {
                response.status = 201;
                snprintf(response.message, sizeof(response.message), "Created container with id '%s'", request.container_id);
            } else if (rc == 1) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Container with id '%s' already exists!", request.container_id);
            } else if (rc == 2) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Rootfs directory is in use by another container!");
            } else {
                response.status = 500;
                snprintf(response.message, sizeof(response.message), "An internal error occurred.");
            }
            break;
        case CMD_RUN:
            rc = start_container(args->ctx, &request, &container);
            send_response = 1;
            if (rc == 1) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Container with id '%s' already exists!", request.container_id);
                break;
            } else if (rc == 2) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Rootfs directory is in use by another container!");
                break;
            } else if (rc != 0) {
                response.status = 500;
                snprintf(response.message, sizeof(response.message), "An internal error occurred.");
                break;
            }

            send_response = 0;
            response.status = 201;
            snprintf(response.message, sizeof(response.message), "Created container with id '%s'", request.container_id);

            rc = write(client_fd, (const void*) &response, sizeof(response));
            if (rc == -1) {
                perror("write");
                break;
            }

            stream_container_stdio(&args->ctx->metadata_lock, container, args->client_fd);
            break;
        case CMD_PS:
            send_response = 1;
            response.status = 200;
            get_containers_status(args->ctx, response.message, sizeof(response.message));
            break;
        case CMD_LOGS:
            rc = get_container_logs(args->ctx, &request, response.message, sizeof(response.message));
            send_response = 1;
            if (rc == 0) {
                response.status = 200;
            } else if (rc == 1) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Container '%s' does not exist!", request.container_id);
            } else {
                response.status = 500;
                snprintf(response.message, sizeof(response.message), "An internal error occurred.");
            }
            break;
        case CMD_STOP:
            rc = stop_container_by_id(args->ctx, request.container_id, SIGTERM);
            send_response = 1;
            if (rc == 0) {
                response.status = 200;
                snprintf(response.message, sizeof(response.message), "Sent terminate signal to container '%s'", request.container_id);
            } else if (rc == 1) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Container '%s' does not exist!", request.container_id);
            } else if (rc == 2) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Could not send terminate signal. The container is not running.");
            } else {
                response.status = 500;
                snprintf(response.message, sizeof(response.message), "An internal error occurred.");
            }
            break;
        default:
            fprintf(stderr, "Received unknown request type: %d\n", request.kind);
            break;
    }

    if (send_response) {
        rc = write(client_fd, (const void*) &response, sizeof(response));
        if (rc == -1) {
            perror("write");
        }
    }

    close(client_fd);
    free(args);
    return NULL;
}

void handle_sigchld(int sig) {
    (void) sig;

    char c = 'c';
    if (supervisor_ctx) {
        if (write(supervisor_ctx->shutdown_pipe_fds[1], &c, 1) == -1) {
            perror("write");
        }
    }
}

void reap_children(supervisor_ctx_t *ctx)
{
    while (1) {
        int status;
        pid_t pid = waitpid(-1, &status, WNOHANG);

        if (pid <= 0) break;

        pthread_mutex_lock(&ctx->metadata_lock);

        container_record_t *container = ctx->containers;
        while (container) {
            if (container->host_pid == pid) {
                int exit_code = -1;

                if (WIFEXITED(status)) {
                    exit_code = WEXITSTATUS(status);
                    if (exit_code < 128) {
                        container->exit_code = exit_code;
                        container->state = CONTAINER_EXITED;
                        printf("Container '%s' exited with code %d\n", container->id, container->exit_code);
                    }
                }

                if (WIFSIGNALED(status) || exit_code > 128) {
                    container->exit_code = exit_code > 128 ? exit_code - 128 : WTERMSIG(status);

                    if (container->stop_requested && container->exit_code != SIGKILL) {
                        container->state = CONTAINER_STOPPED;
                        printf("Container '%s' was stopped by signal %d\n", container->id, container->exit_code);
                    } else {
                        container->state = CONTAINER_KILLED;
                        printf("Container '%s' was killed by signal %d\n", container->id, container->exit_code);
                    }
                } else if (exit_code == -1) {
                    fprintf(stderr, "Container '%s' is in unknown state: %d\n", container->id, status);
                    container->exit_code = -1;
                    container->state = CONTAINER_STOPPED;
                }

                pthread_cond_broadcast(&container->state_cv);

                unregister_from_monitor(ctx->monitor_fd, container->id, container->host_pid);

                free(container->stack_ptr);
                container->stack_ptr = NULL;

                int should_destroy = 0;
                pthread_mutex_lock(&container->stream_lock);
                if (container->should_stream) {
                    close(container->streamfds[1]);
                } else {
                    should_destroy = 1;
                }
                pthread_mutex_unlock(&container->stream_lock);

                if (should_destroy) {
                    pthread_mutex_destroy(&container->stream_lock);
                    container->stream_lock_initialized = 0;
                }

                break;
            }
            container = container->next;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

void stop_supervisor() {
    if (supervisor_ctx == NULL) return;
    int rc = write(supervisor_ctx->shutdown_pipe_fds[1], "stop", 5);
    if (rc == -1) {
        perror("write");
    }
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    __label__ cleanup_bounded_buffer, cleanup_monitor, cleanup_socket;

    if (supervisor_ctx != NULL) {
        fprintf(stderr, "run_supervisor has been called more than once\n");
        return 1;
    }

    supervisor_ctx_t ctx;
    int rc;

    supervisor_ctx = &ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    strncpy(ctx.rootfs_base, rootfs, sizeof(ctx.rootfs_base));
    ctx.rootfs_base[sizeof(ctx.rootfs_base) - 1] = '\0';

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /*
     * TODO:
     *   1) open /dev/container_monitor
     *   2) create the control socket / FIFO / shared-memory channel
     *   3) install SIGCHLD / SIGINT / SIGTERM handling
     *   4) spawn the logger thread
     *   5) enter the supervisor event loop
     */

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd == -1) {
        perror("open");
        goto cleanup_bounded_buffer;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        fprintf(stderr, "Failed to create logger thread. Error code: %d\n", rc);
        goto cleanup_monitor;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd == -1) {
        perror("socket");
        goto cleanup_monitor;
    }

    struct sockaddr_un addr;
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path));

    rc = bind(ctx.server_fd, (struct sockaddr*) &addr, sizeof(addr));
    if (rc == -1) {
        perror("bind");
        goto cleanup_socket;
    }

    rc = listen(ctx.server_fd, 10);
    if (rc == -1) {
        perror("listen");
        goto cleanup_socket;
    }
    printf("Listening for requests\n");

    rc = pipe(ctx.shutdown_pipe_fds);
    if (rc != 0) {
        perror("pipe");
        goto cleanup_socket;
    }

    signal(SIGINT, stop_supervisor);
    signal(SIGTERM, stop_supervisor);
    signal(SIGCHLD, handle_sigchld);

    struct pollfd pollfds[2];
    pollfds[0].fd = ctx.shutdown_pipe_fds[0];
    pollfds[0].events = POLLIN;

    pollfds[1].fd = ctx.server_fd;
    pollfds[1].events = POLLIN;

    while (1) {
        int n = poll(pollfds, 2, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (pollfds[0].revents & POLLIN) {
            // Check if it was shutdown signal or SIGCHLD
            char buf[32];
            if (read(ctx.shutdown_pipe_fds[0], buf, sizeof(buf)) == -1) {
                perror("read");
                break;
            }

            if (buf[0] == 'c') {
                reap_children(&ctx);
            } else if (strncmp(buf, "stop", 4) == 0) {
                break;
            }

            continue;
        }

        if (!(pollfds[1].revents & POLLIN)) {
            fprintf(stderr, "Unexpected event on server fd: %d\n", pollfds[1].revents);
            break;
        }

        client_handler_thread_args_t *client_thread_args = malloc(sizeof(client_handler_thread_args_t));
        client_thread_args->ctx = &ctx;
        client_thread_args->client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_thread_args->client_fd == -1) {
            perror("accept");
            free(client_thread_args);
            continue;
        }

        pthread_t handler_thread;
        rc = pthread_create(&handler_thread, NULL, handle_client, (void*) client_thread_args);
        if (rc != 0) {
            fprintf(stderr, "Failed to spawn client handler thread. Error code: %d\n", rc);
            close(client_thread_args->client_fd);
            free(client_thread_args);
            continue;
        }

        pthread_detach(handler_thread);
    }

    printf("\nStopping supervisor\n");

    close(ctx.shutdown_pipe_fds[1]); // sends shutdown signal to logging threads

cleanup_socket:
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

cleanup_monitor:
    close(ctx.monitor_fd);

cleanup_bounded_buffer:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    while (ctx.containers != NULL) {
        container_record_t *next = ctx.containers->next;
        free(ctx.containers);
        ctx.containers = next;
    }

    return rc;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req, int *server_fd_out)
{
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path));

    int rc = connect(server_fd, (struct sockaddr*) &addr, sizeof(addr));
    if (rc == -1) {
        perror("connect");
        close(server_fd);
        return 1;
    }

    rc = write(server_fd, (const void*) req, sizeof(control_request_t));
    if (rc == -1) {
        perror("write");
        close(server_fd);
        return 1;
    }

    control_response_t response;
    rc = read(server_fd, (void*) &response, sizeof(response));
    if (rc == -1) {
        perror("read");
        close(server_fd);
    } else if (rc > 0) {
        response.message[sizeof(response.message) - 1] = '\0';
        printf("Status: %d\n%s\n", response.status, response.message);
    }

    if (server_fd_out == NULL) {
        close(server_fd);
    } else {
        *server_fd_out = server_fd;
    }
    return 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req, NULL);
}

int g_server_fd = -1;

void forward_signal(int sig) {
    if (g_server_fd == -1) return;

    streaming_chunk_t chunk;
    chunk.is_control = 1;
    chunk.info.control_signal = sig;

    int rc = write(g_server_fd, (const void*) &chunk, sizeof(chunk));
    if (rc == -1) {
        perror("write");
    }
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    int server_fd = -1;
    int rc = send_control_request(&req, &server_fd);
    if (rc != 0) {
        return rc;
    }

    g_server_fd = server_fd;
    signal(SIGINT, forward_signal);
    signal(SIGTERM, forward_signal);

    struct pollfd pollfds[2];
    pollfds[0].events = POLLIN;
    pollfds[0].fd = STDIN_FILENO;

    pollfds[1].events = POLLIN;
    pollfds[1].fd = server_fd;

    streaming_chunk_t chunk;
    char buf[128];
    while (1) {
        rc = poll(pollfds, 2, -1);
        if (rc < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (pollfds[0].revents & POLLIN) {
            rc = read(STDIN_FILENO, buf, sizeof(buf));
            if (rc == -1) {
                perror("read");
                break;
            }

            chunk.is_control = 0;
            chunk.info.payload_len = rc;
            rc = write(server_fd, (void*) &chunk, sizeof(chunk));
            if (rc == -1) {
                perror("write");
                break;
            }

            rc = write(server_fd, buf, chunk.info.payload_len);
            if (rc == -1) {
                perror("write");
                break;
            }
        }

        if (pollfds[1].revents & POLLIN) {
            rc = read(server_fd, &chunk, sizeof(chunk));
            if (rc == -1) {
                perror("read");
                break;
            }

            if (chunk.is_control) {
                printf("\nContainer exited with code %d\n", chunk.info.exit_signal);
                break;
            }

            while (chunk.info.payload_len) {
                size_t read_len = ((size_t) chunk.info.payload_len) > sizeof(buf) ? sizeof(buf) : ((size_t) chunk.info.payload_len);
                rc = read(server_fd, buf, read_len);
                if (rc == -1) {
                    perror("read");
                    break;
                }
                rc = write(STDOUT_FILENO, buf, rc);
                if (rc == -1) {
                    perror("write");
                    break;
                }
                chunk.info.payload_len -= read_len;
            }

            fflush(stdout);
            if (rc == -1) break;
        }

        if ((pollfds[0].revents & POLLHUP) || (pollfds[1].revents & POLLHUP)) {
            break;
        }
    }

    g_server_fd = -1;
    close(server_fd);
    return 0;
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req, NULL);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req, NULL);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req, NULL);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
