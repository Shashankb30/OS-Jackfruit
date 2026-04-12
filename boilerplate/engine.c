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
#include <sys/socket.h>
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
#define CONTROL_MESSAGE_LEN 256
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
    int exit_signal;

    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;

    int log_write_fd;
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
    char container_id[CONTAINER_ID_LEN];
    int log_fd;
} log_capture_thread_args_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} client_handler_thread_args_t;

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

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
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
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (container == NULL) {
            fprintf(stderr, "Could not find container with id '%s'\n", container->id);
            continue;
        }

        int bytes_written = write(container->log_file_fd, log_item.data, log_item.length);
        if (bytes_written < 0) {
            perror("write");
        }
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
void child_fn(const char *rootfs, const char *command, int pipe_fd[2])
{
    const char shell[] = "/bin/sh";
    char shell_path[PATH_MAX + sizeof(shell)] = {0};
    strncpy(shell_path, rootfs, PATH_MAX);
    strcat(shell_path, shell);

    close(pipe_fd[0]);
    dup2(pipe_fd[1], STDOUT_FILENO);
    dup2(pipe_fd[1], STDERR_FILENO);
    close(pipe_fd[1]);

    execl(shell_path, "/bin/sh", "-c", command, NULL);
    perror("execl");
    exit(1);
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

    struct pollfd fds[2];
    fds[0].events = POLLIN;
    fds[0].fd = args->ctx->shutdown_pipe_fds[0];
    fds[1].events = POLLIN;
    fds[1].fd = args->log_fd;

    log_item_t log_item;
    strncpy(log_item.container_id, args->container_id, sizeof(log_item.container_id));

    while (1) {
        int n = poll(fds, 2, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (fds[0].revents & (POLLIN | POLLHUP)) break; // shutdown signal

        if (fds[1].revents & POLLHUP) {
            pthread_mutex_lock(&args->ctx->metadata_lock);
            container_record_t *container = args->ctx->containers;
            while (container != NULL && container->log_write_fd != args->log_fd) {
                container = container->next;
            }
            pthread_mutex_unlock(&args->ctx->metadata_lock);

            if (container == NULL) {
                fprintf(stderr, "Failed to find container with log write fd %d\n", args->log_fd);
                break;
            }

            int status;
            pid_t rpid = waitpid(container->host_pid, &status, 0);
            close(container->log_file_fd);
            if (rpid < 0) {
                perror("waitpid");
                break;
            }

            if (WIFEXITED(status)) {
                container->exit_code = WEXITSTATUS(status);
                container->state = CONTAINER_EXITED;
                printf("Container '%s' exited with code %d\n", container->id, container->exit_code);
            } else if (WIFSIGNALED(status)) {
                container->exit_code = WTERMSIG(status);
                container->state = CONTAINER_STOPPED;
                printf("Container '%s' was terminated by signal %d\n", container->id, container->exit_code);
            } else {
                fprintf(stderr, "Container '%s' is in unknown state: %d\n", container->id, status);
                container->exit_code = -1;
                container->state = CONTAINER_STOPPED;
            }

            break;
        }

        if (fds[1].revents & POLLIN) {
            int bytes_read = read(fds[1].fd, &log_item.data[log_item.length], sizeof(log_item.data) - log_item.length);
            if (bytes_read < 0) {
                perror("read");
                break;
            }

            log_item.length = bytes_read;
            bounded_buffer_push(&args->ctx->log_buffer, &log_item);
        }
    }

    close(args->log_fd);
    free(args);
    return NULL;
}

int start_container(supervisor_ctx_t *ctx, control_request_t *request) {
    __label__ cleanup_pipe, cleanup_ctx_containers, cleanup_logger;

    container_record_t *container = malloc(sizeof(container_record_t));
    strncpy(container->id, request->container_id, CONTAINER_ID_LEN);
    strncpy(container->rootfs, request->rootfs, PATH_MAX);
    strncpy(container->command, request->command, CHILD_COMMAND_LEN);
    container->nice_value = request->nice_value;
    container->state = CONTAINER_STARTING;

    int pipe_fd[2] = {-1};
    int rc = pipe(pipe_fd);
    if (rc != 0) {
        perror("rc");
        free(container);
        return -1;
    }
    container->log_write_fd = pipe_fd[0];

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *tmp = ctx->containers;
    container_record_t *prev = NULL;
    while (tmp != NULL) {
        if (strncmp(tmp->id, container->id, sizeof(container->id)) == 0) {
            if (tmp->state == CONTAINER_RUNNING || tmp->state == CONTAINER_STARTING) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                rc = 1;
                goto cleanup_pipe;
            } else {
                if (prev != NULL) {
                    prev->next = tmp->next;
                } else {
                    ctx->containers = NULL;
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
    struct stat st = {0};
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

    log_capture_thread_args_t *args = malloc(sizeof(log_capture_thread_args_t));
    strncpy(args->container_id, container->id, sizeof(args->container_id));
    args->ctx = ctx;
    args->log_fd = pipe_fd[0];

    pthread_t container_log_thread;
    rc = pthread_create(&container_log_thread, NULL, capture_container_log, args);
    if (rc != 0) {
        fprintf(stderr, "Failed to create log capture thread for container %s\n", container->id);
        rc = -1;
        goto cleanup_logger;
    }

    pid_t pid = fork();
    if (pid == 0) {
        child_fn(request->rootfs, request->command, pipe_fd); // does not return
    } else if (pid > 0) {
        close(pipe_fd[1]);
        printf("Started container %s with PID %d\n", container->id, pid);
        container->host_pid = pid;
        container->started_at = time(NULL);
        container->state = CONTAINER_RUNNING;
        return 0;
    }

    perror("fork");
    rc = -1;

cleanup_logger:
    free(args);
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
    close(pipe_fd[0]);
    close(pipe_fd[1]);
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
            len += snprintf(buf + len, n - len, "%s (%s) - Start Time: %lu", container->id, state_str, container->started_at);
            if (state == CONTAINER_RUNNING) {
                len += snprintf(buf + len, n - len, ", PID: %d, Nice: %d", container->host_pid, container->nice_value);
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

int stop_container(supervisor_ctx_t *ctx, const char *container_id) {
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *container = ctx->containers;
    if (container == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return 1;
    } else {
        while (container != NULL) {
            if (strncmp(container_id, container->id, sizeof(container->id)) == 0) {
                pthread_mutex_unlock(&ctx->metadata_lock);

                if (container->state != CONTAINER_RUNNING) {
                    return 2;
                }

                int rc = kill(container->host_pid, SIGTERM);
                if (rc == -1) {
                    perror("kill");
                    return -1;
                }

                return 0;
            }
            container = container->next;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    return 1;
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

    switch (request.kind) {
        case CMD_SUPERVISOR:
            fprintf(stderr, "Received request of type 'CMD_SUPERVISOR'\n");
            break;
        case CMD_START:
            rc = start_container(args->ctx, &request);
            send_response = 1;
            if (rc == 0) {
                response.status = 201;
                snprintf(response.message, sizeof(response.message), "Created container with id '%s'", request.container_id);
            } else if (rc == 1) {
                response.status = 400;
                snprintf(response.message, sizeof(response.message), "Container with id '%s' already exists!", request.container_id);
            } else {
                response.status = 500;
                snprintf(response.message, sizeof(response.message), "An internal error occurred.");
            }
            break;
        case CMD_RUN:
            printf("Received run request\n");
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
            rc = stop_container(args->ctx, request.container_id);
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
    __label__ cleanup_bounded_buffer, cleanup_socket;
    (void) rootfs;

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

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        fprintf(stderr, "Failed to create logger thread. Error code: %d\n", rc);
        goto cleanup_bounded_buffer;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd == -1) {
        perror("socket");
        goto cleanup_bounded_buffer;
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
            break;
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
    }

    printf("\nStopping supervisor\n");

    close(ctx.shutdown_pipe_fds[1]); // sends shutdown signal to logging threads

cleanup_socket:
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

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
static int send_control_request(const control_request_t *req)
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
    } else if (rc > 0) {
        response.message[sizeof(response.message) - 1] = '\0';
        printf("Status: %d\nMessage: %s\n", response.status, response.message);
    }

    close(server_fd);
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

    return send_control_request(&req);
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

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
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

    return send_control_request(&req);
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

    return send_control_request(&req);
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
