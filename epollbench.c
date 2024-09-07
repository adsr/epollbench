#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <getopt.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <netinet/ip.h>

typedef struct _buf_t buf_t;
typedef struct _client_t client_t;

struct _buf_t {
    unsigned char *buf;
    size_t cap;
    size_t len;
};

struct _client_t {
    int sockfd;
    int lepfd;
    pthread_spinlock_t spinlock;
    pthread_mutex_t mutex;
    int lock_inited;
    buf_t rbuf;
    buf_t wbuf;
    size_t rbuf_wanted_len;
    size_t wbuf_cursor;
    int is_writing_to;
    int alive;
    client_t *next;
};

void join_threads();
void close_epolls();
void setup_threads();
void setup_epoll();
void setup_listener();
void *worker_main(void *arg);
void lock_client(client_t *client);
void unlock_client(client_t *client);
void *accept_main(void *arg);
void do_accept(int lepfd);
void add_client(int sockfd, int lepfd);
void handle_client(client_t *client, uint32_t events);
void kill_client(client_t *client);
void free_clients();
void setup_clients();
void lock_client_list();
void unlock_client_list();
void parse_opts(int argc, char **argv);

int opt_port = 1234;
int opt_backlog = 256;
int opt_num_worker_threads = 4;
int opt_num_accept_threads = 0;
int opt_epoll_nevents = 256;
int opt_epoll_timeout_ms = 1000;
int opt_epoll_per_worker = 0;
int opt_spinlock = 1;
int opt_soreuseport = 1;
int opt_epollexclusive = 1;
int opt_read_size = 256;
int opt_max_clients = 1024;
// TODO epolloneshot?
// TODO epollet?

int stat_client_accept = 0;
int stat_client_reject = 0;
int stat_spurious_accept = 0;
int stat_epoll_del_fail = 0;

int listenfd = -1;
int *listenfdp = NULL;
int epfd = -1;
int done = 0;
pthread_t *worker_threads = NULL;
pthread_t *accept_threads = NULL;
int *worker_epfds = NULL;
client_t *clients;
client_t *free_client_list;
pthread_spinlock_t client_list_spinlock;
pthread_mutex_t client_list_mutex;

#define die(pwhat) \
    do { fprintf(stderr, "(tid=%ld) %s: %s: %s\n", syscall(__NR_gettid), __func__, (pwhat), strerror(errno)); exit(1); } while(0)

#define stat_incr(pstat) \
    do { __sync_fetch_and_add(&(pstat), 1); } while(0)

int main(int argc, char **argv) {
    parse_opts(argc, argv);

    setup_clients();
    setup_epoll();
    setup_listener();
    setup_threads();

    join_threads();
    close(listenfd);
    free_clients();
    close_epolls();

    return 0;
}

void join_threads() {
    int i;
    if (opt_num_worker_threads > 0) {
        for (i = 0; i < opt_num_worker_threads; i++) {
            pthread_join(*(worker_threads + i), NULL);
        }
        free(worker_threads);
    }
    if (opt_num_accept_threads > 0) {
        for (i = 0; i < opt_num_accept_threads; i++) {
            pthread_join(*(accept_threads + i), NULL);
        }
        free(accept_threads);
    }
}

void close_epolls() {
    int i;
    if (opt_epoll_per_worker) {
        for (i = 0; i < opt_num_worker_threads; i++) {
            close(*(worker_epfds + i));
        }
    } else {
        close(epfd);
    }
}

void setup_threads() {
    size_t i;

    if (opt_num_worker_threads > 0) {
        worker_threads = calloc(opt_num_worker_threads, sizeof(pthread_t));
        for (i = 0; i < (size_t)opt_num_worker_threads; i++) {
            if ((errno = pthread_create(worker_threads + i, NULL, worker_main, (void*)i)) != 0) {
                die("pthread_create");
            }
        }
    }

    if (opt_num_accept_threads > 0) {
        accept_threads = calloc(opt_num_accept_threads, sizeof(pthread_t));
        for (i = 0; i < (size_t)opt_num_accept_threads; i++) {
            if ((errno = pthread_create(accept_threads + i, NULL, accept_main, NULL)) != 0) {
                die("pthread_create");
            }
        }
    }
}

void setup_epoll() {
    int i;
    if (opt_epoll_per_worker) {
        worker_epfds = calloc(opt_num_worker_threads, sizeof(int));
        for (i = 0; i < opt_num_worker_threads; i++) {
            if ((*(worker_epfds + i) = epoll_create(1)) < 0) {
                die("epoll_create");
            }
        }
    } else {
        if ((epfd = epoll_create(1)) < 0) {
            die("epoll_create");
        }
    }
}

void setup_listener() {
    struct epoll_event epe;
    struct sockaddr_in addr;
    int optval, sock_flags, i;

    /* Create listener socket */
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        die("socket");
    }

    /* Set SO_REUSEPORT */
    if (opt_soreuseport) {
        optval = 1;
        if ((setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval))) < 0) {
            die("setsockopt");
        }
    }

    if ((sock_flags = fcntl(listenfd, F_GETFL, 0)) < 0 || fcntl(listenfd, F_SETFL, sock_flags | O_NONBLOCK) < 0) {
        die("fcntl");
    }

    /* Bind to port */
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(opt_port);
    if (bind(listenfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        die("bind");
    }

    /* Start listening */
    if (listen(listenfd, opt_backlog) < 0) {
        die("listen");
    }

    if (opt_num_accept_threads <= 0) {
        /* Add listenfd to epollfd(s) */
        listenfdp = &listenfd;
        epe.events = EPOLLIN | EPOLLOUT | EPOLLET;
        epe.data.ptr = listenfdp;
        if (opt_epoll_per_worker) {
            for (i = 0; i < opt_num_worker_threads; i++) {
                if (epoll_ctl(*(worker_epfds + i), EPOLL_CTL_ADD, listenfd, &epe) < 0) {
                    die("epoll_ctl");
                }
            }
        } else {
            if (epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &epe) < 0) {
                die("epoll_ctl");
            }
        }
    }
}

void *worker_main(void *arg) {
    int lepfd, rv, i;
    void *ptr;
    struct epoll_event *events;
    size_t worker_num;

    worker_num = (size_t)arg;

    if (opt_epoll_per_worker) {
        lepfd = worker_epfds[worker_num];
    } else {
        lepfd = epfd;
    }

    events = calloc(opt_epoll_nevents, sizeof(struct epoll_event));

    while (!done) {
        rv = epoll_wait(lepfd, events, opt_epoll_nevents, opt_epoll_timeout_ms);
        if (rv < 0 && errno != EINTR) {
            die("epoll_wait");
        }
        for (i = 0; i < rv; i++) {
            ptr = events[i].data.ptr;
            if (ptr == listenfdp) {
                fprintf(stderr, "(tid=%ld) %s: epoll_wait events=%d accept\n", syscall(__NR_gettid), __func__, events[i].events);
                do_accept(lepfd);
            } else {
                fprintf(stderr, "(tid=%ld) %s: epoll_wait events=%d client=%p\n", syscall(__NR_gettid), __func__, events[i].events, ptr);
                lock_client(ptr);
                handle_client(ptr, events[i].events);
                unlock_client(ptr);
            }
        }
    }
    return NULL;
}

void lock_client(client_t *client) {
    if (opt_spinlock) {
        pthread_spin_lock(&client->spinlock);
    } else {
        pthread_mutex_lock(&client->mutex);
    }
}

void unlock_client(client_t *client) {
    if (opt_spinlock) {
        pthread_spin_unlock(&client->spinlock);
    } else {
        pthread_mutex_unlock(&client->mutex);
    }
}

void *accept_main(void *arg) {
    fd_set readfds;
    struct timeval timeout;
    int rv, worker_num, lepfd;

    (void)arg;
    while (!done) {
        FD_ZERO(&readfds);
        FD_SET(listenfd, &readfds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        rv = select(listenfd + 1, &readfds, NULL, NULL, &timeout);

        if (rv < 0 && errno != EINTR) {
            die("select");
        } else if (rv > 0) {
            /* Set epfd */
            if (opt_epoll_per_worker) {
                lepfd = worker_epfds[worker_num];

                worker_num += 1;
                if (worker_num >= opt_num_worker_threads) {
                    worker_num = 0;
                }
            } else {
                lepfd = epfd;
            }

            /* Accept client */
            fprintf(stderr, "(tid=%ld) %s: select accept\n", syscall(__NR_gettid), __func__);
            do_accept(lepfd);
        }
    }

    return NULL;
}

void do_accept(int lepfd) {
    int sockfd;
    int n;

    n = 0;
    while (1) {
        /* Accept client conn */
        if ((sockfd = accept4(listenfd, NULL, NULL, SOCK_NONBLOCK)) < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (n == 0) {
                    stat_incr(stat_spurious_accept);
                }
                return;
            } else if (errno == EINTR) {
                continue;
            }
            die("accept4");
        }

        /* Create client */
        stat_incr(stat_client_accept);
        add_client(sockfd, lepfd);

        n += 1;
    }
}

void add_client(int sockfd, int lepfd) {
    client_t *client;
    struct epoll_event epe;

    /* Get client from free list */
    client = NULL;
    lock_client_list();
    if (free_client_list != NULL) {
        client = free_client_list;
        free_client_list = free_client_list->next;
    }
    unlock_client_list();

    /* Reject if unable to find */
    if (!client) {
        stat_incr(stat_client_reject);
        close(sockfd);
    }

    /* (Re)init client */
    client->sockfd = sockfd;
    if (!client->lock_inited) {
        if (opt_spinlock) {
            if ((errno = pthread_spin_init(&client->spinlock, PTHREAD_PROCESS_SHARED)) != 0) {
                die("pthread_spin_init");
            }
        } else {
            if ((errno = pthread_mutex_init(&client->mutex, NULL)) != 0) {
                die("pthread_mutex_init");
            }
        }
        client->lock_inited = 1;
    }
    client->rbuf.len = 0;
    client->wbuf.len = 0;
    client->wbuf_cursor = 0;
    client->is_writing_to = 0;
    client->alive = 1;
    client->lepfd = lepfd;

    /* Add client to epoll */
    epe.events = EPOLLIN | EPOLLOUT | EPOLLET;
    #ifdef EPOLLEXCLUSIVE
    if (opt_epollexclusive) {
        epe.events |= EPOLLEXCLUSIVE;
    }
    #endif
    epe.data.ptr = client;
    if (epoll_ctl(lepfd, EPOLL_CTL_ADD, sockfd, &epe) < 0) {
        die("epoll_ctl");
    }
}

void handle_client(client_t *client, uint32_t events) {
    ssize_t rv;

    if (!client->alive) {
        return;
    }

    if (client->is_writing_to) {
        if (events & EPOLLERR) {
            kill_client(client);
            return;
        }
        if (!(events & EPOLLOUT)) {
            return;
        }
        while (client->wbuf_cursor < client->wbuf.len) {
            /* Write */
            rv = write(client->sockfd, client->wbuf.buf + client->wbuf_cursor, client->wbuf.len - client->wbuf_cursor);
            if (rv < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return;
                } else if (errno == EINTR) {
                    continue;
                } else {
                    die("write");
                }
            } else if (rv == 0) {
                kill_client(client);
            } else {
                client->wbuf_cursor += rv;
            }
        }
        client->is_writing_to = 0;
    } else {
        if (events & EPOLLRDHUP || events & EPOLLERR || events & EPOLLHUP) {
            kill_client(client);
            return;
        }
        if (!(events & EPOLLIN)) {
            return;
        }
        while (client->rbuf_wanted_len == 0 || client->rbuf.len < client->rbuf_wanted_len) {
            /* Ensure memory in rbuf */
            if (client->rbuf.len + opt_read_size > client->rbuf.cap) {
                client->rbuf.cap = client->rbuf.len + opt_read_size;
                client->rbuf.buf = realloc(client->rbuf.buf, client->rbuf.cap);
                if (!client->rbuf.buf) {
                    die("realloc rbuf");
                }
            }

            /* Read */
            rv = read(client->sockfd, client->rbuf.buf + client->rbuf.len, opt_read_size);
            if (rv < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return;
                } else if (errno == EINTR) {
                    continue;
                } else {
                    die("read");
                }
            } else if (rv == 0) {
                kill_client(client);
                return;
            } else {
                client->rbuf.len += rv;
                if (client->rbuf_wanted_len == 0 && client->rbuf.len >= sizeof(uint32_t)) {
                    memcpy(&client->rbuf_wanted_len, client->rbuf.buf, sizeof(uint32_t));
                }
            }
        }

        /* Check for special 'done' command */
        if (client->rbuf.len == sizeof(uint32_t) + 4
            && strncmp((char*)(client->rbuf.buf + sizeof(uint32_t)), "done", 4) == 0
        ) {
            done = 1;
            return;
        }

        /* Ensure cap in wbuf */
        if (client->rbuf.len > client->wbuf.cap) {
            client->wbuf.cap = client->rbuf.len;
            client->wbuf.buf = realloc(client->wbuf.buf, client->wbuf.cap);
            if (!client->wbuf.buf) {
                die("realloc wbuf");
            }
        }

        /* Echo rbuf to wbuf */
        memcpy(client->wbuf.buf, client->rbuf.buf, client->rbuf.len);
        client->wbuf.len = client->rbuf.len;
        client->wbuf_cursor = 0;

        client->rbuf.len = 0;
        client->rbuf_wanted_len = 0;
        client->is_writing_to = 1;
        handle_client(client, events | EPOLLOUT);
    }
}

void kill_client(client_t *client) {

    client->alive = 0;
    client->rbuf.len = 0;
    client->wbuf.len = 0;
    client->rbuf_wanted_len = 0;
    client->wbuf_cursor = 0;
    client->is_writing_to = 0;

    fprintf(stderr, "(tid=%ld) %s: client=%p\n", syscall(__NR_gettid),  __func__, (void*)client);

    if (epoll_ctl(client->lepfd, EPOLL_CTL_DEL, client->sockfd, NULL) < 0) {
        fprintf(stderr, "kill_client: epoll_ctl: %s\n", strerror(errno));
    }

    close(client->sockfd);

    client->lepfd = -1;
    client->sockfd = -1;

    /* Put client back in free list */
    lock_client_list();
    client->next = free_client_list;
    free_client_list = client;
    unlock_client_list();
}

void free_clients() {
    int i;
    client_t *client;
    for (i = 0; i <  opt_max_clients; i++) {
        client = clients + i;
        if (client->alive) {
            kill_client(client);
        }
        if (client->rbuf.buf) free(client->rbuf.buf);
        if (client->wbuf.buf) free(client->wbuf.buf);
    }
    free(clients);
}

void setup_clients() {
    int i;
    clients = calloc(opt_max_clients, sizeof(client_t));
    for (i = 0; i < opt_max_clients - 1; i++) {
        clients[i].next = clients + i + 1;
    }
    free_client_list = clients;
    if (opt_spinlock) {
        if ((errno = pthread_spin_init(&client_list_spinlock, PTHREAD_PROCESS_SHARED)) != 0) {
            die("pthread_spin_init");
        }
    } else {
        if ((errno = pthread_mutex_init(&client_list_mutex, NULL)) != 0) {
            die("pthread_mutex_init");
        }
    }
}

void lock_client_list() {
    if (opt_spinlock) {
        pthread_spin_lock(&client_list_spinlock);
    } else {
        pthread_mutex_lock(&client_list_mutex);
    }
}

void unlock_client_list() {
    if (opt_spinlock) {
        pthread_spin_unlock(&client_list_spinlock);
    } else {
        pthread_mutex_unlock(&client_list_mutex);
    }
}

void parse_opts(int argc, char **argv) {
    int c;
    struct option long_opts[] = {
        { "help",               no_argument,       NULL, 'h' },
        { "port",               required_argument, NULL, 'p' },
        { "listen-backlog",     required_argument, NULL, 'b' },
        { "num-worker-threads", required_argument, NULL, 'w' },
        { "num-accept-threads", required_argument, NULL, 'a' },
        { "epoll-num-events",   required_argument, NULL, 'n' },
        { "epoll-timeout-ms",   required_argument, NULL, 't' },
        { "epoll-per-worker",   required_argument, NULL, 'r' },
        { "use-spinlock",       required_argument, NULL, 's' },
        { "use-soreuseport",    required_argument, NULL, 'o' },
        { "use-epollexclusive", required_argument, NULL, 'x' },
        { "read-size",          required_argument, NULL, 'i' },
        { "max-clients",        required_argument, NULL, 'c' },
        { 0,                    0,                 0,    0   }
    };

    optind = 1;
    while ((c = getopt_long(argc, argv, "hp:b:w:a:n:t:r:s:o:x:i:c:", long_opts, NULL)) != -1) {
        switch (c) {
            case 'h':
                printf("Usage: epollbench [options]\n");
                printf("\n");
                printf("Options:\n");
                printf("  -h, --help                         Show this help\n");
                printf("  -p, --port=<port>                  Listen on `port` (default=%d)\n", opt_port);
                printf("  -b, --listen-backlog=<num>         Set listen backlog to `num` (default=%d)\n", opt_backlog);
                printf("  -w, --num-worker-threads=<num>     Set number of worker threads to `num` (default=%d)\n", opt_num_worker_threads);
                printf("  -a, --num-accept-threads=<num>     Set number of accept threads to `num` (default=%d)\n", opt_num_accept_threads);
                printf("  -n, --epoll-num-events=<num>       Set number of epoll events (per epoll) to `num` (default=%d)\n", opt_epoll_nevents);
                printf("  -t, --epoll-timeout-ms=<num>       Set epoll timeout in millis to `num` (default=%d)\n", opt_epoll_timeout_ms);
                printf("  -r, --epoll-per-worker=<1|0>       En/disable epoll per worker instead of shared epoll (default=%d)\n", opt_epoll_per_worker);
                printf("  -s, --use-spinlock=<1|0>           En/disable use of spinlocks instead of mutexes (default=%d)\n", opt_spinlock);
                printf("  -o, --use-soreuseport=<1|0>        En/disable use of SO_REUSEPORT (default=%d)\n", opt_soreuseport);
                printf("  -x, --use-epollexclusive=<1|0>     En/disable use of EPOLLEXCLUSIVE (default=%d)\n", opt_epollexclusive);
                printf("  -i, --read-size=<num>              Set read size to `num` (default=%d)\n", opt_read_size);
                printf("  -c, --max-clients=<num>            Set max clients to `num` (default=%d)\n", opt_max_clients);
                exit(0);
                break;
            case 'p': opt_port               = atoi(optarg); break;
            case 'b': opt_backlog            = atoi(optarg); break;
            case 'w': opt_num_worker_threads = atoi(optarg); break;
            case 'a': opt_num_accept_threads = atoi(optarg); break;
            case 'n': opt_epoll_nevents      = atoi(optarg); break;
            case 't': opt_epoll_timeout_ms   = atoi(optarg); break;
            case 'r': opt_epoll_per_worker   = atoi(optarg); break;
            case 's': opt_spinlock           = atoi(optarg); break;
            case 'o': opt_soreuseport        = atoi(optarg); break;
            case 'x': opt_epollexclusive     = atoi(optarg); break;
            case 'i': opt_read_size          = atoi(optarg); break;
            case 'c': opt_max_clients        = atoi(optarg); break;
        }
    }
}
