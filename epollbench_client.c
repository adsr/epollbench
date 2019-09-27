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
#include <netinet/in.h>
#include <netinet/ip.h>

int opt_port = 1234;
char *opt_host = "127.0.0.1";
int opt_num_worker_threads = 1000;
int opt_num_messages = 10000000;

void setup_threads();
void join_threads();
void *worker_main(void *arg);
void parse_opts(int argc, char **argv);

int done = 0;
pthread_t *worker_threads = NULL;

#define die(pwhat) \
    do { fprintf(stderr, "%s: %s: %s\n", __func__, (pwhat), strerror(errno)); exit(1); } while(0)

#define stat_incr(pstat) \
    do { __sync_fetch_and_add(&(pstat), 1); } while(0)

int main(int argc, char **argv) {
    parse_opts(argc, argv);
    setup_threads();
    join_threads();
    return 0;
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
}

void *worker_main(void *arg) {
    size_t worker_num;

    worker_num = (size_t)arg;
    (void)worker_num;

    while (!done) {
        /* connect */

        /* loop N */
            /* write|sleep... */
            /* read|sleep... */
            /* check */
        /* end loop */

        /* disconnect */
    }

    return NULL;
}

void join_threads() {
    int i;
    for (i = 0; i < opt_num_worker_threads; i++) {
        pthread_join(*(worker_threads + i), NULL);
    }
    free(worker_threads);
}

void parse_opts(int argc, char **argv) {
    int c;
    struct option long_opts[] = {
        { "help",               no_argument,       NULL, 'h' },
        { "host",               required_argument, NULL, 'H' },
        { "port",               required_argument, NULL, 'p' },
        { "num-messages",       required_argument, NULL, 'n' },
        { "num-worker-threads", required_argument, NULL, 'w' },
        { 0,                    0,                 0,    0   }
    };

    optind = 1;
    while ((c = getopt_long(argc, argv, "hH:p:n:w:", long_opts, NULL)) != -1) {
        switch (c) {
            case 'h':
                printf("Usage: epollbench_client [options]\n");
                printf("\n");
                printf("Options:\n");
                printf("  -h, --help                         Show this help\n");
                printf("  -H, --host=<ip>                    Connect to `host` (default=%s)\n", opt_host);
                printf("  -p, --port=<port>                  Connect to host on `port` (default=%d)\n", opt_port);
                printf("  -n, --num-messages=<num>           Set number of messages to `num` (default=%d)\n", opt_num_messages);
                printf("  -w, --num-worker-threads=<num>     Set number of worker threads to `num` (default=%d)\n", opt_num_worker_threads);
                exit(0);
                break;
            case 'H': opt_host               = optarg;       break;
            case 'p': opt_port               = atoi(optarg); break;
            case 'n': opt_num_messages       = atoi(optarg); break;
            case 'w': opt_num_worker_threads = atoi(optarg); break;
        }
    }
}
