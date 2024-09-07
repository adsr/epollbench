#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
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
#include <arpa/inet.h>

int opt_port = 1234;
char *opt_host_ip = "127.0.0.1";
int opt_num_worker_threads = 512;
int opt_num_messages = 16384;
int opt_max_payload_len = 16384;
unsigned int opt_rand_seed = 0;
unsigned int opt_rw_chunk_size = 32;

void setup_threads();
void join_threads();
void *worker_main(void *arg);
void parse_opts(int argc, char **argv);

int done = 0;
pthread_t *worker_threads = NULL;
int message_count = 0;
#define RANDI_LEN 10240
int randi_array[RANDI_LEN];

#define die(pwhat) \
    do { fprintf(stderr, "%s: %s: %s\n", __func__, (pwhat), strerror(errno)); exit(1); } while(0)

#define stat_incr(pstat) \
    do { __sync_fetch_and_add(&(pstat), 1); } while(0)

#define randi(pi) randi_array[(((pi)++) + worker_num) % RANDI_LEN]

int main(int argc, char **argv) {
    int i;
    parse_opts(argc, argv);
    srand(opt_rand_seed);
    for (i = 0; i < RANDI_LEN; i++) randi_array[i] = rand();
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
    size_t worker_num, buf_size, cursor, rw_len, j;
    int sockfd, loop_count, sleep_us, i, r;
    struct sockaddr_in addr;
    uint32_t payload_len, total_len;
    ssize_t rv;
    unsigned char *buf, *rbuf;

    worker_num = (size_t)arg;
    (void)worker_num;
    r = 0;

    /* Alloc buf */
    buf_size = sizeof(payload_len) + opt_max_payload_len + 1;
    buf = calloc(buf_size, sizeof(unsigned char));
    rbuf = calloc(buf_size, sizeof(unsigned char));

    while (!done) {
        /* Connect to server */
        if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            die("socket");
        }
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(opt_host_ip);
        addr.sin_port = htons(opt_port);
        if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            /* Done (connect failure) */
            fprintf(stderr, "%s: connect: %s\n", __func__, strerror(errno));
            goto worker_main_done;
        }

        /* Mostly 0 but up to 100 micros */
        sleep_us = (int)(100.0 * pow(10.0, -300.0) * pow((double)(randi(r) % 1000), 100.0));
        loop_count = randi(r) % 10;
        fprintf(stderr, "%s: loop_count=%d sleep_us=%d\n", __func__, loop_count, sleep_us);

        /* Write/read in loop */
        for (i = 0; i < loop_count; i++) {

            /* Make payload */
            if (__sync_fetch_and_add(&message_count, 1) >= opt_num_messages) {
                goto worker_main_done;
                /*
                payload_len = 4;
                total_len = sizeof(payload_len) + payload_len;
                memcpy(buf + sizeof(payload_len), "done", 4);
                */
            } else {
                payload_len = (randi(r) % opt_max_payload_len);
                if (payload_len == 0) payload_len = 1;
                total_len = sizeof(payload_len) + payload_len;
                for (j = 0; j < total_len; j++) {
                    buf[j] = rand() % 256;
                }
            }
            memcpy(buf, &total_len, sizeof(total_len));

            /* Write */
            cursor = 0;
            while (cursor < total_len) {
                if (sleep_us > 0) {
                    rw_len = opt_rw_chunk_size;
                    if (rw_len > total_len - cursor) {
                        rw_len = total_len - cursor;
                    }
                } else {
                    rw_len = total_len - cursor;
                }
                if ((rv = write(sockfd, buf + cursor, rw_len)) < 1) {
                    /* Done (write failure) */
                    fprintf(stderr, "%s: write: %s\n", __func__, strerror(errno));
                    goto worker_main_done;
                }
                cursor += rv;
                if (sleep_us > 0) {
                    usleep(sleep_us);
                }
            }

            /* Read */
            cursor = 0;
            while (cursor < total_len) {
                if (sleep_us > 0) {
                    rw_len = opt_rw_chunk_size;
                    if (rw_len > total_len - cursor) {
                        rw_len = total_len - cursor;
                    }
                } else {
                    rw_len = total_len - cursor;
                }
                if ((rv = read(sockfd, rbuf + cursor, rw_len)) < 1) {
                    /* Done (read failure) */
                    if (rv < 0) {
                        fprintf(stderr, "%s: read: %s\n", __func__, strerror(errno));
                    }
                    goto worker_main_done;
                }
                cursor += rv;
                if (sleep_us > 0) {
                    usleep(sleep_us);
                }
            }

            /* Compare */
            if (memcmp(buf, rbuf, total_len) != 0) {
                /* Done (read failure) */
                fprintf(stderr, "%s: memcmp: %s\n", __func__, "Payload mismatch");
                goto worker_main_done;
            } else {
                fprintf(stderr, "%s: memcmp: %s\n", __func__, "Payload match!");
            }
        }

        /* Disconnect */
        close(sockfd);
        sockfd = -1;
    }

worker_main_done:
    if (sockfd >= 0) close(sockfd);
    free(buf);
    free(rbuf);

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
        { "host-ip",            required_argument, NULL, 'i' },
        { "port",               required_argument, NULL, 'p' },
        { "num-messages",       required_argument, NULL, 'n' },
        { "num-worker-threads", required_argument, NULL, 'w' },
        { "max-payload-len",    required_argument, NULL, 'm' },
        { "rw-chunk-size",      required_argument, NULL, 'r' },
        { "rand-seed",          required_argument, NULL, 's' },
        { 0,                    0,                 0,    0   }
    };

    optind = 1;
    while ((c = getopt_long(argc, argv, "hi:p:n:w:m:r:s:", long_opts, NULL)) != -1) {
        switch (c) {
            case 'h':
                printf("Usage: epollbench_client [options]\n");
                printf("\n");
                printf("Options:\n");
                printf("  -h, --help                         Show this help\n");
                printf("  -i, --host-ip=<ip>                 Connect to `host-ip` (default=%s)\n", opt_host_ip);
                printf("  -p, --port=<port>                  Connect to host on `port` (default=%d)\n", opt_port);
                printf("  -n, --num-messages=<num>           Set number of messages to `num` (default=%d)\n", opt_num_messages);
                printf("  -w, --num-worker-threads=<num>     Set number of worker threads to `num` (default=%d)\n", opt_num_worker_threads);
                printf("  -m, --max-payload-len=<num>        Set max payload len to `num` (default=%d)\n", opt_max_payload_len);
                printf("  -r, --rw-chunk-size=<num>          Read/write in increments of `num` between sleeps (default=%d)\n", opt_rw_chunk_size);
                printf("  -s, --rand-seed=<num>              Set rand seed (srand) to `num` (default=%d)\n", opt_rand_seed);
                exit(0);
                break;
            case 'i': opt_host_ip            = optarg;       break;
            case 'p': opt_port               = atoi(optarg); break;
            case 'n': opt_num_messages       = atoi(optarg); break;
            case 'w': opt_num_worker_threads = atoi(optarg); break;
            case 'm': opt_max_payload_len    = atoi(optarg); break;
            case 's': opt_rand_seed          = (unsigned int)atoi(optarg); break;
        }
    }
}
