epollbench_cflags:=-std=c99 -Wall -Wextra -pedantic -g -O2 $(CFLAGS)
epollbench_libs:=-pthread -lm $(LDLIBS)

all: epollbench epollbench_client

epollbench: epollbench.c
	$(CC) $(epollbench_cflags) epollbench.c -o epollbench $(epollbench_libs)

epollbench_client: epollbench_client.c
	$(CC) $(epollbench_cflags) epollbench_client.c -o epollbench_client $(epollbench_libs)

clean:
	rm -f epollbench epollbench_client

.PHONY: all
