#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>

#define SYS_SOCKETCALL_I386 102
#define SYS_SOCKET 1

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <address_family> <socket_type>\n", argv[0]);
        return 2;
    }
    unsigned int family = atoi(argv[1]);
    unsigned int type = atoi(argv[2]);

    /*
     * The int $0x80 ia32 compat path truncates all registers to 32 bits.
     * The args pointer must live below 4 GB, so allocate it with MAP_32BIT.
     */
    unsigned int *args = mmap(NULL, 4096,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_32BIT,
        -1, 0);
    if (args == MAP_FAILED) {
        perror("mmap");
        return 2;
    }
    args[0] = family;
    args[1] = type;
    args[2] = 0;

    int ret;
    asm volatile (
        "int $0x80"
        : "=a"(ret)
        : "a"(SYS_SOCKETCALL_I386), "b"(SYS_SOCKET), "c"(args)
        : "memory"
    );

    if (ret < 0) {
        errno = -ret;
        perror("socket");
        return 1;
    }

    printf("socket(%u, %u, 0) created via socketcall\n", family, type);
    close(ret);
    return 0;
}
