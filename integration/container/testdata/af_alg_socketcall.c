#include <stdio.h>
#include <errno.h>

#define SYS_SOCKETCALL_I386 102
#define SYS_SOCKET 1
#define AF_ALG 38
#define SOCK_SEQPACKET 5

int main() {
    unsigned long args[3] = { AF_ALG, SOCK_SEQPACKET, 0 };
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

    printf("AF_ALG socket created via socketcall\n");
    return 0;
}
