#include <stdio.h>
#include <signal.h>

int x = 5;

void sighandler(int sig) {
    x -= 1;
    printf("x: %d\n", x);
    fflush(stdout);
    if (x <= 0) signal(sig, SIG_DFL);
}

int main() {
    signal(SIGINT, sighandler);

    while (x > 0);

    return 0;
}
