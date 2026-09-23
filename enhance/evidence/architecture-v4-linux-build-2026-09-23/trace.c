#define _GNU_SOURCE
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <ucontext.h>
#include <execinfo.h>
static void crash(int sig, siginfo_t *info, void *ctx) {
 ucontext_t *uc=ctx; void *frames[24];
 dprintf(2,"signal=%d address=%p rip=%llx\n",sig,info->si_addr,(unsigned long long)uc->uc_mcontext.gregs[REG_RIP]);
 int n=backtrace(frames,24);backtrace_symbols_fd(frames,n,2);_exit(128+sig);
}
__attribute__((constructor)) static void setup(void) {
 struct sigaction sa={0};sa.sa_sigaction=crash;sa.sa_flags=SA_SIGINFO;
 sigaction(SIGSEGV,&sa,0);sigaction(SIGILL,&sa,0);write(2,"trace-installed\n",16);
}
