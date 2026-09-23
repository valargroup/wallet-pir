#include <stdio.h>
int main(void) { __builtin_cpu_init(); printf("avx2=%d bmi2=%d fma=%d pclmul=%d\n", !!__builtin_cpu_supports("avx2"), !!__builtin_cpu_supports("bmi2"), !!__builtin_cpu_supports("fma"), !!__builtin_cpu_supports("pclmul")); return 0; }
