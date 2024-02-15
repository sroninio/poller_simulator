#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#define CL_SIZE (64)
#define ARR_SIZE_CLS (5000000 * 3) // Size of the big array
#define NUM_ITERATIONS (1000)
/*
'''
#define RAND_A 1103515245
#define RAND_C 12345
#define RAND_M 2147483647
'''
*/
#define RAND_A 1104715245
#define RAND_C 16345
#define RAND_M 2147988697

//#define TO_MEASURE_TOUCHES (1)

int main(int argc, char *argv[])
{
    int CACHE_SIZE_CLS = atoi(argv[1]);
    struct timespec start, end;
    volatile char *big_array = (volatile char *)malloc(ARR_SIZE_CLS * CL_SIZE * sizeof(char));
    time_t rand_seed, curr_rand_val;
    if (big_array == NULL) {
        printf("Memory allocation failed\n");
        return 1;
    }

#ifdef TO_MEASURE_TOUCHES
    char * touched_cls_array = (char*)calloc(ARR_SIZE_CLS, (sizeof(char)));
#endif
    
    rand_seed = time(NULL);
    clock_gettime(CLOCK_MONOTONIC, &start);
    uint64_t ret = 17;
    for (int iter = 0; iter < NUM_ITERATIONS; iter++){
	curr_rand_val = rand_seed;
        for (int i = 0; i < CACHE_SIZE_CLS; i++) {
	    curr_rand_val = (RAND_A * curr_rand_val + RAND_C) % RAND_M;
            int cl_index = (curr_rand_val  % ARR_SIZE_CLS);
            big_array[cl_index * CL_SIZE] = big_array[cl_index * CL_SIZE] * 23 + ret;
	    ret = big_array[cl_index * CL_SIZE];
#ifdef TO_MEASURE_TOUCHES
            touched_cls_array[cl_index] = 1;
#endif
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    long int diff = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    free((void*)big_array);
#ifdef TO_MEASURE_TOUCHES
        uint64_t cnt = 0;
        for (int iter = 0; iter < ARR_SIZE_CLS; iter++) {
                cnt += touched_cls_array[iter];
        }
        printf("touched %ld out of %ld ", cnt, CACHE_SIZE_CLS);
#endif
    printf ("total_loads: %ld total_time_in_ns %ld avg_time_per_acces_in_ns %f \n", NUM_ITERATIONS * CACHE_SIZE_CLS, diff, ((float)diff) / ((float)(NUM_ITERATIONS * CACHE_SIZE_CLS)));
    return (ret != 1567984746);
}

