#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>

#define NUM_THREADS 4

void *readFile(void *path) {
    printf("Read File function \n");
    pthread_exit(NULL);
}

int main(int argc, char **argv) {
    pthread_t threads[NUM_THREADS];

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, &readFile, NULL);
    }

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}