#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>

#define NUM_THREADS 4

FILE *ftpr;
float sumCXS;
float sumBG;
int contCXS;
int contBG;

typedef struct {
    char name[30];
    float maxTemp;
    float minTemp;
    float avgTemp;
} City;

void *readFile(void *path) {
    ftpr = fopen("", "r");
    if(ftpr == NULL) {
        printf("Arquivo nao encontrado.\n");
    }
    pthread_exit(NULL);
}

int main(int argc, char **argv) {
    pthread_t threads[NUM_THREADS];
    City cities[2];

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, &readFile, NULL);
    }

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    return 0;
}