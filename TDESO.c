#define _POSIX_C_SOURCE 199309L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <cjson/cJSON.h>

#define MAX_RECORDS 100000
#define LOG_QUEUE_SIZE 1000
#define MAX_SF 16
#define NUM_DEVICES 2

typedef struct {
    char city[64];
    char timestamp[64];
    float temperature;
    float humidity;
    float pressure;
    float battery;
    int sf;
} Record;

typedef struct {
    Record records[MAX_RECORDS];
    int count;
    pthread_mutex_t mutex;
} RecordList;

typedef struct {
    char messages[LOG_QUEUE_SIZE][256];
    int head;
    int tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} LogQueue;

typedef struct {
    const char *filename;
    const char *city;
} FileThreadData;

typedef struct {
    RecordList *records;
    LogQueue *logQueue;
} StatsThreadData;

// Global variables
RecordList globalRecords;
LogQueue logQueue;
struct timespec startTime, endTime;

// ---------------- Logging ----------------
void log_message(LogQueue *queue, const char *msg) {
    pthread_mutex_lock(&queue->mutex);
    snprintf(queue->messages[queue->tail], 256, "%s", msg);
    queue->tail = (queue->tail + 1) % LOG_QUEUE_SIZE;
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

void *logging_thread(void *arg) {
    LogQueue *queue = (LogQueue *)arg;
    FILE *logFile = fopen("processamento.log", "w");
    if (!logFile) {
        perror("Cannot open log file");
        pthread_exit(NULL);
    }

    while (1) {
        pthread_mutex_lock(&queue->mutex);
        while (queue->head == queue->tail)
            pthread_cond_wait(&queue->cond, &queue->mutex);

        char msg[256];
        strcpy(msg, queue->messages[queue->head]);
        queue->head = (queue->head + 1) % LOG_QUEUE_SIZE;
        pthread_mutex_unlock(&queue->mutex);

        if (strcmp(msg, "END") == 0) break;

        fprintf(logFile, "%s\n", msg);
        fflush(logFile);
    }

    fclose(logFile);
    pthread_exit(NULL);
}

// ---------------- File Reader ----------------
void *file_reader_thread(void *arg) {
    const char *filename = (const char *) arg;
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        char log[256];
        snprintf(log, 256, "Error opening file: %s", filename);
        log_message(&logQueue, log);
        pthread_exit(NULL);
    }

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);
    char *buffer = malloc(size + 1);
    fread(buffer, 1, size, fp);
    buffer[size] = '\0';
    fclose(fp);

    cJSON *root = cJSON_Parse(buffer);
    free(buffer);

    if (!root || !cJSON_IsArray(root)) {
        char log[256];
        snprintf(log, 256, "Invalid JSON in file: %s", filename);
        log_message(&logQueue, log);
        pthread_exit(NULL);
    }

    char log[256];
    snprintf(log, 256, "File loaded: %s, records: %d", filename, cJSON_GetArraySize(root));
    log_message(&logQueue, log);

    pthread_mutex_lock(&globalRecords.mutex);

    typedef struct {
        char *id;
        char *name;
        char *city;
    } Device;

    Device devices[NUM_DEVICES] = {
        {"67bfa5e2020d2a000aec6673", "Caxias - Praça (S2)", "Caxias do Sul"},
        {"67bfa56d36089a000a3254d5", "Bento - Praça (S3)", "Bento Gonçalves"}
    };


    for (int i = 0; i < cJSON_GetArraySize(root); i++) { // Array 
        cJSON *obj = cJSON_GetArrayItem(root, i);
        cJSON *payload = cJSON_GetObjectItem(obj, "brute_data");
        if (!payload) payload = cJSON_GetObjectItem(obj, "payload");
        if (!payload) continue;                                 // Verificar se não deveria dar erro 
        cJSON *dataArr = cJSON_GetObjectItem(payload, "data"); // brute_data/data OR payload/data OR data
        if (!cJSON_IsArray(dataArr)) continue;                 // Verificar isso aqui também


        Record rec = {0};
        cJSON *device_id = cJSON_GetObjectItem(payload, "device_id");
        for (int k = 0; k < NUM_DEVICES; k++){
            if(strcmp(devices[k].id,device_id->valuestring)) {strcpy(rec.city, devices[k].city);break;}
        }

        for (int j = 0; j < cJSON_GetArraySize(dataArr); j++) {
            cJSON *item = cJSON_GetArrayItem(dataArr, j);
            cJSON *var = cJSON_GetObjectItem(item, "variable");
            cJSON *val = cJSON_GetObjectItem(item, "value");
            cJSON *time = cJSON_GetObjectItem(item, "time");
            if (!var || !val || !time) continue;              // Talvez loggar

            if (cJSON_IsNumber(val)) {
                if (strcmp(var->valuestring, "temperature") == 0) { rec.temperature = val->valuedouble; strcpy(rec.timestamp, time->valuestring); }
                else if (strcmp(var->valuestring, "humidity") == 0) rec.humidity = val->valuedouble;
                else if (strcmp(var->valuestring, "airpressure") == 0) rec.pressure = val->valuedouble;
                else if (strcmp(var->valuestring, "batterylevel") == 0) rec.battery = val->valuedouble;
                else if (strcmp(var->valuestring, "snr") == 0) rec.sf = (int)val->valuedouble;
            }
        }
        globalRecords.records[globalRecords.count++] = rec;
    }
    pthread_mutex_unlock(&globalRecords.mutex);
    cJSON_Delete(root);

    pthread_exit(NULL);
}

// ---------------- Statistics ----------------
void *statistics_thread(void *arg) {
    StatsThreadData *data = (StatsThreadData *)arg;
    RecordList *records = data->records;

    typedef struct {
        char city[64];
        float tempMin, tempMax, tempSum;
        char tempMinTime[64], tempMaxTime[64];
        float humMin, humMax, humSum;
        char humMinTime[64], humMaxTime[64];
        float presMin, presMax, presSum;
        char presMinTime[64], presMaxTime[64];
        float batteryStart, batteryEnd;
        int sfUsed[MAX_SF], sfCount;
        int tempCount, humCount, presCount;
    } CityStats;

    CityStats cities[2];
    strcpy(cities[0].city, "Caxias do Sul");
    strcpy(cities[1].city, "Bento Gonçalves");

    for (int c = 0; c < 2; c++) {
        cities[c].tempMin = cities[c].humMin = cities[c].presMin = 1e30;
        cities[c].tempMax = cities[c].humMax = cities[c].presMax = -1e30;
        cities[c].tempSum = cities[c].humSum = cities[c].presSum = 0;
        cities[c].tempCount = cities[c].humCount = cities[c].presCount = 0;
        cities[c].batteryStart = -1;
        cities[c].batteryEnd = -1;
        cities[c].sfCount = 0;
    }

    pthread_mutex_lock(&records->mutex);
    for (int i = 0; i < records->count; i++) {
        Record r = records->records[i];
        CityStats *city = NULL;
        if (strcmp(r.city, "Caxias do Sul") == 0) city = &cities[0];
        else if (strcmp(r.city, "Bento Gonçalves") == 0) city = &cities[1];
        if (!city) continue;

        if (r.temperature && r.temperature < city->tempMin) { city->tempMin = r.temperature; strcpy(city->tempMinTime, r.timestamp); }
        if (r.temperature && r.temperature > city->tempMax) { city->tempMax = r.temperature; strcpy(city->tempMaxTime, r.timestamp); }
        if (r.temperature) { city->tempSum += r.temperature; city->tempCount++; }

        if (r.humidity && r.humidity < city->humMin) { city->humMin = r.humidity; strcpy(city->humMinTime, r.timestamp); }
        if (r.humidity && r.humidity > city->humMax) { city->humMax = r.humidity; strcpy(city->humMaxTime, r.timestamp); }
        if (r.humidity) { city->humSum += r.humidity; city->humCount++; }

        if (r.pressure && r.pressure < city->presMin) { city->presMin = r.pressure; strcpy(city->presMinTime, r.timestamp); }
        if (r.pressure && r.pressure > city->presMax) { city->presMax = r.pressure; strcpy(city->presMaxTime, r.timestamp); }
        if (r.pressure) { city->presSum += r.pressure; city->presCount++; }

        if (city->batteryStart < 0 && r.battery) city->batteryStart = r.battery;
        if (r.battery) city->batteryEnd = r.battery;

        int found = 0;
        for (int s = 0; s < city->sfCount; s++) if (city->sfUsed[s] == r.sf) found = 1;
        if (!found && r.sf && city->sfCount < MAX_SF) city->sfUsed[city->sfCount++] = r.sf;
    }
    pthread_mutex_unlock(&records->mutex);

    // Print results
    printf("============================================================\n");
    printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n\n");

    for (int c = 0; c < 2; c++) {
        printf("Cidade: %s\n", cities[c].city);
        printf("TEMPERATURA: Min %.2f em %s | Max %.2f em %s | Média %.2f\n",
            cities[c].tempMin, cities[c].tempMinTime,
            cities[c].tempMax, cities[c].tempMaxTime,
            cities[c].tempCount ? cities[c].tempSum / cities[c].tempCount : 0);
        printf("UMIDADE: Min %.2f em %s | Max %.2f em %s | Média %.2f\n",
            cities[c].humMin, cities[c].humMinTime,
            cities[c].humMax, cities[c].humMaxTime,
            cities[c].humCount ? cities[c].humSum / cities[c].humCount : 0);
        printf("PRESSÃO: Min %.2f em %s | Max %.2f em %s | Média %.2f\n",
            cities[c].presMin, cities[c].presMinTime,
            cities[c].presMax, cities[c].presMaxTime,
            cities[c].presCount ? cities[c].presSum / cities[c].presCount : 0);
        printf("BATERIA: Inicial %.2f | Final %.2f | Consumo %.2f\n",
            cities[c].batteryStart, cities[c].batteryEnd,
            cities[c].batteryStart - cities[c].batteryEnd);
        printf("SPREADING FACTORS: ");
        for (int s = 0; s < cities[c].sfCount; s++) {
            if (s > 0) printf(", ");
            printf("SF%d", cities[c].sfUsed[s]);
        }
        printf("\n\n");
    }

    log_message(&logQueue, "Statistics computed successfully.");
    pthread_exit(NULL);
}

// ---------------- Main ----------------
int main() {
    pthread_t threads[4];




    globalRecords.count = 0;
    pthread_mutex_init(&globalRecords.mutex, NULL);

    logQueue.head = logQueue.tail = 0;
    pthread_mutex_init(&logQueue.mutex, NULL);
    pthread_cond_init(&logQueue.cond, NULL);

    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // Start logging thread
    pthread_create(&threads[0], NULL, logging_thread, &logQueue);

    // File reading threads
    const char *f1 = "arquives/mqtt_senzemo_cx_bg.json";
    const char *f2 = "arquives/senzemo_cx_bg.json";
    pthread_create(&threads[1], NULL, file_reader_thread, (void *) f1);
    pthread_create(&threads[2], NULL, file_reader_thread, (void *) f2);

    // Wait for file threads
    pthread_join(threads[1], NULL);
    pthread_join(threads[2], NULL);

    // Start statistics thread
    StatsThreadData statsData = {&globalRecords, &logQueue};
    pthread_create(&threads[3], NULL, statistics_thread, &statsData);
    pthread_join(threads[3], NULL);

    // Stop logging
    log_message(&logQueue, "END");
    pthread_join(threads[0], NULL);

    clock_gettime(CLOCK_MONOTONIC, &endTime);
    double elapsed = (endTime.tv_sec - startTime.tv_sec) + (endTime.tv_nsec - startTime.tv_nsec)/1e9;
    printf("Tempo total de execução: %.2f segundos\n", elapsed);

    return 0;
}