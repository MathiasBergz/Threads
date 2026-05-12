#define _POSIX_C_SOURCE 199309L
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "yyjson.h"

#define MAX_RECORDS 100000
#define LOG_QUEUE_SIZE 1000
#define MAX_SF 6
#define NUM_DEVICES 2

typedef struct {
    char city[64];
    char block_timestamp[64];
    char temp_timestamp[64];
    char hum_timestamp[64];
    char pres_timestamp[64];
    char bat_timestamp[64];
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
    char log[256];

    snprintf(log, 256, "Thread iniciada: Abrindo arquivo %s para leitura", filename);
    log_message(&logQueue, log);

    FILE *fp = fopen(filename, "r");
    if (!fp) {
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

    yyjson_doc *doc = yyjson_read(buffer, strlen(buffer), 0);
    free(buffer);

    yyjson_val *root = yyjson_doc_get_root(doc);

    if (!root || !yyjson_is_arr(root)) {
        snprintf(log, 256, "Invalid JSON in file: %s", filename);
        log_message(&logQueue, log);
        pthread_exit(NULL);
    }

    snprintf(log, 256, "File loaded: %s, records: %zu", filename, yyjson_arr_size(root));
    log_message(&logQueue, log);

    typedef struct {
        char *id;
        char *name;
        char *city;
    } Device;

    Device devices[NUM_DEVICES] = {
        {"67bfa5e2020d2a000aec6673", "Caxias - Praça (S2)", "Caxias do Sul"},
        {"67bfa56d36089a000a3254d5", "Bento - Praça (S3)", "Bento Gonçalves"}
    };

    yyjson_arr_iter iter;
    yyjson_arr_iter_init(root, &iter);

    yyjson_val *obj;

    double prev_air_press[NUM_DEVICES] = {-999, -999}; // pos 0 == Caxias, pos 1 == Bento
    double prev_hum[NUM_DEVICES] = {-999, -999}; // pos 0 == Caxias, pos 1 == Bento
    double prev_temp[NUM_DEVICES] = {-999, -999}; // pos 0 == Caxias, pos 1 == Bento
    while ((obj = yyjson_arr_iter_next(&iter))) {
        bool flag_press_rep = false;
        bool flag_hum_rep = false;
        bool flag_temp_rep = false;

        yyjson_val *data_block_date_val = yyjson_obj_get(obj, "created_at");
        
        if (!data_block_date_val) {
            data_block_date_val = yyjson_obj_get(obj, "payload_date");
        }
        
        const char *data_block_date = data_block_date_val ? yyjson_get_str(data_block_date_val) : "Data_Desconhecida";
        
        yyjson_val *payload = yyjson_obj_get(obj, "brute_data");
        if (!payload) payload = yyjson_obj_get(obj, "payload");
        if (!payload) {
            snprintf(log, 256, "Não achou payload no arquivo %s\n", filename);
            log_message(&logQueue, log);
            continue;
        }
        yyjson_val *dataArr = yyjson_obj_get(payload, "data");
        if (!yyjson_is_arr(dataArr)) {
            snprintf(log, 256, "Não achou dataArr no arquivo %s\n", filename);
            log_message(&logQueue, log);
            continue;
        }

        Record rec = {0};
        strcpy(rec.block_timestamp, data_block_date);

        yyjson_val *device_id = yyjson_obj_get(payload, "device_id");
        const char *dev_id_str = yyjson_get_str(device_id);

        int dev_index = -1;

        for (int k = 0; k < NUM_DEVICES; k++) {
            if (strcmp(devices[k].id, dev_id_str) == 0) {
                strcpy(rec.city, devices[k].city); // k==0: Caxias, k==1: Bento
                dev_index = k;
                break;
            }
        }

        yyjson_arr_iter data_iter;
        yyjson_arr_iter_init(dataArr, &data_iter);

        yyjson_val *item;
        while ((item = yyjson_arr_iter_next(&data_iter))) {

            yyjson_val *var  = yyjson_obj_get(item, "variable");
            yyjson_val *val  = yyjson_obj_get(item, "value");
            yyjson_val *time = yyjson_obj_get(item, "time");

            if (!var || !val || !time) {
                snprintf(log, 256, "Faltou info: variable:%s | value:%s | time:%s no arquivo %s\n", yyjson_get_str(var),yyjson_get_str(val),yyjson_get_str(time),filename);
                log_message(&logQueue, log);
                continue;
            }

            const char *var_str  = yyjson_get_str(var);
            const char *time_str = yyjson_get_str(time);

            if (yyjson_is_num(val)) {
                double v = yyjson_get_num(val);
                
                if (strcmp(var_str, "temperature") == 0) {

                    if (v == prev_temp[dev_index]) {
                        flag_temp_rep = true;
                    }

                    rec.temperature = v;
                    prev_temp[dev_index] = v;
                    strcpy(rec.temp_timestamp, time_str);
                }
                else if (strcmp(var_str, "humidity") == 0) {
                    if (v == prev_hum[dev_index]) {
                        flag_hum_rep = true;
                    }
                    rec.humidity = v;
                    prev_hum[dev_index] = v;
                    strcpy(rec.hum_timestamp, time_str);
                }
                else if (strcmp(var_str, "airpressure") == 0) {
                    if (v == prev_air_press[dev_index]) {
                        flag_press_rep = true;
                    }
                    rec.pressure = v;
                    prev_air_press[dev_index] = v;
                    strcpy(rec.pres_timestamp, time_str);
                }
                else if (strcmp(var_str, "batterylevel") == 0) {
                    rec.battery = v;
                    strcpy(rec.bat_timestamp, time_str);
                } 
                else if (strcmp(var_str, "lora_spreading_factor") == 0) rec.sf = (int)v;
            }
        }
        if (flag_press_rep && flag_hum_rep && flag_temp_rep) {
            snprintf(log, 256, "Registro de dados repetidos ignorados: %s | %s no arquivo %s\n", rec.city, data_block_date, filename);
            log_message(&logQueue, log);
        }
        else { 
            pthread_mutex_lock(&globalRecords.mutex);
            globalRecords.records[globalRecords.count++] = rec;
            pthread_mutex_unlock(&globalRecords.mutex);
        }
    }
    
    yyjson_doc_free(doc);
    pthread_exit(NULL);
}

// ---------------- Formatação de data e hora ----------------
void formatar_data(const char *data_original, char *data_formatada) {
    int ano, mes, dia, hora, min, seg;
    
    if (sscanf(data_original, "%d-%d-%dT%d:%d:%d", &ano, &mes, &dia, &hora, &min, &seg) == 6) {

        snprintf(data_formatada, 64, "%02d/%02d/%04d %02d:%02d:%02d", dia, mes, ano, hora, min, seg);
    } else {
        strcpy(data_formatada, data_original);
    }
}

// ---------------- Formatação de data curta (Apenas DD/MM/AAAA) ----------------
void formatar_data_curta(const char *data_original, char *data_formatada) {
    int ano, mes, dia;
    
    // Lê apenas até o dia e ignora o resto (a partir do 'T')
    if (sscanf(data_original, "%d-%d-%d", &ano, &mes, &dia) == 3) {
        snprintf(data_formatada, 64, "%02d/%02d/%04d", dia, mes, ano);
    } else {
        strcpy(data_formatada, data_original); // Fallback caso dê erro
    }
}

// ---------------- Statistics ----------------
void *statistics_thread(void *arg) {
    StatsThreadData *data = (StatsThreadData *)arg;
    RecordList *records = data->records;

    char log[256];
    snprintf(log, 256, "Iniciando calculo estatistico para %d registros validos.", records->count);
    log_message(&logQueue, log);

    typedef struct {
        char city[64];
        char periodStart[64], periodEnd[64];
        float tempMin, tempMax, tempSum;
        char tempMinTime[64], tempMaxTime[64];
        float humMin, humMax, humSum;
        char humMinTime[64], humMaxTime[64];
        float presMin, presMax, presSum;
        char presMinTime[64], presMaxTime[64];
        float batteryStart, batteryEnd;
        char batStartTime[64], batEndTime[64];
        int sfUsed[MAX_SF], sfCount;
        int totalRegCount, tempCount, humCount, presCount;
    } CityStats;

    CityStats cities[2];
    strcpy(cities[0].city, "Caxias do Sul");
    strcpy(cities[1].city, "Bento Gonçalves");

    for (int c = 0; c < NUM_DEVICES; c++) {
        cities[c].periodStart[0] = cities[c].periodEnd[0] = '\0';
        cities[c].tempMin = cities[c].humMin = cities[c].presMin = 1e30;
        cities[c].tempMax = cities[c].humMax = cities[c].presMax = -1e30;
        cities[c].tempSum = cities[c].humSum = cities[c].presSum = 0;
        cities[c].tempCount = cities[c].humCount = cities[c].presCount = 0;
        cities[c].totalRegCount = 0;
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
        if (!city) {
            log_message(&logQueue, "Há registros sem cidade.");
            continue;
        }

        city->totalRegCount++;

        if (city->periodStart[0] == '\0' || strcmp(r.block_timestamp, city->periodStart) < 0) {
            strcpy(city->periodStart, r.block_timestamp);
        }
        if (city->periodEnd[0] == '\0' || strcmp(r.block_timestamp, city->periodEnd) > 0) {
            strcpy(city->periodEnd, r.block_timestamp);
        }

        if (r.temperature && r.temperature < city->tempMin) { city->tempMin = r.temperature; strcpy(city->tempMinTime, r.temp_timestamp); }
        if (r.temperature && r.temperature > city->tempMax) { city->tempMax = r.temperature; strcpy(city->tempMaxTime, r.temp_timestamp); }
        if (r.temperature) { city->tempSum += r.temperature; city->tempCount++; }

        if (r.humidity && r.humidity < city->humMin) { city->humMin = r.humidity; strcpy(city->humMinTime, r.hum_timestamp); }
        if (r.humidity && r.humidity > city->humMax) { city->humMax = r.humidity; strcpy(city->humMaxTime, r.hum_timestamp); }
        if (r.humidity) { city->humSum += r.humidity; city->humCount++; }

        if (r.pressure && r.pressure < city->presMin) { city->presMin = r.pressure; strcpy(city->presMinTime, r.pres_timestamp); }
        if (r.pressure && r.pressure > city->presMax) { city->presMax = r.pressure; strcpy(city->presMaxTime, r.pres_timestamp); }
        if (r.pressure) { city->presSum += r.pressure; city->presCount++; }

        if (r.battery){
            if (city->batteryStart < 0) {
                city->batteryStart = city->batteryEnd = r.battery; 
                strcpy(city->batStartTime, r.bat_timestamp); strcpy(city->batEndTime, r.bat_timestamp);
            }
            else{
                if(strcmp(city->batStartTime,r.bat_timestamp) > 0){
                    city->batteryStart = r.battery;
                    strcpy(city->batStartTime, r.bat_timestamp);
                }
                if(strcmp(r.bat_timestamp,city->batEndTime) > 0){
                    city->batteryEnd = r.battery;
                    strcpy(city->batEndTime, r.bat_timestamp);
                }
            }
            
        }

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

    char StartPeriod_Formatado[64], EndPeriod_Formatado[64];
    for (int c = 0; c < NUM_DEVICES; c++) {
        char f_start[64], f_end[64];
        formatar_data_curta(cities[c].periodStart, StartPeriod_Formatado);
        formatar_data_curta(cities[c].periodEnd, EndPeriod_Formatado);
        
        printf("Cidade Analisada: %s\n", cities[c].city);
        printf("Total de registros processados: %d\n", cities[c].totalRegCount);
        printf("Período analisado: %s a %s\n\n", StartPeriod_Formatado, EndPeriod_Formatado);
    }

    printf("------------------------------------------------------------\n");
    printf("TEMPERATURA (°C)\n");
    printf("------------------------------------------------------------\n"); 
    printf("%-17s | %-6s | %-21s | %-6s | %-21s | %s\n", "Cidade", "Mínima", "Data/Hora", "Máxima", "Data/Hora", "Média");
    printf("-----------------------------------------------------------------------------------------------\n");

    for (int c = 0; c < NUM_DEVICES; c++){
        char dataFormatada_TempMin[64], dataFormatada_TempMax[64];
        formatar_data(cities[c].tempMinTime, dataFormatada_TempMin);
        formatar_data(cities[c].tempMaxTime, dataFormatada_TempMax);
        
        printf("%s | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n",
                c==0 ? "Caxias do Sul    " : "Bento Gonçalves  ", cities[c].tempMin, dataFormatada_TempMin, cities[c].tempMax, dataFormatada_TempMax,
                cities[c].tempCount ? cities[c].tempSum / cities[c].tempCount : 0);
    }
    printf("\n\n");

    printf("------------------------------------------------------------\n");
    printf("UMIDADE (%%)\n");
    printf("------------------------------------------------------------\n"); 
    printf("%-17s | %-6s | %-21s | %-6s | %-21s | %s\n", "Cidade", "Mínima", "Data/Hora", "Máxima", "Data/Hora", "Média");
    printf("-----------------------------------------------------------------------------------------------\n");

    for (int c = 0; c < NUM_DEVICES; c++){
        char dataFormatada_HumMin[64], dataFormatada_HumMax[64];
        formatar_data(cities[c].humMinTime, dataFormatada_HumMin);
        formatar_data(cities[c].humMaxTime, dataFormatada_HumMax);
        
        printf("%s | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n",
                c==0 ? "Caxias do Sul    " : "Bento Gonçalves  ", cities[c].humMin, dataFormatada_HumMin, cities[c].humMax, dataFormatada_HumMax,
                cities[c].humCount ? cities[c].humSum / cities[c].humCount : 0);
    }
    printf("\n\n");

    printf("------------------------------------------------------------\n");
    printf("PRESSÃO ATMOSFÉRICA (hPa)\n");
    printf("------------------------------------------------------------\n"); 
    printf("%-17s | %-6s | %-21s | %-6s | %-21s | %s\n", "Cidade", "Mínima", "Data/Hora", "Máxima", "Data/Hora", "Média");
    printf("-----------------------------------------------------------------------------------------------\n");

    for (int c = 0; c < NUM_DEVICES; c++){
        char dataFormatada_PresMin[64], dataFormatada_PresMax[64];
        formatar_data(cities[c].presMinTime, dataFormatada_PresMin);
        formatar_data(cities[c].presMaxTime, dataFormatada_PresMax);
        
        printf("%s | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n",
                c==0 ? "Caxias do Sul    " : "Bento Gonçalves  ", cities[c].presMin, dataFormatada_PresMin, cities[c].presMax, dataFormatada_PresMax,
                cities[c].presCount ? cities[c].presSum / cities[c].presCount : 0);
    }
    printf("\n\n");

    printf("------------------------------------------------------------\n");
    printf("BATERIA\n");
    printf("------------------------------------------------------------\n");
    printf("%-17s | %-11s | %-9s | %s\n", "Cidade", "Inicial (V)", "Final (V)", "Consumo (V)");
    printf("------------------------------------------------------------\n");
    for (int c = 0; c < NUM_DEVICES; c++) {
        printf("%s | %-11.2f | %-9.2f | %.2f\n",
                c == 0 ? "Caxias do Sul    " : "Bento Gonçalves  ",
                cities[c].batteryStart, cities[c].batteryEnd,
                cities[c].batteryStart - cities[c].batteryEnd);
    }
    printf("\n\n");

    printf("------------------------------------------------------------\n");
    printf("SPREADING FACTORS UTILIZADOS\n");
    printf("------------------------------------------------------------\n");
    printf("%-17s | %s\n", "Cidade", "SF utilizados");
    printf("------------------------------------------------------------\n");
    for (int c = 0; c < NUM_DEVICES; c++) {
        
        // Ordenação Spreading Factors
        int aux;
        for (int i = 0; i < cities[c].sfCount-1; i++) {
            for (int j = 0; j < cities[c].sfCount-1-i; j++) {
                if (cities[c].sfUsed[j] > cities[c].sfUsed[j+1]) {
                    aux = cities[c].sfUsed[j];
                    cities[c].sfUsed[j] = cities[c].sfUsed[j+1];
                    cities[c].sfUsed[j+1] = aux;
                }
            }
        }

        printf("%s | ", c == 0 ? "Caxias do Sul    " : "Bento Gonçalves  ");
        if (cities[c].sfCount == 0) {
            printf("Nenhum registro de Spreading Factor encontrado\n");
        } else {
            for (int s = 0; s < cities[c].sfCount; s++) {
                if (s > 0) printf(", ");
                printf("SF%d", cities[c].sfUsed[s]);
            }
            printf("\n");
        }
    }
    printf("\n\n");

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

    log_message(&logQueue, "Sistema inicializado. Estruturas e mutexes criados.");

    // File reading threads
    const char *f1 = "files/mqtt_senzemo_cx_bg.json";
    const char *f2 = "files/senzemo_cx_bg.json";

    log_message(&logQueue, "Iniciando threads de leitura dos arquivos JSON...");

    pthread_create(&threads[1], NULL, file_reader_thread, (void *) f1);
    pthread_create(&threads[2], NULL, file_reader_thread, (void *) f2);

    // Wait for file threads
    pthread_join(threads[1], NULL);
    pthread_join(threads[2], NULL);

    log_message(&logQueue, "Leitura de todos os arquivos concluida. Iniciando analise de dados...");

    // Start statistics thread
    StatsThreadData statsData = {&globalRecords, &logQueue};
    pthread_create(&threads[3], NULL, statistics_thread, &statsData);
    pthread_join(threads[3], NULL);

    // Stop logging
    log_message(&logQueue, "END");
    pthread_join(threads[0], NULL);

    clock_gettime(CLOCK_MONOTONIC, &endTime);
    double elapsed = (endTime.tv_sec - startTime.tv_sec) + (endTime.tv_nsec - startTime.tv_nsec)/1e9;

    printf("------------------------------------------------------------\n");
    printf("DESEMPENHO\n");
    printf("------------------------------------------------------------\n");
    printf("Tempo total de execução: %.2f segundos\n", elapsed);
    
    printf("Threads utilizadas: 4\n");
    printf(" - Thread 1: registro de logs em background\n");
    printf(" - Thread 2: leitura do arquivo 1\n");
    printf(" - Thread 3: leitura do arquivo 2\n");
    printf(" - Thread 4: cálculo das estatísticas\n\n");
    
    printf("Arquivo de log gerado: processamento.log\n\n");
    
    printf("============================================================\n");
    printf("Processamento finalizado com sucesso.\n");
    printf("============================================================\n");

    pthread_mutex_destroy(&globalRecords.mutex);
    pthread_mutex_destroy(&logQueue.mutex);
    pthread_cond_destroy(&logQueue.cond);

    return 0;
}