INSTRUÇÕES PARA EXECUTAR O ALGORÍTMO:

1. Clone o reposítório do GitHub na sua máquina;

2. Garanta que você tem os arquivos "yyjson.c" e "yyjson.h" no mesmo diretório do "main.c";

3. Garanta que você tenha a pasta "files" com os arquivos "senzemo_cx_bg.json" e "mqtt_senzemo_cx_bg.json" dentro dela, e que a pasta files esteja no mesmo diretório que o "main.c"

4. Para compilar rode o seguinte comando:
gcc main.c yyjson.c -o main -lpthread -lm

5. Para executar:
./main
