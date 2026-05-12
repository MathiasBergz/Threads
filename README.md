1. Garanta que você tem os arquivos yyjson.c e yyjson.h no mesmo diretório do main.c.

2. Para compilar rode o seguinte comando:
gcc main.c yyjson.c -o main -lpthread -lm

3. Para executar:
./main

4. Caso queira executar e gerar um arquivo .txt com o relatório:
./main | tee relatorio.txt