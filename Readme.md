Necessario para a execução de toda a solução

- Python 3
- Docker: RDS Postgres
- Pyspark: Processamento csv

1- Executar o comando docker-compose -f docker-compose.postgres.yaml up para instanciar o servidor do Postgres
2- Executar o arquivo "get_api_data.py" para realizar carga no Banco Relacional utilizando a API
    - O mesmo código pode ser adaptado para atualizar os meses seguintes 
3- Analise realizada a partir dos dados nos arquivos CSV utilizando PYSPARK:
    - Transformação dos arquivos para PARQUET (redução de armazenamento)