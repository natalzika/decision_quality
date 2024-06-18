import awswrangler as wr
import boto3
import pandas as pd
import logging
from botocore.exceptions import ClientError

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para contar itens em uma partição específica
def count_items_in_partition(database: str, table: str, partition: str) -> int:
    query = f"""
    SELECT COUNT(1) AS count
    FROM "{database}"."{table}"
    WHERE anomesdia = {partition}
    """
    logger.info("Executing query to count items in partition")
    result = wr.athena.read_sql_query(query, database=database)
    count = result['count'][0]
    logger.info(f"Count result for partition {partition}: {count}")
    return count

# Função para obter o schema da tabela do Glue Data Catalog
def get_table_schema(database: str, table: str) -> set:
    glue_client = boto3.client('glue')
    try:
        response = glue_client.get_table(DatabaseName=database, Name=table)
        schema = {col['Name'] for col in response['Table']['StorageDescriptor']['Columns']}
        logger.info(f"Schema for {database}.{table}: {schema}")
        return schema
    except ClientError as e:
        logger.error(f"An error occurred while fetching the table schema: {e}")
        return set()

# Função para extrair colunas especificadas nas regras de qualidade de dados
def extract_columns_from_ruleset(ruleset: dict) -> set:
    columns = set()
    for rule in ruleset.get('rules', []):
        expression = rule.get('Expression', '')
        columns.update(col.strip() for col in expression.replace('IS NOT NULL', '').replace('>', '').replace('<', '').replace('=', '').replace('AND', ',').replace('BETWEEN', ',').split(',') if col.strip())
    return columns

# Função para montar um dataframe com as colunas especificadas nas regras de qualidade de dados
def filter_dataframe_with_payload(database: str, table: str, partition: str, dqdl_payload: dict) -> pd.DataFrame:
    schema = get_table_schema(database, table)
    print(f'This is schema:{schema}')
    print(f'This is payload:{dqdl_payload}')
    columns_in_ruleset = extract_columns_from_ruleset(dqdl_payload)
    
    # Filtrar colunas que estão presentes no schema e especificadas no ruleset
    valid_columns = columns_in_ruleset.intersection(schema)

    return valid_columns    


# Função polimórfica para contagem e processamento com payload
def count_items(database: str, table: str, partition: str, row_threshold: int = 100_000, dqdl_payload: dict = None, role: str = None):

    # Determina o ambiente e realiza a contagem
    count = count_items_in_partition(database, table, partition)
    
    # Se o número de linhas for maior que o threshold, processa o payload e retorna o DataFrame
    if count > row_threshold and dqdl_payload:
        df = filter_dataframe_with_payload(database, table, partition, dqdl_payload)
        print(df)
        #logger.info(f"DataFrame with specified columns: \n{df.head()}")

# Exemplo de uso
if __name__ == "__main__":
    database = 'y'
    table = 'x'
    partition = '20230601'  # Partição no formato YYYYMMDD
    row_threshold = 4  # Limite de linhas para decidir entre Python Shell e Spark
    dqdl_payload = {
        'rules': [  # Regras de qualidade de dados em DQDL
            {
                'Name': 'NonNullCheck',
                'Expression': 'id IS NOT NULL'
            },
            {
                'Name': 'RangeCheck',
                'Expression': 'value > 0 AND id BETWEEN 1 AND 1000'
            }
        ]
    }
    result = count_items(database, table, partition, row_threshold, dqdl_payload)
    if isinstance(result, pd.DataFrame):
        logger.info(f"DataFrame result:\n{result}")
    else:
        logger.info(f"Total items in partition {partition}: {result}")
