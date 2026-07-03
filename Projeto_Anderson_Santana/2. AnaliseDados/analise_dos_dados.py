from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, year, to_date, current_date, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("AnaliseClientesCompleta") \
    .getOrCreate()

# Definição do Schema
schema = StructType([
    StructField("cod_cliente", StringType(), True),
    StructField("nm_cliente", StringType(), True),
    StructField("nm_pais_cliente", StringType(), True),
    StructField("nm_cidade_cliente", StringType(), True),
    StructField("nm_rua_cliente", StringType(), True),
    StructField("num_casa", StringType(), True),
    StructField("telefone_cliente", StringType(), True),
    StructField("dt_nascimento_cliente", StringType(), True),
    StructField("dt_atualizacao", StringType(), True),
    StructField("tp_pessoa", StringType(), True),
    StructField("vl_renda", DoubleType(), True)
])

# Leitura do arquivo
df = spark.read.option("header", "true").schema(schema).csv("clientes_sinteticos.csv")

# Tratamento de datas compatível com Spark 3.x
# Identifica o padrão por caractere e converte. Se não for nenhum dos dois, vira NULL.
df_analise = df.withColumn("dt_nascimento_cliente", 
    when(col("dt_nascimento_cliente").contains("/"), to_date(col("dt_nascimento_cliente"), "dd/MM/yyyy"))
    .when(col("dt_nascimento_cliente").contains("-"), to_date(col("dt_nascimento_cliente"), "yyyy-MM-dd"))
    .otherwise(None)
)

# 1. Identificar os 5 clientes que mais sofreram atualizações
# Usamos o DataFrame original 'df' pois ele contém todas as linhas (histórico)
print("\n--- Top 5 clientes com mais atualizações ---")
df.groupBy("cod_cliente") \
    .agg(count("*").alias("total_atualizacoes")) \
    .orderBy(col("total_atualizacoes").desc()) \
    .show(5)

# 2. Calcular a média de idade dos clientes
print("--- Média de idade de todos os clientes ---")
df_analise.withColumn("idade", (year(current_date()) - year(col("dt_nascimento_cliente")))) \
    .select(avg("idade")).show()