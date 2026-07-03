import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, row_number, when, current_date, date_format, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # Inicializa a sessão local do Spark
    spark = SparkSession.builder \
        .appName("Desafio-ETL-Local") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    # Caminhos configurados para o seu ambiente local em Documents
    input_path = "datasets/clientes_sinteticos.csv"
    bronze_output_path = "output/bucket-bronze/tabela_cliente_landing"
    silver_output_path = "output/bucket-silver/tb_cliente"

    # =========================================================================
    # PASSO 1: Especifique um schema para o dataset.
    # =========================================================================
    # Lemos as datas como StringType primeiro para tratarmos o formato dd/MM/yyyy do arquivo
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

    # Leitura do CSV aplicando o Schema estrutural
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .schema(schema) \
        .csv(input_path)

    # Conversão explícita das strings de data para DateType do Spark usando o padrão correto (dd/MM/yyyy)
    df_parsed_dates = df_raw \
        .withColumn("dt_nascimento_cliente", to_date(col("dt_nascimento_cliente"), "dd/MM/yyyy")) \
        .withColumn("dt_atualizacao", to_date(col("dt_atualizacao"), "dd/MM/yyyy"))

    # Criação da partição física/lógica exigida: anomesdia (AAAAMMDD) baseado no processamento
    df_with_partition = df_parsed_dates.withColumn("anomesdia", date_format(current_date(), "yyyyMMdd"))

    # =========================================================================
    # PASSO 2: Trate os nomes dos clientes para que fiquem todos com letra maiúscula.
    # =========================================================================
    df_upper_name = df_with_partition.withColumn("nm_cliente", upper(col("nm_cliente")))

    # =========================================================================
    # PASSO 3: Renomeie a coluna telefone_cliente para num_telefone_cliente.
    # =========================================================================
    df_bronze_final = df_upper_name.withColumnRenamed("telefone_cliente", "num_telefone_cliente")

    # =========================================================================
    # PASSO 4: Realize a escrita do dado no bucket s3://bucket-bronze/tabela_cliente_landing
    # =========================================================================
    df_bronze_final.write \
        .mode("overwrite") \
        .partitionBy("anomesdia") \
        .parquet(bronze_output_path)
        
    print(f"[SUCESSO] Dados gravados na camada Bronze: {bronze_output_path}")

    # =========================================================================
    # PASSO 5: Deduplique o dataset mantendo sempre somente a última data de 
    #          atualização do cadastro de cada cliente.
    # =========================================================================
    # Agora a ordenação por 'dt_atualizacao' funcionará perfeitamente por ser um tipo Date real
    window_spec = Window.partitionBy("cod_cliente").orderBy(col("dt_atualizacao").desc())
    
    df_deduplicated = df_bronze_final \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # =========================================================================
    # PASSO 6: Trate a coluna de telefone de modo a permitir somente valores 
    #          que sigam o padrão (NN)NNNNN-NNNN os demais devem ficar nulos.
    # =========================================================================
    regex_telefone_padrao = r"^\(\d{2}\)\d{4,5}-\d{4}$"
    
    df_silver_final = df_deduplicated.withColumn(
        "num_telefone_cliente",
        when(col("num_telefone_cliente").rlike(regex_telefone_padrao), col("num_telefone_cliente"))
        .otherwise(None)
    )

    # =========================================================================
    # PASSO 7: Realize a escrita do dado no bucket s3://bucket-silver/tb_cliente
    # =========================================================================
    df_silver_final.write \
        .mode("overwrite") \
        .partitionBy("anomesdia") \
        .parquet(silver_output_path)

    print(f"[SUCESSO] Dados gravados na camada Silver: {silver_output_path}")

if __name__ == "__main__":
    main()