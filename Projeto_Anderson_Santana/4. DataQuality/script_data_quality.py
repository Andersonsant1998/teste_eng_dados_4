from pyspark.sql.functions import col, current_date, when, to_date, isnan, length
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Resgata o DataFrame da camada Silver usando o caminho definido
# Nota: Ajuste o formato (parquet/csv) conforme o que você usou para salvar
silver_output_path = "output/bucket-silver/tb_cliente"
df_silver = spark.read.parquet(silver_output_path)

# 2. Identificação de Problemas (Audit Trail)
df_audit = df_silver.withColumn("motivo_falha",
    when(col("cod_cliente").isNull(), "cod_cliente vazio")
    .when(col("nm_cliente").isNull(), "nm_cliente vazio")
    .when(to_date(col("dt_nascimento_cliente").cast("string"), "yyyy-MM-dd").isNull(), "data inválida")
    .when(col("num_telefone_cliente").isNull(), "telefone vazio")
    .when(col("vl_renda").cast("double").isNull(), "renda não é numérico")
    .when(~col("tp_pessoa").isin("PF", "PJ"), "tp_pessoa inválido")
    .otherwise("OK")
)

# 3. Separar o que é lixo do que é limpo
df_limpo = df_audit.filter(col("motivo_falha") == "OK").drop("motivo_falha")
df_ruim = df_audit.filter(col("motivo_falha") != "OK")

# 4. Exibir os problemas encontrados (Opcional: Salvar em um log de erros)
print("Registros com problemas encontrados:")
df_ruim.select("cod_cliente", "motivo_falha").show()

# 5. Salvar resultado limpo para a próxima etapa (Gold)
df_limpo.write.mode("overwrite").parquet(f"{silver_output_path}_final")

print(f"Total limpos: {df_limpo.count()}")
print(f"Total com erros: {df_ruim.count()}")