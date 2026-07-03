import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

silver_output_path = "output/bucket-silver/tb_cliente"

# --- 1. LÓGICA DE VALIDAÇÃO (O "CÉREBRO" DO JOB) ---
def validar_cliente(row):
    """
    Função de validação que será executada linha a linha pelo Spark.
    
    EXPLICAÇÃO TÉCNICA:
    Quando o Spark envia os dados para uma UDF, ele passa um objeto do tipo 'Row'.
    O 'Row' não é um dicionário, por isso não aceita o método .get().
    Usamos 'asDict()' para converter o Row em um dicionário Python padrão,
    tornando o código compatível tanto com o Spark quanto com testes unitários.
    """
    # Converte o objeto Row do Spark para um dicionário Python puro
    row_dict = row.asDict() if hasattr(row, 'asDict') else row
    
    # Validação de presença obrigatória:
    # .get() é seguro porque não quebra se a coluna não existir, retorna None
    if not row_dict.get('cod_cliente'): return "cod_cliente vazio"
    if not row_dict.get('nm_cliente'): return "nm_cliente vazio"
    
    # Validação de Telefone:
    # 1. re.sub remove tudo que não for dígito (limpeza de máscara como "(11)-")
    # 2. length valida se o número resultante tem entre 10 e 11 caracteres (padrão Brasil)
    tel = re.sub(r'[^0-9]', '', str(row_dict.get('num_telefone_cliente', '')))
    if len(tel) < 10 or len(tel) > 11:
        return "telefone fora de padrão"
        
    # Validação de Renda:
    # Tentamos converter para float. Se falhar, é erro de tipo (não numérico).
    # Se for menor que 0, é erro de negócio.
    try:
        if float(row_dict.get('vl_renda', 0)) < 0: return "renda negativa"
    except (TypeError, ValueError):
        return "renda não numérica"
        
    # Validação de Domínio (Lista permitida):
    if row_dict.get('tp_pessoa') not in ['PF', 'PJ']:
        return "tp_pessoa inválido"
        
    # Se passar por todos os 'if', o dado está íntegro!
    return "OK"

# --- 2. EXECUÇÃO NO SPARK ---

# Transforma a função Python em uma UDF (User Defined Function) do Spark
# O tipo de retorno é String (o status "OK" ou o motivo do erro)
validador_udf = F.udf(validar_cliente, StringType())

# Leitura do path definido (assumindo que o spark está instanciado no seu ambiente)
df_silver = spark.read.parquet(silver_output_path)

# A MÁGICA DA UDF:
# F.struct transforma o DataFrame em uma coleção de colunas tratadas como uma "linha".
# A UDF recebe essa linha, executa a lógica de negócio e retorna o status.
cols_to_validate = [F.col(c) for c in df_silver.columns]
df_audit = df_silver.withColumn("motivo_falha", validador_udf(F.struct(cols_to_validate)))

# Segregação final:
# Onde 'motivo_falha' for 'OK', o dado é confiável (Gold).
# Onde for diferente de 'OK', o dado é lixo (Quarentena) e deve ser investigado.
df_limpo = df_audit.filter(F.col("motivo_falha") == "OK").drop("motivo_falha")
df_ruim = df_audit.filter(F.col("motivo_falha") != "OK")

# Persistência:
# Salva os dois DataFrames separadamente. Isso garante que a Gold nunca consuma sujeira.
df_limpo.write.mode("overwrite").parquet(f"{silver_output_path}_final")
df_ruim.write.mode("overwrite").parquet(f"{silver_output_path}_quarentena")

print(f"Validação concluída!")
print(f"Dados íntegros salvos em: {silver_output_path}_final")
print(f"Registros com problemas em: {silver_output_path}_quarentena")