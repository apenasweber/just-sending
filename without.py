from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, round as round_spark, lit, when
from typing import Union
from pyspark.sql.column import Column

def treatment_percentage_without_division(column: Union[Column, str]) -> Column:
    """Trata um valor percentual, removendo caracteres não numéricos e substituindo vírgula por ponto."""
    column = col(column) if isinstance(column, str) else column

    # Remove caracteres não numéricos e substitui vírgula por ponto
    cleaned_column = regexp_replace(column, "[^0-9,.]", "")
    cleaned_column = regexp_replace(cleaned_column, ",", ".")

    # Converte para float e arredonda para 4 casas decimais, ou mantém o valor original se não for possível converter
    output_column = when(cleaned_column.rlike(r"^\d*\.?\d+$"), round_spark(cleaned_column.cast("float"), 4)).otherwise(column)

    return output_column

# Exemplo de uso
spark = SparkSession.builder \
    .appName("example_app") \
    .getOrCreate()

data = [
    ("John", "95.789 %"),
    ("Alice", "-95.789 %"),
    ("Bob", "95.789"),
    ("Bob", "-95.789 %"),
    ("Eve", "5,789 %"),
    ("Mallory", "-5,789 %"),
    ("Alex", "95.789%"),
    ("Charlie", "100%"),
    ("Dave", "0 %"),
    ("Frank", "1"),
    ("Grace", "0.123"),
    ("Heidi", "0,123"),
    ("Ivy", "1000")
]


# Cria o DataFrame
df = spark.createDataFrame(data, ["Name", "Percentage"])

# Aplica a função treatment_percentage_without_division
df = df.withColumn("CleanedPercentage", treatment_percentage_without_division("Percentage")) \
    .fillna("N/A", subset=["CleanedPercentage"])

# Exibe o DataFrame modificado
print("DataFrame Modificado:")
df.show()
