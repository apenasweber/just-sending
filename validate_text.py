from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lower, when

# Configura a sessão do Spark
spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

# Criação do dataframe de exemplo
data = [("John", "95.789%"), ("Alice", "42.5%"), ("Bob", None), ("Bob", "abc")]
df = spark.createDataFrame(data, ["Name", "Percentage"])

# Definição da função validate_text
def validate_text(value, initial_validation: int = 1) -> int:
    """Valida se o valor é um texto"""
    if initial_validation == 0 or value is None:
        return 0

    stripped = lower(value)
    return when((stripped != "none") & (stripped != "null") & (length(value) >= 1), 1).otherwise(0)

# Aplica a função validate_text utilizando funções nativas do PySpark
df = df.withColumn("ValidatedText", validate_text(col("Name")))
df.show()

def test_validate_text_should_return_1_for_non_empty_string():
    value = "Hello"
    assert validate_text(value) == 1

def test_validate_text_should_return_0_for_empty_string():
    value = ""
    assert validate_text(value) == 0

def test_validate_text_should_return_0_for_none():
    value = None
    assert validate_text(value) == 0

def test_validate_text_should_return_0_for_none_string():
    value = "None"
    assert validate_text(value) == 0

def test_validate_text_should_return_0_for_null_string():
    value = "Null"
    assert validate_text(value) == 0

def test_validate_text_should_return_0_for_whitespace_string():
    value = "   "
    assert validate_text(value) == 0
