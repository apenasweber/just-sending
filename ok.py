from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, round, lit, when
from typing import Union
from pyspark.sql.column import Column

def treatment_percentage_without_division(column: Union[Column, str]) -> Column:
    """Trata um valor percentual, removendo caracteres não numéricos e substituindo vírgula por ponto."""
    column = col(column) if isinstance(column, str) else column

    # Remove caracteres não numéricos e substitui vírgula por ponto
    cleaned_column = regexp_replace(column, "[^0-9,.]", "")
    cleaned_column = regexp_replace(cleaned_column, ",", ".")

    # Converte para float e arredonda para 4 casas decimais, ou mantém o valor original se não for possível converter
    output_column = when(cleaned_column.rlike(r"^\d*\.?\d+$"), round(cleaned_column.cast("float"), 4)).otherwise(column)

    return output_column

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, round, when

def treatment_percentage_with_division(value):
    """Trata um valor percentual, removendo caracteres não numéricos, substituindo vírgula por ponto e dividindo por 100."""
    cleaned_value = regexp_replace(value, r"[^0-9,.-]", "")
    cleaned_value = regexp_replace(cleaned_value, ",", ".")
    
    processed_value = when(cleaned_value.cast("float").isNotNull(), round(cleaned_value.cast("float") / 100, 4)).otherwise(value)
    return processed_value