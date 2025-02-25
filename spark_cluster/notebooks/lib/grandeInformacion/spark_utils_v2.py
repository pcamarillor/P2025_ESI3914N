from pyspark.sql.types import (StructType, StructField, ShortType, StringType, 
                               IntegerType, FloatType, BooleanType, LongType, MapType,
                               DoubleType, DateType, TimestampType, BinaryType, ArrayType)

class SparkUtils:

    types_schema = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "BooleanType": BooleanType(),
        "LongType": LongType(),
        "ShortType": ShortType(),
        "DoubleType": DoubleType(),
        "FloatType": FloatType(),
        "DateType": DateType(),
        "TimestampType": TimestampType(),
        "BinaryType": BinaryType(),
        "StructType": StructType()
    }

    # (name, value)
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        
        for nom_columna, tipo_columna in columns_info:
            schema = StructType([
                StructField(nom_columna, SparkUtils.types_schema[tipo_columna], True)
            ])
            return schema

        raise NotImplementedError("Not implemented yet")