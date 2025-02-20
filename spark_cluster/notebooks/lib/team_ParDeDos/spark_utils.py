from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

class SparkUtils:
    # [("name", "StringType"),
    #  ("age", "IntegerType"),
    #  ("city", "StringType")]
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        typeDict = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "LongType": LongType(),
            "ShortType": ShortType(),
            "DoubleType": DoubleType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "TimestampType": TimestampType(),
            "BinaryType": BinaryType(),
            "ArrayType": ArrayType(StringType()),  # Se requiere un tipo de elemento
            "MapType": MapType(StringType(), StringType()),  # Se requieren clave y valor
            "DateType": DateType(),
        }

        # list of tuples
        struct = []
        for k, v in columns_info:
            if v in typeDict:
            # StructField("name", StringType(), True),
                struct.append(StructField(f"${k}", typeDict.get(v), True))

        return StructType(struct)
