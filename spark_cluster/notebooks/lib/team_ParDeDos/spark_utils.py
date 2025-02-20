from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

class SparkUtils:
    def __init__(self):
        self.typeDict = {
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

    # [("name", "StringType"),
    #  ("age", "IntegerType"),
    #  ("city", "StringType")]

    @staticmethod
    def generate_schema(self, columns_info) -> StructType:
        # list of tuples
        schema = []
        for k, v in columns_info:
            if v in self.typeDict:
            # StructField("name", StringType(), True),
                schema.append(StructField(f"${k}", type.get(v), True))
        return schema
