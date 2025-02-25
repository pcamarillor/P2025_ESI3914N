from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """_summary_

        Args:
            columns_info (array): An array that contains tuples (cols_name, cols_type)

        Raises:
            TypeError: Show and error when we receive a not valid type

        Returns:
            StructType: Array of StructFields
        """
        typeDict = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
        }
        schema = []
        for k, v in columns_info:
            if v not in typeDict:
                raise TypeError(f"{v} is not a valid type.")
            schema.append(StructField(f"{k}", typeDict.get(v), True))
        return StructType(schema)
