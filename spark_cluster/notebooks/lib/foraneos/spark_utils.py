from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
    
        TypeDict = {
            "string": StringType(),
            "double": DoubleType(),
            "int": IntegerType(),
            "float": FloatType(),
            "bool": BooleanType(), 
            "data": DateType(),
            "timestamp": TimestampType(),
            "array": ArrayType(),
            "map": MapType(),
            "struct": StructType(), 
            "long": LongType(),
            "short": ShortType()
        }
        

        schema_list = [ StructField((tuple_arg(0), TypeDict[tuple_arg(1)], True)) for tuple_arg in columns_info ]
    
    
        return StructType(schema_list)