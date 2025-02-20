from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
    
        type_dict = {
            "StringType": StringType(),
            "DoubleType": DoubleType(),
            "IntegerType": IntegerType(),
            "FloatType": FloatType(),
            "BooleanType": BooleanType(),
            "DateType": DateType(),
            "TimestampType": TimestampType(),
            "ArrayType": lambda element_type: ArrayType(element_type),
            "MapType": lambda key_type, value_type: MapType(key_type, value_type),
            "StructType": lambda fields: StructType(fields),
            "LongType": LongType(),
            "ShortType": ShortType()
        }
        

        schema_list = [ StructField(  tuple_arg[0], type_dict[tuple_arg[1]], True  ) for tuple_arg in columns_info ]
    
    
        return StructType(schema_list)