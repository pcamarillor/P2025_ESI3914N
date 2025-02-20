from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType

class SparkUtils:
    @staticmethod
    def generate_schema(columns_info) -> StructType:
    
        type_dict = {
                "string": StringType(),
                "double": DoubleType(),
                "int": IntegerType(),
                "float": FloatType(),
                "bool": BooleanType(), 
                "date": DateType(),
                "timestamp": TimestampType(),
                "array": lambda element_type: ArrayType(element_type),
                "map": lambda key_type, value_type: MapType(key_type, value_type),
                "struct": lambda fields: StructType(fields),
                "long": LongType(),
                "short": ShortType()
            }
        

        schema_list = [ StructField(  tuple_arg[0], type_dict[tuple_arg[1]], True  ) for tuple_arg in columns_info ]
    
    
        return StructType(schema_list)