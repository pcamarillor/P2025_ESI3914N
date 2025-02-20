from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType

class SparkUtils:
    
    
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        '''
        method to create a correct spark dataframe schema taking a list of tuples of names and datatypes
        possible datatypes: string, double, int, float, long, bool, date, timestamp
                            array, map, struct, short
        
        Args:
            columns_info (list):             contains tuples like ("name","sting")
        Returns:
            StructType (object):             Spark dataframe StructType for given input 
        '''
    
    
        # defining dict to map simplified field types passed by user to actual spark types
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
        

        #create list of different columns in dataframe
        schema_list = [ StructField(  tuple_arg[0], type_dict[tuple_arg[1]], True  ) for tuple_arg in columns_info ]
    
    
        return StructType(schema_list)