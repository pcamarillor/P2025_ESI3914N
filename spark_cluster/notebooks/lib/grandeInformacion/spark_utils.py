from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import (StructType, ShortType, StringType, 
                               IntegerType, FloatType, BooleanType, LongType, MapType,
                               DoubleType, DateType, TimestampType, BinaryType, ArrayType)
from time import time, sleep
from random import randint

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

class SparkUtils:
    # @staticmethod
    # def generate_schema(columns_info) -> StructType:
    #     counter = 0
    #     str_shido = ''
    #     for i in columns_info:
    #         if counter == (len(columns_info) - 1):
    #             str_shido = str_shido + f"StructField{i[0],{},True}".format(types_schema[i[1]])
    #         else:
    #             str_shido = str_shido + f"StructField{i[0],{},True}, ".format(types_schema[i[1]])
    #         counter = counter + 1
                
    #     struct_type = "StructType([{}])".format(str_shido)
    #     result = eval(struct_type)
        
    #     print(type(result))
    #     return result

    #     #raise NotImplementedError("Not implemented yet")

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Generates a list of StructField objects from a list of tuples.

        Args:
            column_info (list of tuples): Each tuple contains (column_name, data_type_string).

        Returns:
            list: A list of StructField objects.
        """
        # Mapping from string type names to PySpark data types
        type_mapping = {
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

        struct_fields = []
        for column_info in columns_info:
            if column_info[1] not in type_mapping:
                raise ValueError(f"Unsupported data type: {column_info[1]}")

            # Create a StructField for the column
            struct_field = StructField(column_info[0], type_mapping[column_info[1]], True)
            struct_fields.append(struct_field)

        return StructType(struct_fields)

    @staticmethod
    def clean_df(df):
        return df.dropna()
    
    @staticmethod
    def write_df(df):
        df.write \
            .mode("overwrite") \
            .partitionBy("release_year") \
            .parquet("/home/jovyan/notebooks/data/netflix_output/")
    
    @staticmethod
    def logs(path):
        logs = [' | WARN | Disk usage 85% | server-node-1', '  | ERROR | 500 Internal Server Error | server-node-2', ' | INFO | User login successful | server-node-1']
        for i in range(0, 3):
            f = open(f"{path}/log-{time()}", 'x')
            f.write(str(time()) + logs[randint(0, 2)])
            f.close()    
            sleep(3)