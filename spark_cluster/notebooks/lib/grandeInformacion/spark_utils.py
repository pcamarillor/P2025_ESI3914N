from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import (StructType, ShortType, StringType, 
                               IntegerType, FloatType, BooleanType, LongType, MapType,
                               DoubleType, DateType, TimestampType, BinaryType, ArrayType)


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
    @staticmethod
    def generate_schema(columns_info) -> StructType:
        list = []
        str_shido = ''
        for i in columns_info:
            if i == len(columns_info):
                str_shido = str_shido + f"StructField({i[0],{},True})".format(types_schema[i[1]])
            else:
                str_shido = str_shido + f"StructField({i[0],{},True}), ".format(types_schema[i[1]])
            #list.append(StructField(i[0],i[1]))

        struct_type = "StructType([{}])".format(str_shido)
        print(struct_type)
        result = eval(struct_type)
        return result

        #raise NotImplementedError("Not implemented yet")