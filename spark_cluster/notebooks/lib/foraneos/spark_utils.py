from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, FloatType, BooleanType, ShortType,LongType, MapType, ArrayType, TimestampType ,DateType
from pyspark.sql import DataFrame

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
        schema_list = [ StructField(  tuple_arg[0], type_dict[tuple_arg[1]], nullable =tuple_arg[2]  ) for tuple_arg in columns_info ]
    
    
        return StructType(schema_list)
    
    
    
    @staticmethod
    def clean_df(df, correction_dict):
        '''
        Method to clean Null values from a df
        
        Args:
            df (DataFrame):         The PySpark DataFrame to clean     
            correction_dict (dict): Dictionary containing the relevant information as 
                                    - key = ColumnName
                                    - value = ReplaceValue
                                    
        Returns:
            corrected DataFrame   
        '''
        return df.fillna(correction_dict)
    


    @staticmethod
    def write_df(config: dict) -> None:
        '''
        Method to write a PySpark DataFrame to a Parquet file, partitioned by specified criteria.
        
        Possible partitioning criteria: Any column(s) present in the DataFrame.

        Args:
            config (dict): A dictionary containing the following keys:
                - dataframe (DataFrame): The PySpark DataFrame to write.
                - path (str): The file path where the Parquet file will be saved.
                - mode (str): The write mode ('overwrite', 'append', 'ignore', 'error').
                - criteria (list): A list of column names to partition the data by.
        
        Returns:
            None
        '''
        
        dataframe: DataFrame = config.get("dataframe")
        path: str = config.get("path")
        mode: str = config.get("mode", "overwrite")
        criteria: list = config.get("criteria", [])
        
        # Starts the write method
        # Replaces existing files
        # Partitions the data based on the specified criteria. 
        # Defines the file path.
        dataframe.write \
            .mode(mode) \
            .partitionBy(criteria) \
            .parquet(path)

