from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, ArrayType, MapType
from datetime import date, datetime

class SparkUtils:

    @staticmethod
    def clean_df(df, schema):
        """_summary_

        Args:
            df (_type_): _description_

        Returns:
            _type_: _description_
        """

        """
        ("show_id", "string"),
        ("type", "string"),
        ("title", "string"),
        ("director", "string"),
        ("country", "string"),
        ("rating", "string"),
        ("duration", "string"),
        ("listed_in", "string"),
        ("release_year", "integer"),
        ("date_added", "date")
        """
        defaults = {
            "string": "Unkown",
            "integer": 0,
            "long": 0,
            "short": 0,
            "double": 0.0,
            "float": 0.0,
            "boolean": True,
            "date": date.today(),
            "timestamp": datetime.now(),
            "binary": b""
        }
        # Extraer nombre y tipo del esquema
        column_defaults = {field.name: defaults.get(field.dataType.typeName(), None) for field in schema.fields}

        # Reemplazar valores nulos en todas las columnas
        temp_df = df.fillna(column_defaults)

        return temp_df

        """for k, v in schema:
            if v not in defaults:
                raise TypeError(f"{v} is not a valid type.")
            temp_df = df.fill(value=defaults.get(v),subset=[f"{k}"])
        return temp_df"""

    @staticmethod
    def write_df(df):
        """_summary_

        Args:
            df (_type_): _description_

        Returns:
            _type_: _description_
        """
        return 0

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
