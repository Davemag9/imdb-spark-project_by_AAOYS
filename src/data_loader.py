from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def load_data(file_path: str, data_schema: StructType, delimiter: str = "\t"):
    spark = SparkSession.builder.appName("DataLoader").getOrCreate()

    df = spark.read.csv(file_path, schema=data_schema, sep=delimiter, header=True)
    # df.show(5)
    return df
