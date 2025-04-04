from pyspark.sql import SparkSession

def load_data(file_path: str, data_schema) -> None:
    spark = SparkSession.builder.appName("DataLoader").getOrCreate()

    df = spark.read.csv(file_path, schema=data_schema, sep="\t", header=True)
    # df.show(5)
    return df
