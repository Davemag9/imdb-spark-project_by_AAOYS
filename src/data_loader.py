from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import tempfile

def load_data(file_path: str, data_schema: StructType, delimiter: str = "\t"):
    spark = SparkSession.builder.appName("DataLoader").getOrCreate()

    df = spark.read.csv(file_path, schema=data_schema, sep=delimiter, header=True)
    return df


def write_data_to_csv(df, path: str):
    df.write \
      .option("header", "true") \
      .mode("overwrite") \
      .csv(path)

    print("Data written to CSV at:", path)


def clear_all_cache(spark):
    spark.catalog.clearCache()

    # Set a valid temporary checkpoint directory
    temp_dir = tempfile.mkdtemp()
    spark.sparkContext.setCheckpointDir(temp_dir)

    for rdd in spark.sparkContext._jsc.getPersistentRDDs().values():
        rdd.unpersist(True)

    import gc
    gc.collect()


