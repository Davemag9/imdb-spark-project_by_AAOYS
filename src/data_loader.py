from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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


def show_data_summary(df, df_name="DataFrame", extended_stats=False):
    print(f"\nSummary for {df_name}:")
    df.show(20, truncate=False)  

    print("Number of Rows:", df.count())  
    print("Number of Columns:", len(df.columns))  

    if extended_stats:
        print("Columns:")
        print(df.columns) 

        print("\nSchema:")
        df.printSchema()  

        numeric_columns = [col for col, dtype in df.dtypes if dtype in ('int', 'double', 'float')]
        if numeric_columns:
            print(f"\nBasic statistics for numeric columns in {df_name}:")
            stats = df.select(numeric_columns).describe().toPandas()
            print(stats)
        else:
            print("No numeric columns found in the DataFrame.")

        missing_values = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
        print("\nMissing values per column:")
        missing_values.show(truncate=False)
        
        categorical_columns = [col for col, dtype in df.dtypes if dtype == 'string']
        if categorical_columns:
            unique_values_list = []
            for col in categorical_columns:
                unique_count = df.select(col).distinct().count()
                unique_values_list.append((col, unique_count))
            print("\nUnique Values Count for Categorical Columns:")
            for col, unique_count in unique_values_list:
                print(f"{col} - {unique_count}")


def clear_all_cache(spark):
    spark.catalog.clearCache()

    # Set a valid temporary checkpoint directory
    temp_dir = tempfile.mkdtemp()
    spark.sparkContext.setCheckpointDir(temp_dir)

    for rdd in spark.sparkContext._jsc.getPersistentRDDs().values():
        rdd.unpersist(True)

    import gc
    gc.collect()


