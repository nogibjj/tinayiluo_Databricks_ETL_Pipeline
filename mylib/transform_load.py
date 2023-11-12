from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/tinayiluo_Databricks_ETL_Pipeline/"
                "airline-safety.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema
    airline_safety_df = spark.read.csv(
        dataset, header=True, inferSchema=True
    )

    columns = airline_safety_df.columns

    # Calculate mid index
    mid_idx = len(columns) // 2

    # Split columns into two halves
    columns1 = columns[:mid_idx]
    columns2 = columns[mid_idx:]

    # Create two new DataFrames
    airline_safety_df1 = airline_safety_df.select(*columns1)
    airline_safety_df2 = airline_safety_df.select(*columns2)

    # add unique IDs to the DataFrames
    airline_safety_df1 = airline_safety_df1.withColumn(
        "id", monotonically_increasing_id()
    )
    airline_safety_df2 = airline_safety_df2.withColumn(
        "id", monotonically_increasing_id()
    )

    # transform into a delta lakes table and store it
    airline_safety_df1.write.format("delta").mode("overwrite").saveAsTable(
        "airline_safety1_delta"
    )
    airline_safety_df2.write.format("delta").mode("overwrite").saveAsTable(
        "airline_safety2_delta"
    )
    
    num_rows = airline_safety_df1.count()
    print(num_rows)
    num_rows = airline_safety_df2.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()
