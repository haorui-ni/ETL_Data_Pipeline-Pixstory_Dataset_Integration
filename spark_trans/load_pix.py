import sys


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when
from pyspark.sql import SQLContext



def main():

    spark = SparkSession \
            .builder \
            .master("local[*]") \
            .config("spark.executor.memory", "70g") \
            .config("spark.driver.memory", "50g") \
            .config("spark.memory.offHeap.enabled",True) \
            .config("spark.memory.offHeap.size","16g") \
            .appName("load_pix_flag") \
            .config("spark.jars", "spark-snowflake_2.12-2.11.0-spark_3.3.jar, snowflake-jdbc-3.13.22.jar") \
            .getOrCreate()

    # preprocess for pixstory
    pix_location="pixstory.csv"

    pix_schema = StructType([
        StructField("pk_id", IntegerType(), True),
        StructField("story_primary_id", IntegerType(), True),
        StructField("story_id", StringType(), True),
        StructField("user_prime_id", IntegerType(), True),
        StructField("user_id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("narrative", StringType(), True),
        StructField("media", StringType(), True),
        StructField("account_created_date", StringType(), True),
        StructField("interest", StringType(), True)
        ])

    df_pix = spark.read.option("header", "true").csv(pix_location, schema = pix_schema)
    df_pix = df_pix.dropna(subset=['narrative'])
    df_pix = df_pix.dropna(subset=['title'])
    df_pix.cache()

    sfOptions = {
        "sfURL": "https://duxpsxv-zlb27815.snowflakecomputing.com",
        "sfDatabase": "pixstory_data",
        "sfWarehouse": "pix",
        "sfSchema": "pixstory",
        "sfRole": "ACCOUNTADMIN",
        "sfUser": "HAORUINI",
        "sfPassword": "NiHaoRui@55af5587f"
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    df_pix.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "pixstory") \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":
    main()
