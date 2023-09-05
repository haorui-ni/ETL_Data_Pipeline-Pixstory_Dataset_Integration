from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when


spark = SparkSession \
        .builder \
         .master("local[*]") \
        .config("spark.executor.memory", "70g") \
        .config("spark.driver.memory", "50g") \
        .config("spark.memory.offHeap.enabled",True) \
        .config("spark.memory.offHeap.size","16g") \
        .appName("load_hs") \
        .config("spark.jars", "spark-snowflake_2.12-2.11.0-spark_3.3.jar, snowflake-jdbc-3.13.22.jar") \
        .getOrCreate()
'''spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.region", "us-east-1")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA2J6KBGMPYV6EDB3L")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")'''

#.config("spark.jars", "spark-snowflake_2.12-2.11.0-spark_3.3.jar, snowflake-jdbc-3.13.22.jar") \

def main():
    hs_location="hate_speech.csv"
    hs_schema = StructType([
        StructField("glaad_symbol", StringType(), True),
        StructField("adl_symbol", StringType(), True)])
    
    df_hs = spark.read.option("header", "true").csv(hs_location,schema = hs_schema)
    df_hs.cache()

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

    df_hs.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "hate_speech") \
        .mode("overwrite") \
        .save()
    

if __name__ == "__main__":
    main()