import sys


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when, broadcast


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.memory.offHeap.enabled",True) \
        .config("spark.memory.offHeap.size","8g") \
        .appName("load_pix_flag") \
        .config("spark.jars", "spark-snowflake_2.12-2.11.0-spark_3.3.jar, snowflake-jdbc-3.13.22.jar") \
        .getOrCreate()
        
        


def main():
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

    # preprocess for hate_speech

    hs_location="hate_speech.csv"
    hs_schema = StructType([
        StructField("glaad_symbol", StringType(), True),
        StructField("adl_symbol", StringType(), True)])
    
    df_hs = spark.read.option("header", "true").csv(hs_location, schema = hs_schema)
    df_hs.cache()

   
    # detect flag for narrative
    glaad_label = df_hs.select("glaad_symbol")
    adl_label = df_hs.select("adl_symbol")
    b_glaad_label = broadcast(glaad_label)
    b_adl_label = broadcast(adl_label)

    sub_df_pix = df_pix.select("pk_id", "narrative")
    sub_df_pix.cache()

    # Join and detect phrases
    glaad_flag_df = sub_df_pix.crossJoin(b_glaad_label) \
        .withColumn("glaad_flag", when(col("narrative").contains(col("glaad_symbol")), lit(True)).otherwise(lit(False))) \
        .select("pk_id", "narrative", "glaad_flag")
    glaad_flag_df.cache()
    

    adl_flag_df = glaad_flag_df.crossJoin(b_adl_label) \
        .withColumn("adl_flag", when(col("narrative").contains(col("adl_symbol")), lit(True)).otherwise(lit(False))) \
        .select("pk_id", "glaad_flag", "adl_flag")
    adl_flag_df.cache()

   # precess and write result to file
    flag_df = adl_flag_df.dropDuplicates(["pk_id"]).orderBy("pk_id")
    flag_df = flag_df.dropna(subset=["pk_id"])

    flag_df.cache()

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
    flag_df = flag_df.coalesce(10) 

    flag_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "flag") \
        .mode("overwrite") \
        .save()
    #flag_df.write.csv("flag.csv", header=True, mode="overwrite")

    #ms.coalesce(1).write.format("parquet").mode('overwrite').save("s3://irisseta/output_folder/")

if __name__ == "__main__":
    main()