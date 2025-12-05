import pandas as pd
import numpy as np
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType # Use Double, not Float
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, split, to_timestamp, coalesce, 
                                   regexp_replace, trim, regexp_extract, length, 
                                   substring, lit)
import os
import findspark
findspark.init()

# --- ADD THIS BLOCK ---
# Allow the Security Manager for Java 21+
os.environ["JDK_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
# ----------------------

spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext

# Initialize Spark
spark = SparkSession.builder.appName("Traffy").getOrCreate()

#Turn off ANSI mode to allow silent null coercion
spark.conf.set("spark.sql.ansi.enabled", "false")

# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------
df_traffy = spark.read.csv("../data/bangkok_traffy.csv", header=True, inferSchema=False)
df_geo = spark.read.csv("../thailand_geography.csv", header=True, inferSchema=True)

# ... (Load data code remains the same) ...

# ---------------------------------------------------------
# STEP 1: IMPROVED COORDINATE IMPUTATION
# ---------------------------------------------------------

# Clean Geo Data first
df_geo_clean = df_geo.select(
    trim(col("district")).alias("geo_district"), 
    col("latitude").alias("geo_lat"),       
    col("longitude").alias("geo_long")      
)

# Parse Traffy Data
# We replace square brackets just in case the format is [10.1, 20.2]
df_parsed = df_traffy.withColumn("coords_clean", regexp_replace(col("coords"), r"[\[\]]", "")) \
                     .withColumn("lat_str", split(col("coords_clean"), ",").getItem(0)) \
                     .withColumn("long_str", split(col("coords_clean"), ",").getItem(1)) \
                     .withColumn("raw_lat", regexp_extract(col("lat_str"), r"([0-9]+\.?[0-9]*)", 1).cast(DoubleType())) \
                     .withColumn("raw_long", regexp_extract(col("long_str"), r"([0-9]+\.?[0-9]*)", 1).cast(DoubleType()))

# Debug: Print schema to ensure types are correct
df_parsed.printSchema()

# Join
# Note: Ensure both sides are trimmed during the join condition
df_joined = df_parsed.join(df_geo_clean, 
                           trim(df_parsed.district) == df_geo_clean.geo_district, 
                           "left")

df_loc_fixed = df_joined.withColumn("final_lat", coalesce(col("raw_lat"), col("geo_lat"))) \
                        .withColumn("final_long", coalesce(col("raw_long"), col("geo_long")))

# Check how many Nulls exist now
print("Rows with NULL latitude:", df_loc_fixed.filter(col("final_lat").isNull()).count())
# ---------------------------------------------------------
# STEP 2: TEXT CLEANING
# ---------------------------------------------------------
df_clean_text = df_loc_fixed.withColumn("clean_comment", 
    regexp_replace(col("comment"), r"[\n\r\t]", " ")) \
    .withColumn("clean_comment", regexp_replace(col("clean_comment"), r"\s+", " ")) \
    .withColumn("clean_comment", trim(col("clean_comment")))

# Filter out short comments
df_ready = df_clean_text.filter(length(col("clean_comment")) > 3)

# ---------------------------------------------------------
# STEP 3: TEMPORAL FEATURES
# ---------------------------------------------------------
df_final = df_ready.withColumn("timestamp_dt", 
                               to_timestamp(substring(col("timestamp"), 1, 19), "yyyy-MM-dd HH:mm:ss")) \
                   .withColumn("last_activity_dt", 
                               to_timestamp(substring(col("last_activity"), 1, 19), "yyyy-MM-dd HH:mm:ss"))

# ---------------------------------------------------------
# STEP 4: FINAL EXPORT
# ---------------------------------------------------------
output_df = df_final.select(
    "ticket_id",
    "type", 
    "clean_comment",        
    "final_lat",          
    "final_long",           
    "district",             
    "timestamp_dt",
    "state",
    "star"
)

# Export
print("Exporting data...")
output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("traffy_data_powerBI")
print("Process Complete! Files saved to 'traffy_data_powerBI'")