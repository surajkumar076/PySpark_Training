from pyspark import *
from pyspark import SparkConf, SparkContext
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# from utility import *


spark = SparkSession.builder.master('local[3]').appName('assignment_1').getOrCreate()

Schema = StructType([StructField("Username", StringType(), False),
                     StructField(" Identifier", IntegerType(), True),
                     StructField("One-time password", StringType(), False),
                     StructField("Recovery code", StringType(), True),
                     StructField("First name", StringType(), True),
                     StructField("Last name", StringType(), True),
                     StructField("Department", StringType(), True),
                     StructField("Location", StringType(), True)])

df = spark.read.format("csv").schema(Schema).option("header", True).option("Delimiter", ";"). \
    load("data/username-password-recovery-code.csv")
df.printSchema()
df.show(20, False)

# df.write.csv("data/username_new.csv")

# df.write.parquet("data/user_info.parquet") #.mode("overwrite")
# df.write.mode('append').parquet(r"C:\Users\suraj\PycharmProjects\Assignment-2\Assignment_2\data\user_info.parquet")

df.write.mode('overwrite').csv("data/user_csv")

df.write.mode('overwrite').parquet("data/user_parquet")
print('Reading Parquet file')
df_parquet = spark.read.parquet("data/user_parquet")
df_parquet.show(20, False)
