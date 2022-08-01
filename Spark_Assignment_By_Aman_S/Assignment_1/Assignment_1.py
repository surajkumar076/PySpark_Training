"""
Assignment

Create 2 data frame and and join them.

Create a dataframe and replace the null values of a column with some meaningful thing using withcolumn.

Note: Please use below csv to create data frame.

CSV FILES - Employee_info.csv & Employee_info_1.csv
"""

from pyspark.sql import *
from pyspark import *
from pyspark.sql.functions import regexp_replace

#starting the session or driver
spark = SparkSession.builder.master('local[3]').appName('assignments').getOrCreate()


# Read Employee_info.csv file
df_Employee_info = spark.read.option('inferSchema' , 'True').\
    option('header', 'True').csv('data\Employee_info.csv')
df_Employee_info.show(20, False) #Print(1st dataframe)

# Read Employee_info_1.csv file
df_Employee_info_1= spark.read.option('inferSchema' , 'True').\
    option('header', 'True').csv('data\Employee_info_1.csv')
df_Employee_info_1.show(20, False) #Print(2nd dataframe)

# Join two DataFrames:-
df_join= df_Employee_info.join(df_Employee_info_1,df_Employee_info.Id == df_Employee_info_1.Id, "Full")
df_join.show(20, False)

#Replacing 'NUll' with meaningfull
Remove_Null_Values = df_join.withColumn('Department',regexp_replace('Department', 'Null', 'Data Science'))\
  .withColumn('City', regexp_replace('City', 'Null', 'Mumbai'))\
  .show(truncate=False)

