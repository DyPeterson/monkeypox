from pyspark.sql import SparkSession
from datetime import date
import quinn
from pyspark.sql.functions import lit

def dashes_to_underscores(s):
    return s.replace("-", "_")

#create spark session
spark = SparkSession.builder.getOrCreate()
#create df
daily_df = spark.read.csv('Daily_Country_Wise_Confirmed_Cases.csv', header=True)
#perform transformations
dataset_df = daily_df.transform(quinn.with_columns_renamed(dashes_to_underscores))
#create pandas df and set index
pandas_df = daily_df.toPandas()
pandas_df.set_index('Country', inplace=True)
#transpose and reset index
transpose_df = pandas_df.transpose()
transpose_df.reset_index(inplace=True)
#create spark df 
spark_df=spark.createDataFrame(transpose_df)
spark_df = spark_df.withColumnRenamed('index', 'Date')

spark_df.write.csv(f"Daily_CC_{date.today()}")