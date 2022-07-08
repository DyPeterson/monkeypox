from pyspark.sql import SparkSession
from datetime import date
import quinn
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

def add_row(daily_df, rows):
    """
    Add rows to dataset but no new columns
    """
    #grab number of new rows
    daily_df = daily_df.select('Country',daily_df[-rows])
    #create pandas df and set index
    pandas_df = daily_df.toPandas()
    pandas_df.set_index('Country', inplace=True)
    #transpose and reset index
    transpose_df = pandas_df.transpose()
    transpose_df.reset_index(inplace=True)
    #create spark df 
    spark_df=spark.createDataFrame(transpose_df)
    spark_df = spark_df.withColumnRenamed('index', 'Date')
    #write to csv? or update?
    spark_df.write.csv(f"Daily_CC_{date.today()}", header=True)

def add_column(daily_df, rows):
    """
    Add rows and columns to dataset
    """
    #grab number of new rows
    daily_df = daily_df.select('Country',daily_df[rows])
    #create pandas df and set index
    pandas_df = daily_df.toPandas()
    pandas_df.set_index('Country', inplace=True)
    #transpose and reset index
    transpose_df = pandas_df.transpose()
    transpose_df.reset_index(inplace=True)
    #create spark df 
    spark_df=spark.createDataFrame(transpose_df)
    spark_df = spark_df.withColumnRenamed('index', 'Date')
    #pull dataset down from GCS??
    df = df
    for column in [column for column in spark_df.columns if column not in df.columns]:
        df = df.withColumn(column, lit(0))
    df.union(spark_df)
    #write to csv? overwrite table?


def update(shape_xcom):
    """
    Main function to create df of new data
    """
    spark = SparkSession.builder.getOrCreate()
    daily_df = spark.read.csv('Daily_Country_Wise_Confirmed_Cases.csv', header=True)
    daily_shape = (len(daily_df.columns), daily_df.count())
    if daily_shape == shape_xcom:
        pass
    elif daily_shape[0] == shape_xcom[1]:
        rows = daily_df.count() - shape_xcom[0]
        add_row(daily_df, rows)
    else:
        rows = daily_df.count() - shape_xcom[0]
        add_column(daily_df, rows)

update((53, 56))