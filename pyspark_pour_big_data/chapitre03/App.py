from pyspark.sql import SparkSession
from data_custom import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName('structured_streaming').getOrCreate()
    employee_dataframe = spark.createDataFrame(
        employees_data).write.csv("csv_folder", mode="append")
    employee_schema = StructType().add("name", "string").add("salary", "string").add(
        "company", "string").add("country", "string").add("grade", "integer")

    employee_data = spark.readStream.option(
        "sep", ",").schema(employee_schema).csv("csv_folder")
    employee_data.printSchema()
    country_count = employee_data.groupBy("grade").count()
    employee_query = (country_count.writeStream.queryName(
        'employee_data_query').outputMode('complete').format('memory').start())
    spark.sql("select * from employee_data_query").toPandas().head(10)
