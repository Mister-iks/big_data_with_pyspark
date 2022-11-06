from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from airflow.decorators import task

@task
def chapitre3():
    spark = SparkSession.builder.appName('structured_streaming').getOrCreate()
    employees_data = [
        (
            'Samson Maynard',
            '2416',
            'Luctus Ltd',
            'India',
            1
        ),
        (
            'Christine Dotson',
            '4309',
            'Aliquam Ornare Corporation',
            'Turkey',
            1
        ),
        (
            'Matthew Phelps',
            '2419',
            'Ut Inc.',
            'Ukraine',
            1
        ),
        (
            'Reagan Langley',
            '3455',
            'Auctor PC',
            'Spain',
            1,
        ),
        (
            'Piper Love',
            '4891',
            'Nibh Consulting',
            'Russian Federation',
            3
        ),
        (
            'Orlando Mckenzie',
            '3848',
            'Sagittis Augue Incorporated',
            'Australia',
            3
        ),
        (
            'Byron Trevino',
            '2339',
            'Vitae Nibh Corp.',
            'Chile',
            3
        ),
        (
            'Harper Ramsey',
            '2973',
            'Erat Vitae Corp.',
            'Poland',
            2
        ),
        (
            'John Riddle',
            '2013',
            'Nibh Dolor Institute',
            'India',
            2
        ),
        (
            'Xanthus Schmidt',
            '3000',
            'Fringilla LLP',
            'Nigeria',
            2
        ),
        (
            'Dorothy Greer',
            '4109',
            'Donec Industries',
            'New Zealand',
            2
        ),
        (
            'Heather Thomas',
            '4836',
            'Eros Nam Consequat Ltd',
            'Netherlands',
            2
        ),
        (
            'Noelle Lynn',
            '4323',
            'Est LLP',
            'Austria',
            2
        ),
        (
            'Driscoll Sweet',
            '3416',
            'Integer Sem Foundation',
            'Ukraine',
            2
        ),
        (
            'Rigel Stephenson',
            '3798',
            'Erat Eget Tincidunt Limited',
            'Pakistan',
            2
        ),
        (
            'Shoshana Benjamin',
            '4490',
            'Interdum Nunc Sollicitudin Industries',
            'Pakistan',
            2
        ),
        (
            'Beck Pratt',
            '4999',
            'Lorem Ut Aliquam Associates',
            'Poland',
            2
        ),
        (
            'Katell Woods',
            '2886',
            'Eleifend Non Dapibus Incorporated',
            'Sweden',
            2
        ),
        (
            'Olga Mcbride',
            '2234',
            'Nisl Sem Consulting',
            'Colombia',
            2
        ),
        (
            'Dawn Knowles',
            '2002',
            'Tincidunt Dui Company',
            'Poland',
            2
        ),
        (
            'Hyatt Kirby',
            '3313',
            'Maecenas PC',
            'Ireland',
            2
        ),
        (
            'Dolan Bray',
            '4925',
            'Aenean Gravida Company',
            'Germany',
            2
        ),
        (
            'Ryan Shepherd',
            '3641',
            'Consectetuer Limited',
            'Chile',
            2
        ),
        (
            'Amethyst Suarez',
            '3964',
            'Pede Nonummy Ut LLP',
            'Mexico',
            2
        ),
        (
            'Judah Aguirre',
            '3825',
            'Sem Eget Massa LLC',
            'Norway',
            2
        ),
        (
            'MacKensie Lamb',
            '3453',
            'Magnis Dis Parturient Ltd',
            'Belgium',
            2
        ),
        (
            'Honorato Schwartz',
            '4174',
            'Consectetuer Euismod LLC',
            'Canada',
            2
        ),
        (
            'Leah Best',
            '4299',
            'Magna Limited',
            'Ukraine',
            2
        ),
        (
            'Jaquelyn Best',
            '4549',
            'Risus Foundation',
            'Indonesia',
            2
        ),
        (
            'Skyler Calderon',
            '4297',
            'Eu Tellus Ltd',
            'Italy',
            1
        )
    ]

    employee_dataframe = spark.createDataFrame(employees_data).write.csv("csv_folder", mode="append")
    employee_schema = StructType().add("name", "string").add("salary", "string").add(
        "company", "string").add("country", "string").add("grade", "integer")

    employee_data = spark.readStream.option(
        "sep", ",").schema(employee_schema).csv("csv_folder")
    employee_data.printSchema()
    country_count = employee_data.groupBy("grade").count()
    employee_query = (country_count.writeStream.queryName(
        'employee_data_query').outputMode('complete').format('memory').start())
    spark.sql("select * from employee_data_query").toPandas().head(10)
