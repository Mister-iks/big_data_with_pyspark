from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from airflow.decorators import task


# def display_data():
#     """
#     Cette fonction permet d'afficher le contenu d'un dataframe
#     :param data_schema : la structure du dataframe
#     :param data: correspond aux données que contiennent le dataframe
#     """
#     user_schema = StructType().add("user_id", "string").add("country", "string").add("browser", "string").add(
#         "os", "string").add("age", "integer")  # permet de definir la structure du dataframe

#     user_data = [("A203", 'India', "Chrome", "WIN", 33), (
#         "A201", 'China', "Safari", "MacOS", 35), (
#         "A205", 'UK', "Mozilla", "Linux", 25)]

#     frame_data = spark.createDataFrame(
#         user_data, user_schema)  # creation du dataframe
#     return frame_data.show()


def read_csv(path: str):
    """
    Charge et lit un fichier CSV

    Parameters
    ----------
    path `str` : chemin du fichier

    Return
    ------
    `pyspark.sql.DataFrame.show`
    """
    spark = SparkSession.builder.appName('data_frame').getOrCreate()
    file = spark.read.csv(path=path, header=True, inferSchema=True)
    return file.show()

@task
def chapitre2():
    read_csv("./employees_1.csv", 20)
