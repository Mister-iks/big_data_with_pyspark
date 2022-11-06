from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
spark = SparkSession.builder.appName('basic_stats').getOrCreate()


def createVectorRepresentation(frameDataPath: str, columnName: str):
    """
    Combine plusieurs colonnes d'un dataframe en une seule.

    Parameters
    -----------
    frameDataPath: `str` chemin vers le fichier

    columnName:`str` nom de la nouvelle colonne

     Returns
    -------
    `DataFrame`
    """
    col = columnName
    data = spark.read.csv(frameDataPath, header=True, inferSchema=True)
    assembler = VectorAssembler(inputCols=data.columns, outputCol=columnName)
    data_transformed = assembler.transform(data)
    return data_transformed


def correlationCalcul(vectorRepresentation: DataFrame, columName: str, method: str):
    """
    Calcul de la correlation.

    Parameters
    -----------
    vectorRepresentation: `pyspark.sql.DataFrame`

    columnName:`str` nom de la nouvelle colonne

    method:`str` methode de calcul à utiliser pearson/spearman

    Returns
    -------
    `DataFrame`
    """
    corr = Correlation.corr(vectorRepresentation, columName, method)
    return corr


vector = createVectorRepresentation("./data/job_experience.csv", "features")
task_4 = correlationCalcul(vector, "features", "spearman").show(2, False)

correlationCalcul(vector, "features", "pearson").show(2, False)