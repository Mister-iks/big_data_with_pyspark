from pyspark.sql.functions import *
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import sha2
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import rand, randn
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from mpl_toolkits.mplot3d import Axes3D

spark = SparkSession.builder.appName('clustering').getOrCreate()


def clustering():
    """
    Construis un cluster de données

    Return 
    -------
    `matplotlib.pyplot.show`
    """
    # chargement du fichier
    df = spark.read.csv('./music_data.csv', inferSchema=True, header=True)
    print("########## GROUPED BY USER ID ##############")
    # creating new df with single record for each user
    feature_df = df.stat.crosstab("user_id", "music category")
    feat_cols = [col for col in feature_df.columns if col !=
                 'user_id_music category']
    vec_assembler = VectorAssembler(inputCols=feat_cols, outputCol='features')
    final_data = vec_assembler.transform(feature_df)
    # scaling the features
    scaler = StandardScaler(
        inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(final_data)
    # Normalize each feature to have unit standard deviation.
    cluster_final_data = scalerModel.transform(final_data)
    errors = []

    for k in range(2, 10):
        kmeans = KMeans(featuresCol='scaledFeatures', k=k)
        model = kmeans.fit(cluster_final_data)
        error_tab = model.computeCost(cluster_final_data)
        errors.append(error_tab)
        print("K={}".format(k))
        print("Somme Erreurs = " + str(error_tab))
        print('--'*30)
    cluster_number = range(2, 10)
    plt.scatter(cluster_number, errors)
    plt.xlabel('clusters')
    plt.ylabel('WSSE')
    plt.show()
    # k =6 pour le cluster
    kmeans = KMeans(featuresCol='scaledFeatures', k=6)
    # Raccordement
    model_kmeans = kmeans.fit(cluster_final_data)
    # Nombre de clients pour chaque cluster
    model_kmeans.transform(cluster_final_data).groupBy(
        'prediction').count().show()
    model_kmeans.transform(cluster_final_data).show(10)
    cluser_prediction = model_kmeans.transform(cluster_final_data)
    pca = PCA(k=3, inputCol="scaledFeatures", outputCol="pca_features")
    pca_model = pca.fit(cluser_prediction)

    result = pca_model.transform(cluser_prediction).select(
        'user_id_music category', "pca_features", 'prediction')
    result.show(truncate=False)
    clusters = result.toPandas().set_index('user_id_music category')
    clusters.head(10)
    cluster_vis = plt.figure(figsize=(10, 10)).gca(projection='3d')
    cluster_vis.scatter(clusters.x, clusters.y,
                        clusters.z, c=clusters.prediction)
    cluster_vis.set_xlabel('x')
    cluster_vis.set_ylabel('y')
    cluster_vis.set_zlabel('z')
    return plt.show()


def chapitre7():
    clustering()
