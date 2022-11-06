import os
import numpy as np
import pandas as pd
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, StringType
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StringIndexer

spark = SparkSession.builder.appName('deep_learning').getOrCreate()


def multilayerPercept():
    data = spark.read.csv('./datasource.csv', header=True,
                          inferSchema=True)  # lecture du fichier
    data = data.withColumnRenamed('Orders_Normalized', 'label')
    train, validation, test = data.randomSplit([0.7, 0.2, 0.1], 1234)
    categorical_columns = [item[0]
                           for item in data.dtypes if item[1].startswith('string')]
    numeric_columns = [item[0]
                       for item in data.dtypes if item[1].startswith('double')]
    indexers = [StringIndexer(inputCol=column, outputCol='{0}_index'.format(
        column)) for column in categorical_columns]
    featuresCreator = VectorAssembler(inputCols=[indexer.getOutputCol(
    ) for indexer in indexers] + numeric_columns, outputCol="features")
    layers = [len(featuresCreator.getInputCols()), 4, 2, 2]
    classifier = MultilayerPerceptronClassifier(
        labelCol='label', featuresCol='features', maxIter=100, layers=layers, blockSize=128, seed=1234)
    pipeline = Pipeline(stages=indexers + [featuresCreator, classifier])
    model = pipeline.fit(train)
    train_output_df = model.transform(train)
    validation_output_df = model.transform(validation)
    test_output_df = model.transform(test)
    train_predictionAndLabels = train_output_df.select("prediction", "label")
    validation_predictionAndLabels = validation_output_df.select(
        "prediction", "label")
    test_predictionAndLabels = test_output_df.select("prediction", "label")

    metrics = ['weightedPrecision', 'weightedRecall', 'accuracy']

    for metric in metrics:
        evaluator = MulticlassClassificationEvaluator(metricName=metric)
        print('Train ' + metric + ' = ' +
              str(evaluator.evaluate(train_predictionAndLabels)))
        print('Validation ' + metric + ' = ' +
              str(evaluator.evaluate(validation_predictionAndLabels)))
        print('Test ' + metric + ' = ' +
              str(evaluator.evaluate(test_predictionAndLabels)))


def chapitre8():
    multilayerPercept()
