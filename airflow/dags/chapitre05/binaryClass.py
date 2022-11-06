from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import datetime
import os
spark = SparkSession.builder.appName('binary_class').getOrCreate()


def groupeDataBy(data: DataFrame, column: str):
    """
    Regroupe les donnees par colonne

    Parameters
    ----------
    data `pyspark.sql.DataFrame` : les donnees à traiter

    column `str` : colonne de réference pour grouper

    Return
    -------
    `Any`
    """
    try:
        grouped = data.groupBy(column).count().show()
        return grouped
    except:
        print("Colonne innexistante")


def toVectorisedForm(columnToEncode: str, indexerOutputColomn: str, encoderOutputColumn: str, data: DataFrame):
    """
    Cette fonction permet de vectoriser des données

    Parameters
    ----------
    columnToEncode `str` : colone à convertir ; indexerOutputColomn `str` : colone à afficher apres indexation
    encoderOutputColumn `str` : colone à afficher apres encodage ; data `pyspark.sql.dataFrame` : données entrantes

    Return
    -------
    `pyspark.sql.DataFrame`
    """
    column_indexer = StringIndexer(
        inputCol=columnToEncode, outputCol=indexerOutputColomn).fit(data)
    data: DataFrame = column_indexer.transform(data)

    column_encoder = OneHotEncoder(
        inputCol=indexerOutputColomn, outputCol=encoderOutputColumn)

    data = column_encoder.fit(data).transform(data)

    return data


def assembleData(vectorInputColumns, vectorOutputColumnName, data):
    data_assembler = VectorAssembler(outputCol=vectorOutputColumnName)
    data_assembler.setInputCols(vectorInputColumns)
    return data_assembler.transform(data)


def trainingModel(data: DataFrame, data_test: DataFrame):
    log_reg = LogisticRegression().fit(data)
    log_reg_summary = log_reg.summary
    predictions = log_reg.transform(data_test)
    print("############## IN TRAINING MODEL ############## \n \n")

    print(f"Precision : {log_reg_summary.accuracy}")
    print(f"Zone sous ROC : {log_reg_summary.areaUnderROC}")
    print(f"Precision par label : {log_reg_summary.precisionByLabel}")
    print(f"Zone sous ROC : {log_reg_summary.areaUnderROC}")
    print(f"Rappel par label : {log_reg_summary.recallByLabel} \n \n")

    print("PREDICTIONS")
    predictions.show(10)
    model_predictions = log_reg.transform(data_test)
    model_predictions = log_reg.evaluate(data_test)
    print(f"Precision : {model_predictions.accuracy}")
    print(f"Zone sous ROC : {model_predictions.areaUnderROC}")
    print(f"Precision par label : {model_predictions.precisionByLabel}")
    print(f"Zone sous ROC : {model_predictions.areaUnderROC}")
    print(f"Rappel par label : {model_predictions.recallByLabel} \n \n")

    print("############## END TRAINING MODEL ##############")


def forestClassifier(trainedModel: DataFrame, test_data: DataFrame):
    rf = RandomForestClassifier()
    rf_model = rf.fit(trainedModel)
    model_predictions = rf_model.transform(test_data)
    evaluator = BinaryClassificationEvaluator()
    rf = RandomForestClassifier()
    paramGrid = (ParamGridBuilder().addGrid(rf.maxDepth, [5, 10, 20, 25, 30]).addGrid(
        rf.maxBins, [20, 30, 40]).addGrid(rf.numTrees, [5, 20, 50]).build())
    cv = CrossValidator(
        estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
    cv_model = cv.fit(trainedModel)


def trainingModelCustom(filePath: str):
    input_data = spark.read.csv(filePath, header=True, inferSchema=True)
    print("DONNEES D'ENTREE")
    input_data.show()
    vectorisedData = toVectorisedForm(
        'loan_purpose', 'loan_index', 'loan_purpose_vec', input_data)
    vectorColumns = ['is_first_loan', 'total_credit_card_limit', 'avg_percentage_credit_card_limit_used_last_year',
                     'saving_amount', 'checking_amount', 'is_employed', 'yearly_salary', 'age', 'dependent_number', 'loan_purpose_vec']
    assembledData = assembleData(vectorColumns, "features", vectorisedData)
    #assembledData.select(['features', 'label']).show(10, False)
    training_account_data, test = assembledData.randomSplit([0.75, 0.25])
    return trainingModel(training_account_data, test)


def chapitre5():
    trainingModelCustom("./data/account_info.csv")
