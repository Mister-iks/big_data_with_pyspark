# PySpark - Traitement de l'information

Dans ce chapitre, nous allons étudier les différentes façons de traiter l'information avec Pyspark.

- Pour utiliser spark, nous devrons utiliser un objet **SparkSession**

```cmd
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName('data_processing').getOrCreate()
```

Ainsi pour chacune des parties ci-dessous, vous trouverez un fichier **.ipynb** du même nom avec des exemples.

## Steps

| Parties                       | Fichier                                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dataframes                    | [chapitre02\Dataframes.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/Dataframes.ipynb)                 |
| NullValues                    | [chapitre02\NullValues.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/NullValues.ipynb)                 |
| FileReader                    | [chapitre02\FileReader.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/FileReader.ipynb)                 |
| Subset of a dataframe         | [chapitre02\SubsetOfADataFrame.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/SubsetOfADataFrame.ipynb) |
| User-Defined Functions (UDFs) | [chapitre02\Udf.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/Udf.ipynb)                               |
| Panda UDF                     | [chapitre02\PandaUdf.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/PandaUdf.ipynb)                     |
| Les jointures                 | [chapitre02\Join.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/Join.ipynb)                             |
| Window Functions              | [chapitre02\WindowFunctions.ipynb](https://github.com/KhalilouLahi-Samb/pyspark_pour_big_data/blob/master/chapitre02/WindowFunctions.ipynb)       |
