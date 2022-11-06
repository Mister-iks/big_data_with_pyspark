# PySpark

Dans ce chapitre, nous allons installer et utiliser Pyspark en utilisant docker.

# NOTES

-   Le dossier **_./airflow_** contient l'implementation fait grace a docker
-   Le dossier **_./Notes_jupyter_notebook_** contient les implémentations faites avec jupyter accompagnées de la documentation.

## Installation

Dans ce premier chapitre, nous allons utiliser des **images docker** .

![Logo](https://www.edureka.co/blog/wp-content/uploads/2018/07/PySpark-logo-1.jpeg)

Ouvrez la ligne de commande puis saisir les commandes ci-dessous

```cmd
  docker pull jupyter/pyspark-notebook // pour installer
  docker run -it -p 8888:8888 jupyter/pyspark-notebook //pour run
```

Une fois l'image lancée, vous allez copier le lien qu'il va génerer et l'ouvrir sur votre navigateur .

Vous devriez alors avoir une interface qui ressemble à ça
![alt](https://developers.refinitiv.com/content/dam/devportal/articles/how-to-set-up-and-run-data-science-development-environment-with-jupyter-on-docker/02_jupyter_lab.png)

### Apache airflow

Telecharger le fichier [**_dockeer-compose.yml_**]("https://github.com/marclamberti/docker-airflow/blob/main/docker-compose.yml")
et suivre les instructions d'installation.

## Demarrage du projet

Se rendre dans **_./airflow_** puis faire

```cmd
  docker build .
  docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
  docker-compose up
```
