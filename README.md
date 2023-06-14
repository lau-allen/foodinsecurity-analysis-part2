# Analyzing Food Insecurity in NYC using Safegraph CBG Data - Part 2

The goal of this notebook is to study the food insecurity problem. Specifically, the objective is to determine the distance people traveled to grocery stores by census block group (CBG) using [Safegraph data](https://docs.safegraph.com/docs/weekly-patterns). In particular, we would like to know for each CBG, the average distance they traveled to the listed grocery stores in March 2019, October 2019, March 2020, and October 2020. (We select March and October to avoid summer and holidays with more noise from tourists and festivity shopping). In this project, we will use the distances projected in the NAD83 plane (EPSG:2263).


This analysis will be done using Apache Spark and Google Colab / Google Cloud Dataproc. 
