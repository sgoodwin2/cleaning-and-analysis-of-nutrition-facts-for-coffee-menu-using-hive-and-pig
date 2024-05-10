# Cleaning and Analysis of Nutrition facts for Starbucks Menu using Hadoop

## Description
This repo contains data, screenshots, visualisations and scripts to perform analysis on starbucks menu nutrition facts using both PIG and HIVE. The data folder contains the source data. The outputs folder contains teh outputs to the PIG and HIVE scripts. the screenshots folders contain screenshots of the results of running the code in the scripts. the visualisation folder contains a visualisation created in python using seaborn to give extra insight into on of the hive query results.

## Scripts
There are several scripts written in both HIVE and PIG in this repo as well as a jupyter notebook. The cleaning.pig script loads, cleans and stores the source data. The processing.pig script performs some simple queries on the cleaned data. The hive_queries.hive script performs the same queries as the processing.pig script but in HIVE. It also performs some more complex queries. Finally, the visualisations.ipynb notebook creates a visualisation based on the results of one of the hive queries.

## Running the scripts
To run the PIG and HIVE srcripts on your own machine, you will have to replace the file paths in script files with you own file path.

## Cloud Deployment
The cloud_deployment folder contains a version of the main scripts that are altered so that they can run on Google Cloud