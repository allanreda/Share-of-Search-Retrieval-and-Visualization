# Share of Search Data Retrieval and Visualization

## Overview
This script retrieves search volume data from the Google Ads Keyword Planner, using Google Ads API and exports it to Google BigQuery. It's using the Danish banks as keywords, but you can change these to whatever suits your needs.

## Functionality
The script calculates the date and the name of last month, in order to use it in the data retrieval. This also allows for further automation. The data retrieval itself is done by using the Keyword Planner from the Google Ads API. An important aspect to remember here, is the definition of the exact geographical regions that you want to include, defined as number codes in string format. The retrieved data is processed by a function that turns the API response into a workable dataframe, which is then exported to BigQuery.

## Technologies
The project is built using  
-Google Ads API for retrieving search numbers  
-Google BigQuery API/pandas_gbq for easy data export and storage of dataframes  
-Pandas for data manipulation

## Goal
The goal is to make it easy to keep track of market shares of certain industries. This is useful for understanding the market, figuring out SEO, and measuring how well you are doing against the competition.
Also, the final technical goal is to be able to use the data in the BigQuery table to visualize the data. You can view my example of the Danish banking industry below, created in Looker Studio with BigQuery as the data source.  
![image](https://github.com/allanreda/Share_of_Search_Retrieval_and_Visualization/assets/89948110/c89a6adb-af69-42b2-8794-e621b79d9c22)
