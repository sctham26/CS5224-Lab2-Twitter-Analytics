# CS5224 Lab 2 — Twitter User Statistics

## Overview

This project implements a cloud-based Twitter analytics pipeline to calculate the number of followers and followees for two specified users.

## Technologies

- **AWS Glue (PySpark):** Processes the Twitter relationship dataset in parallel, removes duplicate edges, and calculates follower/followee counts.
- **Amazon S3:** Stores the original dataset and Spark-generated Parquet results.
- **Amazon Athena:** Queries the processed Parquet data.
- **AWS Lambda (Python):** Retrieves statistics from Athena and returns JSON.
- **Amazon API Gateway (HTTP API):** Exposes the `GET /stats` endpoint.
- **HTML, CSS, JavaScript:** Displays the statistics using the Fetch API.

## Results

| User ID | Followers | Followees |
|---|---:|---:|
| 214328887 | 180 | 214 |
| 107830991 | 180 | 157 |

## Source Files

- `index.html` — Frontend implementation
- `lambda_function.py` — Backend implementation
- `glue_spark_analysis.py` — PySpark data analysis

## Execution

The PySpark script was executed as an AWS Glue Spark ETL job. Its results were stored as Parquet files in Amazon S3 and registered in Athena as `user_stats_spark`.

AWS Lambda queries this table through Athena, and Amazon API Gateway exposes the results through an HTTP GET endpoint.

The frontend was tested locally using VS Code Live Server.

## Notes

The AWS infrastructure and source dataset are not included in this repository. Running the code requires appropriately configured AWS resources, permissions, and environment variables.
