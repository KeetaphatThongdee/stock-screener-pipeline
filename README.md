# Stock Data Pipeline

This repository contains an automated web scraping pipeline developed as part of my Data Engineering internship. 

The project automatically extracts financial data from a stock screener on a daily basis. It handles API rate limits using a basic retry mechanism, cleans the data using Pandas, and stores it locally following a simple Bronze (JSON) and Silver (Parquet) structure. The entire workflow is orchestrated by Prefect and runs inside a Docker container.
