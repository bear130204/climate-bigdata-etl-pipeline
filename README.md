# Climate Data Engineering Pipeline with Apache Airflow, Spark, SQL Server and Machine Learning

## Overview

This project presents an end-to-end Big Data pipeline for processing and analyzing large-scale climate data collected from the NOAA Global Historical Climatology Network (GHCN).

The system automates the entire data lifecycle, including data ingestion, transformation, storage, analytics, and prediction. Apache Airflow is used for workflow orchestration, Apache Spark for large-scale ETL processing, SQL Server as the data warehouse, and Streamlit for interactive visualization and forecasting.

The objective is to build a scalable climate analytics platform capable of handling historical weather observations from multiple countries and supporting climate trend analysis as well as machine learning-based forecasting.

---

## Architecture

NOAA Climate Dataset
↓
Apache Airflow (Workflow Orchestration)
↓
Apache Spark (ETL Processing)
↓
SQL Server (Data Warehouse)
↓
Machine Learning Prediction
↓
Streamlit Dashboard

---

## Key Features

### Automated ETL Pipeline

* Automated ingestion of NOAA climate datasets.
* Incremental ETL mechanism to process only newly available years.
* Data cleaning, transformation, and aggregation using Apache Spark.
* Workflow scheduling and monitoring with Apache Airflow.

### Data Warehouse

* Centralized climate data storage using SQL Server.
* Aggregated weather statistics by country, year, month, and climate indicator.
* Optimized structure for analytical queries and dashboard visualization.

### Climate Analytics Dashboard

* Interactive Streamlit dashboard.
* Global climate monitoring and exploration.
* Temperature and precipitation trend analysis.
* Top hottest countries analysis.
* Climate anomaly visualization.

### Machine Learning Forecasting

* Random Forest Regressor for climate prediction.
* Forecasting of aggregated climate indicators.
* Integrated prediction interface within the dashboard.

---

## Dataset

Source: NOAA Global Historical Climatology Network (GHCN)

Climate indicators:

* TMAX (Maximum Temperature)
* TMIN (Minimum Temperature)
* PRCP (Precipitation)

Coverage:

* Historical climate records from 1951–2026
* Multiple countries worldwide
* More than 70 yearly climate files

---

## ETL Workflow

### Extract

* Read NOAA climate files from compressed CSV datasets.
* Detect newly available years not yet stored in the data warehouse.

### Transform

* Filter relevant climate indicators (TMAX, TMIN, PRCP).
* Convert NOAA temperature units to Celsius.
* Extract temporal attributes (year and month).
* Map weather stations to countries.
* Aggregate climate statistics by country and period.

### Load

* Store processed data into SQL Server Data Warehouse.
* Support incremental updates without duplicate records.

---

## Machine Learning Pipeline

### Input Features

* Country Name
* Country Code
* Climate Indicator
* Year
* Month
* Maximum Value
* Minimum Value
* Record Count

### Target Variable

* Average Climate Value (avg_value)

### Model

Random Forest Regressor

### Evaluation Results

* MAE: 4.27
* MSE: 200.13
* R² Score: 0.922

The model explains approximately 92.2% of the variability in climate observations and demonstrates strong predictive performance.

---

## Technology Stack

### Data Engineering

* Apache Airflow
* Apache Spark
* SQL Server
* Docker

### Data Processing

* Pandas
* PySpark
* SQLAlchemy
* PyODBC

### Machine Learning

* Scikit-Learn
* Random Forest Regressor
* Joblib

### Visualization

* Streamlit
* Plotly
* Matplotlib

---

## Results

The project successfully delivers:

* Automated climate data ingestion and processing.
* Incremental ETL architecture.
* Centralized climate data warehouse.
* Interactive climate analytics dashboard.
* Machine learning-based climate prediction.
* Reproducible deployment using Docker and Airflow.

---

## Future Improvements

* Daily weather forecasting using XGBoost or LSTM.
* Climate anomaly detection.
* Real-time climate data ingestion.
* Geospatial climate visualization.
* Multi-model forecasting framework.
