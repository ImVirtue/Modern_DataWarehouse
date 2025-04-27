# An ecommerce on-prem Modern Data Warehouse with 6.000.000 records Integrating with financial AI Agent

<img src="images/Modern_Data_Warehouse_01.jpg" width="500">

## Table of Contents
- [Introduction](#introduction)
- [Overview](#overview)
- [Architecture](#architecture)
- [Result](#result)

## Introduction
- This project's intention is to build an on-prem modern data warehouse architecture (The combination of Datalake and relational Datawarehouse) with a full pipeline from raw sources (OLTP, API, Images,...) to the useful sources Data Warehouse (BI and AI agent)
- The Pipeline leverages a combination of tools and services, including Apache Airflow, PostgreSQL, Apache Spark, Apache Kafka, Clickhouse, Minio, Superset, and an AI agent product.

## Overview
The Architecture is designed to:
1. Extract raw data from an OLTP database
2. Load data and load it into the raw layer in Data Lake (Minio)
3. From the raw layer, load data to the processed layer (Checking null, duplicates, and wrong data)
4. From the processed layer, load data to the serving layer (Transforming and modelling into the dimensional model)
5. From the serving layer, load data to the Data warehouse (Clickhouse server)
6. Using data in the Data Warehouse to create insightful and meaningful reports for sales, finance, customer,...
7. Creating an AI Agent product acts as a financial assistant to directly retrieve information from Data Warehouse and give thorough decisions for users.

## Architecture
 **Data Architecture**:
 <p align="center">
  <img src="images/Data_architecture.png" width="900">
 </p>
 
 **AI Agent Assistant Flow**:
 <p align="center">
  <img src="images/agent_and_bi.png" width="900">
 </p>
 
 **Minio Structure**:
<p align="center">
  <img src="images/MinIO_Raw_Processed.png" width="400"/>
  <img src="images/MinIO_serving.png" width="500"/>
</p>

**Airflow Full Flow Dag fact_orders**:
 <p align="center">
  <img src="images/fullflow_dag.png" width="900">
 </p>

**Debezium OLTP connector**:
<p align="center">
  <img src="images/Debezium_oltp_connector.png" width="900"/>
</p>

**Kafka control center**:
<p align="center">
  <img src="images/Kafka_control_center.png" width="900"/>
</p>


1. **Apache Airflow**: Orchestrates the ETL process and manages task distribution.
2. **PostgreSQL**: An OLTP database for transactional data
3. **Apache kafka**: Message Queue system for real-time data
4. **Apache ZooKeeper**: Managing Kafka broker.
5. **Apache Spark**: Big data processing.
6. **Docker Compose**: To orchestrate the deployment of the above technologies.
7. **ClickHouse**: An OLAP database for analytical data (Data Warehouse)
8. **Minio**: An object Storage layer (Datalake)
9. **Apache Superset**: A BI tool.

## Result - Visualization
**Superset - Batching**:
<p align="center">
  <img src="images/Superset_product.png" width="300"/>
  <img src="images/Superset_customer.png" width="300"/>
  <img src="images/Superset_time.png" width="300"/>
</p>

**CDC - Streaming**:
Change data (left side) in Postgresql, then Kafka -  Debezium Program (right side) will automatically capture changes in real time
<p align="center">
  <img src="images/realtime_CDC.gif" width="500"/>
</p>

**AI Agent Assistant**:
 <p align="center">
  <img src="images/financial_assistant.gif" width="500"/>
</p>

   

