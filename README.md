Overview
This project aims to securely manage, streamline, and perform analysis on structured and semi-structured data from YouTube videos based on categories and trending metrics.

Project Goals
Data Ingestion: Build a mechanism to ingest data from various sources.
ETL System: Transform raw data into a proper format for analysis.
Data Lake: Establish a centralized repository to store data from multiple sources.
Scalability: Ensure the system scales with increasing data size.
Cloud: Utilize AWS to process vast amounts of data efficiently.
Reporting: Develop a dashboard to provide insights based on the analysis.
Services Used
Amazon S3: Object storage service providing scalability, data availability, security, and performance.
AWS IAM: Identity and access management service for securely managing access to AWS resources.
Amazon QuickSight: Scalable, serverless business intelligence service for data analysis and visualization.
AWS Glue: Serverless data integration service for discovering, preparing, and combining data for analytics.
AWS Lambda: Computing service for running code without server management.
AWS Athena: Interactive query service for analyzing data directly in S3 without loading it.
Dataset Used
The dataset used in this project is sourced from Kaggle, containing statistics on daily popular YouTube videos over several months. Each region has a separate file with details such as:

Video title
Channel title
Publication time
Tags
Views
Likes and dislikes
Description
Comment count
Category ID (specific to each region)


Architecture Diagram

![image](https://github.com/user-attachments/assets/cf81844d-3da0-47e5-8219-74d686b4450e)


Getting Started
Prerequisites
AWS Account
Access to AWS services listed above
Python (version x.x)
Required Python libraries
