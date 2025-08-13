# 🎵 ETL Data Pipeline on AWS Cloud: Orchestrating Spotify Data Flow with Python

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![AWS](https://img.shields.io/badge/AWS-Data%20Engineering-orange)

---

## 📌 Overview
This project implements an **end-to-end ETL pipeline** that extracts music-related data from the **Spotify API**, transforms it into an analysis-ready format, and loads it into AWS services for querying and analytics.

---

## 📂 Repository Structure
```
📁 AWS_Lambda                              # Lambda function scripts for data extraction and transformation
📄 README.md                               # Project documentation
🖼️ Spotify_Data_Pipeline_Architecture.PNG  # Architecture diagram for the ETL process
📓 Spotify_ETL_Data_Pipeline_Project.ipynb  # Jupyter notebook for local development and testing
```

---

## 🏗 Architecture:
![Architecture diagram](https://github.com/panditpooja/spotify-end-to-end-data-engineering-project/blob/dev/Spotify_Data_Pipeline_Architecture.PNG)

---

## 🎼 About Dataset/API:
This API contains information about music artists, albums and songs - [Spotify API](https://spotipy.readthedocs.io/en/2.22.1/)

---

## 🛠 AWS Services Used
| Service | Purpose |
|---------|---------|
| **Amazon S3 (Simple Storage Service)** | Highly scalable object storage service that stores raw and transformed datasets |
| **AWS Lambda** | Serverless computing service that executes extraction and transformation scripts serverlessly |
| **Amazon CloudWatch** | Monitoring service for AWS resources that automates Lambda triggers (hourly) for this project |
| **AWS Glue Crawler** | Fully managed service that automatically detects schema of datasets to create an AWS Glue Data Catalog |
| **AWS Glue Data Catalog** | Fully managed metadata repository that stores metadata for Athena queries |
| **Amazon Athena** | Interactive query service runs SQL queries on transformed datasets |

---

## 📦 Install Packages:
```
pip install pandas
pip install numpy
pip install spotipy
```

## 🔄 Project Execution Flow:
1. **Extract** → AWS Lambda fetches data from Spotify API (triggered by CloudWatch every hour)  
2. **Store Raw Data** → Save to Amazon S3  
3. **Transform** → AWS Lambda processes and formats data  
4. **Load** → Save transformed data to S3 (analytics bucket)  
5. **Schema Inference** → AWS Glue Crawler updates Data Catalog  
6. **Query** → Amazon Athena enables SQL analytics

```
Extract Data from API -> Lambda Trigger (Every 1 hour) -> Run Extract Code -> Store Raw Data -> Trigger Transform Function -> Transform Data And Load it -> Query Using Athena
```

---

## 🏆 Achievements:
- Transformed Spotify data into actionable insights using Python, deployed it on AWS Cloud.
- Streamlined Data Processing by 24.8% with Automated Data Ingestion from source to analytics-ready format using CloudWatch, Lambda, S3, and Athena to create an efficient ETL pipeline, enhancing scalability and efficiency.
-  Built serverless architecture ensuring scalability and cost efficiency.

---

## 📊 Example Queries
- Top 10 trending artists in the past 24 hours.
- Most streamed tracks by genre.
- Artist collaboration frequency.

---

## ✍️ Author

**Pooja Pandit**  
Master’s Student in Information Science (Machine Learning)  
The University of Arizona  

[![GitHub](https://img.shields.io/badge/GitHub-panditpooja-black?logo=github)](https://github.com/panditpooja)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-pooja--pandit-0077B5?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/pooja-pandit-177978135/)  
