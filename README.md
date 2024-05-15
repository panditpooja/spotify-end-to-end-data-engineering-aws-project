# Personal Project - ETL Data Pipeline on AWS Cloud: Orchestrating Spotify Data Flow with Python

### Introduction:
In this project, I build an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS cloud. The pipeline will retrieve data from the Spotify API, transform it too a desired format and load it into an AWS data store.

### Architecture:
![Architecture diagram](https://github.com/panditpooja/spotify-end-to-end-data-engineering-project/blob/dev/Spotify_Data_Pipeline_Architecture.PNG)

### About Dataset/API:
This API contains information about music artists, albums and songs - [Spotify API](https://spotipy.readthedocs.io/en/2.22.1/)

### Services used:
1. **S3 (Simple Storage Service):** Amazon S3 (Simple STorage Service) is a highly scalable object storage service that can store and retrieve any amount of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups, and static website files.
2. **AWS Lambda:** AWS Lambda is a serverless computing service that lets you run your code without managing servers. You can use lambda to run in response to changes like events in S3, DynamoDB or any AWS Services.
3. **AWS Cloudwatch:** Amazon Cloudwatch is a monitoring service for AWS resources and the applications you run on them. You can use cloudwatch to collect amd track metrics, collect and monitor log files and set alarms.
4. **Glue Crawler:** AWS Glue Crawler is a fully managed service that automatically crawls your data sources, identifies data formats and infers schemas to create an AWS Glue Data Catalog.
5. **AWS Glue Data Catalog:** AWS Glue Data Catalog is a fully managed metadata repository that makes it easy to discover and manage data in AWS. You can use Data Catalog with other AWS services, such as Athena.
6. **Amazon Athena:** Amazon Athena is an interactive query service that makes it easy to analyse data in Amazon S3 using standard SQL. You can use Athena to analyse data in your Glue Data Catalog or in other S3 buckets.

### Install Packages:
```
pip install pandas
pip install numpy
pip install spotipy
```

### Project Execution Flow:
Extract Data from API -> Lambda Trigger (Every 1 hour) -> Run Extract Code -> Store Raw Data -> Trigger Transform Function -> Transform Data And Load it -> Query Using Athena

### Achievements:
- Transformed Spotify data into actionable insights using Python, deployed it on AWS Cloud.
- Streamlined Data Processing by 24.8% with Automated Data Ingestion from source to analytics-ready format using CloudWatch, Lambda, S3, and Athena to create an efficient ETL pipeline, enhancing scalability and efficiency.
