# Real-Time Unstructured Data Processing with Spark and AWS

## Overview
This project demonstrates a scalable approach for real-time processing of unstructured data using Apache Spark and AWS services. The application streams unstructured data, processes it to extract relevant details, and saves it in a structured format on AWS S3. Additionally, AWS Glue and Athena are used for data cataloging and querying, providing a seamless workflow for data analysis.

---

## Prerequisites

### Software Requirements

- **Apache Spark**: Install Spark with support for Hadoop libraries.
- **Hadoop**: Required to manage large-scale data processing tasks in Spark.
- **PySpark**: Python API for Apache Spark.

### AWS Requirements

- **AWS IAM**: To manage permissions for various AWS services.
- **AWS S3**: To store the processed data in Parquet format.
- **AWS Glue**: To catalog data for querying.
- **AWS Athena**: To query the data from S3.

---

## Installation

### Clone the Repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Run the Spark Application
Execute the following command to submit the job to Spark:
```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.1026 app.py
```

- The details of the extracted data will be displayed in the terminal.
- Ensure that all dependencies are satisfied for smooth execution.

---

## AWS Configuration (ROOT USER)

### Step A: Create an IAM User

1. Log in to AWS Console as **ROOT USER**.
2. Navigate to **IAM** > **Users** and create a new user with **Programmatic access**.

### Step B: Assign IAM Inline Policies
To allow the IAM user to access specific services, assign the following inline policies with full access for:
1. **IAM**
2. **S3**
3. **Glue**
4. **Athena**

These policies allow the user to interact with the services required for this project.

---

## Switch to IAM User and Generate Access Keys

1. Log out of the ROOT account and sign in as the IAM User you created.
2. Go to **IAM** > **Users** > **Security Credentials** and create **Access Key** and **Secret Access Key**.
3. Copy these values into your AWS configuration file (`~/.aws/credentials`) or specify them directly in your configuration code.

---

## AWS S3 Setup

1. Create a new S3 bucket named **spark-unstructured-streaming**.
2. Once you run the code, you should see data saved in **Parquet** format under the `/data` directory in this bucket.

---

## Data Cataloging with AWS Glue

1. Go to **AWS Glue** and create a database.
2. Set up a **Crawler** for the S3 bucket (i.e., `spark-unstructured-streaming`).
3. The crawler will catalog the data, enabling it to be queried from Athena.

---

## Querying Data with AWS Athena

1. After the Glue crawler completes, go to **AWS Athena**.
2. Choose the database where the crawler stored the data.
3. Run queries to retrieve insights from the unstructured data that was transformed into a structured format.

---

## Explanation of Key Steps

### Data Schema Creation
This step defines the structure for handling unstructured data. The schema is designed to accommodate varying data types, including text, JSON, and other file formats.

### User-Defined Functions (UDFs)
Custom UDFs allow for specific data extraction tasks based on the type of unstructured data being processed.

### Data Parsing and Structuring
Data from unstructured sources is parsed, and text content is extracted and then structured into a DataFrame format for ease of manipulation and analysis.

### Data Stream Joining
Both structured and unstructured data streams are joined to generate comprehensive insights from heterogeneous data sources.

### Saving Data to S3
The final DataFrame is saved in **Parquet** format, providing a compact, efficient file storage option that integrates seamlessly with AWS services.

### AWS Glue Crawler and Athena Querying
The Glue crawler catalogs the processed data, and Athena allows for querying without needing a dedicated ETL process, making this pipeline efficient and scalable.

---

## Running the Application

1. Ensure all AWS services are configured as detailed above.
2. Execute the Spark job using the `spark-submit` command.
3. Data will automatically be written to the S3 bucket in Parquet format, and the AWS Glue crawler will catalog this data.
4. Once cataloged, data can be queried in Athena using standard SQL syntax.

---



