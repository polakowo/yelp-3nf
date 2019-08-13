# Project Summary

## Motivation

The aim of this project is to create a relational DWH for storing and querying Yelp data. 

For this, we will build a data pipeline that translates the non-relational Yelp dataset distributed over JSON files in Amazon S3 bucket, into a 3NF-normalized dataset stored on Amazon Redshift. This dataset will be a source of truth for further dimensional tables. Additionally, we will enrich the data with demographics and weather data coming from third-party data sources. The entire process will be done using Apache Spark and Amazon Redshift. 

The designed Amazon Redshift data warehouse follows the [Inmon's Corporate Information Factory (CIF)](https://www.techopedia.com/definition/28024/corporate-information-factory-cif) architecture: it builds upon the centralized corporate data repository (top-down design) which is 3NF normalized. This repository is our target artifact and is designed as a single integrated source of truth. Dimensional data marts can be then created from the DWH based on end user needs. This architecture ensures data integrity and consistency; it also brings the advantage of coordinating data marts through the same ETL processes.

<img width=600 src="https://bennyaustin.files.wordpress.com/2010/05/hybrid.jpg"/>

As a result, the end user can either create data marts on top of the designed central repository, design a set of dimensional tables for their analytics team to find valuable insights, or use BI and visualization tools such as Tableau to access the data on Redshift. We will create a sample star schema to demonstrate the power of the newly designed data model.

## Datasets

<img width=200 src="https://upload.wikimedia.org/wikipedia/commons/thumb/a/ad/Yelp_Logo.svg/1200px-Yelp_Logo.svg.png"/>

The [Yelp Open Dataset](https://www.yelp.com/dataset) is a perfect candidate for this project, since: 

- (1) it is a NoSQL data source; 
- (2) it comprises of 6 files that count together more than 10 million rows; 
- (3) this dataset provides lots of diverse information and allows for many analysis approaches, from traditional analytical queries (such as "*Give me the average star rating for each city*") to Graph Mining, Photo Classification, Natural Language Processing, and Sentiment Analysis; 
- (4) Moreover, it was produced in a real production setting (as opposed to synthetic data generation). 

To make the contribution unique, the Yelp dataset was enriched by demographics and weather data. This allows us to make queries such as "*Does the number of ratings depend upon the city's population density?*" or "*Which restaurants are particularly popular during hot weather?*".

### Yelp Open Dataset

The Yelp dataset is a subset of Yelp's businesses, reviews, and user data, available for academic use. The dataset we use (13.08.2019) takes 9GB disk space (unzipped) and counts 6,685,900 reviews, 192,609 businesses over 10 metropolitan areas, over 1.2 million business attributes like hours, parking, availability, and ambience, 1,223,094 tips by 1,637,138 users, and aggregated check-ins over time. Each file is composed of a single object type, one JSON-object per-line. For more details on dataset structure, proceed to [Yelp Dataset JSON Documentation].

#### Installation

- Create an S3 bucket. 
    - Ensure that the bucket is in the same region as your Amazon EMR and Redshift clusters.
- Download [Yelp Open Dataset](https://www.yelp.com/dataset) and directly upload to your S3 bucket (`yelp_dataset` folder).
- For slow internet connections: 
    - Launch an EC2 instance with at minimum 20GB SSD storage.
    - Connect to this instance via SSH (click "Connect" and proceed according to AWS instructions)
    - Proceed to the dataset homepage, fill in your information, copy the download link, and paste into the command below. Note: the link is valid for 30 seconds.

```bash
wget -O yelp_dataset.tar.gz "[your_download_link]"
tar -xvzf yelp_dataset.tar.gz
```

- Finally, transfer the files as described by [this blog](http://codeomitted.com/transfer-files-from-ec2-to-s3/)
    - Remember to provide IAM role and credentials of user who has AmazonS3FullAccess.
    - In case your instance has no AWS CLI installed, follow [this documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
    - In case you come into errors such as "Unable to locate package python3-pip", follow [this answer](https://askubuntu.com/questions/1061486/unable-to-locate-package-python-pip-when-trying-to-install-from-fresh-18-04-in#answer-1061488)

### U.S. City Demographic Data

This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey. Each JSON object describes the demographics of a particular city and race, and so it can be uniquely identified by the city, state and race fields. More information can be find on [the website](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/) under the section "Dataset schema".

#### Installation

- Download the JSON file from https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
- Upload it to a separate folder (`demo_dataset`) on your S3 bucket.

### Historical Hourly Weather Data 2012-2017

[Historical Hourly Weather Data](https://www.kaggle.com/selfishgene/historical-hourly-weather-data) is a dataset collected by a Kaggle competitor. The dataset contains 5 years of hourly measurements data of various weather attributes, such as temperature, humidity, and air pressure. This data is available for 27 bigger US cities, 3 cities in Canada, and 6 cities in Israel. Each attribute has it's own file and is organized such that the rows are the time axis (timestamps), and the columns are the different cities. Additionally, there is a separate file to identify which city belongs to which country.

####t Installation

- Download the whole dataset from https://www.kaggle.com/selfishgene/historical-hourly-weather-data/downloads/historical-hourly-weather-data.zip
- Unzip and upload `city_attributes.csv`, `temperature.csv`, and `weather_description.csv` files to a separate folder (`weather_dataset`) on your S3 bucket.

## Data pipeline

<img width=100 src="https://www.dashsdk.com/wp-content/uploads/2018/12/amazon-s3-logo.png"/>

All three datasets will reside in a Amazon S3 bucket, which is the easiest and safest option to store and retrieve any amount of data at any time from any other AWS service. 

<img width=100 src="https://upload.wikimedia.org/wikipedia/en/thumb/2/29/Apache_Spark_Logo.svg/1200px-Apache_Spark_Logo.svg.png"/>

Since the data is in JSON format and contains arrays and nested fields, it needs first to be transformed into a relational form. By design, Amazon Redshift does not support loading nested data (only Redshift Spectrum enables you to query complex data types such as struct, array, or map, without having to transform or load your data). To do this in a quick and scalable fashion, we will utilize Apache Spark. In particular, we will run an Amazon EMR (Elastic MapReduce) cluster, which uses Apache Spark and Hadoop to quickly & cost-effectively process and analyze vast amounts of data. The another advantage of Spark is the ability to control data quality, thus most of our tests will be done at this stage. With Spark, we will dynamically load JSON files from S3, process them, and store their normalized and enriched versions back into S3 in Parquet format.

<img width=100 src="https://www.dashsdk.com/wp-content/uploads/2018/12/amazon-s3-logo.png"/>

Parquet stores nested data structures in a flat columnar format. Compared to a traditional approach where data is stored in row-oriented approach, parquet is more efficient in terms of storage and performance. Parquet files are well supported in the AWS ecosystem. Moreover, compared to JSON and CSV formats, we can store timestamp objects, datetime objects and long texts without any post-processing, and load them into Amazon Redshift as-is. From here, we can use an AWS Glue crawler to discover and register the schema for our datasets to be used in Amazon Athena. But our goal is materializing the data rather than querying directly from files on Amazon S3 - to be able to query the data without expensive load times as experienced in Athena or Redshift Spectrum. 

<img width=150 src="https://cdn.sisense.com/wp-content/uploads/aws-redshift-connector.png"/>

To load the data from Parquet files into our Redshift DWH, we can rely on multiple options. The easiest one is by using [spark-redshift](https://github.com/databricks/spark-redshift): Spark reads the parquet files from S3 into the Spark cluster, converts the data to Avro format, writes it to S3, and finally issues a COPY SQL query to Redshift to load the data. Or we can have [an AWS Glue job that loads data into an Amazon Redshift](https://www.dbbest.com/blog/aws-glue-etl-service/). But instead, we will define the tables manually. Why? Because that way we can control data quality and consistency, sortkeys, distkeys and compression. Thus, we will issue SQL statements to Redshift to CREATE the tables and the ones to COPY the data. To make our lives easier, we will utilize the AWS Glue's data catalog to derive the correct data types (for example, should we use int or bigint?).

## Date updates

The whole ETL process for 7 million reviews and related data lasts about 20 minutes. As our target data model is meant to be the source for other dimensional tables, the ETL process can take longer time. Since the Yelp Open Dataset is only a subset of the real dataset and we don't know how many rows Yelp generates each day, we cannot derive the optimal frequency of the updates. But taking only newly appended rows (for example, those collected for one day) can significantly increase the frequency.

## Scenarios

The following scenarios needs to be addressed:
- The data was increased by 100x: That wouldn't be a technical issue as both Amazon EMR and Redshift clusters can handle huge amounts of data. Eventually, they would have to be scaled out.
- The data populates a dashboard that must be updated on a daily basis by 7am every day: That's perfectly plausible and could be done by running the ETL script some time prior to 7am.
- The database needed to be accessed by 100+ people: That wouldn't be a problem as Redshift is highly scalable and available.

## End result

