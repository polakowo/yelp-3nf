# Datasets

The aim of this project is to create a source-of-truth database by translating a NoSQL data model into a 3NF relational data model, and enrich it with information from third-party data sources. The [Yelp Open Dataset](https://www.yelp.com/dataset) is a perfect candidate for this project, since: (1) it is a NoSQL data source; (2) it comprises of 6 files that count together more than 10 million rows; (3) this dataset provides lots of diverse information and allows for many analysis approaches, from traditional analytical queries (such as "*Give me the average star rating for each city*") to Graph Mining, Photo Classification, Natural Language Processing, and Sentiment Analysis; (4) Moreover, it was produced in a real production setting (as opposed to synthetic data generation). To make the contribution unique, the Yelp dataset was enriched by demographics and weather data. This allows us to make queries such as "*Does the number of ratings depend upon the city's population density?*" or "*Which restaurants are particularly popular during hot weather?*".

## Yelp Open Dataset

The Yelp dataset is a subset of Yelp's businesses, reviews, and user data, available for academic use. The dataset we use (13.08.2019) takes 9GB disk space (unzipped) and counts 6,685,900 reviews, 192,609 businesses over 10 metropolitan areas, over 1.2 million business attributes like hours, parking, availability, and ambience, 1,223,094 tips by 1,637,138 users, and aggregated check-ins over time. Each file is composed of a single object type, one JSON-object per-line. For more details on dataset structure, proceed to [Yelp Dataset JSON Documentation].

### Installation

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

## U.S. City Demographic Data

This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey. Each JSON object describes the demographics of a particular city and race, and so it can be uniquely identified by the city, state and race fields. More information can be find on [the website](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/) under the section "Dataset schema".

### Installation

- Download the JSON file from https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
- Upload it to a separate folder (`demo_dataset`) on your S3 bucket.

## Historical Hourly Weather Data 2012-2017

[Historical Hourly Weather Data](https://www.kaggle.com/selfishgene/historical-hourly-weather-data) is a dataset collected by a Kaggle competitor. The dataset contains 5 years of hourly measurements data of various weather attributes, such as temperature, humidity, and air pressure. This data is available for 27 bigger US cities, 3 cities in Canada, and 6 cities in Israel. Each attribute has it's own file and is organized such that the rows are the time axis (timestamps), and the columns are the different cities. Additionally, there is a separate file to identify which city belongs to which country.

### Installation

- Download the whole dataset from https://www.kaggle.com/selfishgene/historical-hourly-weather-data/downloads/historical-hourly-weather-data.zip
- Unzip and upload `city_attributes.csv`, `temperature.csv`, and `weather_description.csv` files to a separate folder (`weather_dataset`) on your S3 bucket.