#### Data acquisition

- Download [Yelp Open Dataset](https://www.yelp.com/dataset) and upload to your S3 bucket.
- Another, faster option: 
    - Launch an EC2 instance with at minimum 20GB SSD storage.
    - Connect to this instance via SSH (click "Connect" and proceed according to instructions)
    - Proceed to https://www.yelp.com/dataset, fill in your information, copy the download link (you've got 30 seconds for this), and paste into the command below.

```bash
wget -O yelp_dataset.tar.gz "[download_link]"
tar -xvzf yelp_dataset.tar.gz
```
    - Finally, transfer the files as described by [this blog](http://codeomitted.com/transfer-files-from-ec2-to-s3/)
    - In case your instance has no AWS CLI installed, follow [this documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
    - In case you come into errors such as "Unable to locate package python3-pip", follow [this answer](https://askubuntu.com/questions/1061486/unable-to-locate-package-python-pip-when-trying-to-install-from-fresh-18-04-in#answer-1061488)
    - Remember to provide IAM role and credentials of user who has AmazonS3FullAccess.
- Download [US Cities: Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
- Download [Historical Hourly Weather Data 2012-2017](https://www.kaggle.com/selfishgene/historical-hourly-weather-data/downloads/historical-hourly-weather-data.zip/2#weather_description.csv)
- Upload those complementary datasets to the S3 bucket.
- Ensure that the bucket is in the same region as your Amazon EMR and Redshift instances.

#### ETL

- To configure Amazon EMR to run a PySpark job using Python 3.6, follow [these instructions](https://aws.amazon.com/premiumsupport/knowledge-center/emr-pyspark-python-3x/)
- Rule of thumb: Use generated keys for entities and composite keys for relationships.
- Use AWS Glue to derive schema automatically or help with schema definition in Amazon Redshift.
- [Documentation for Yelp Open Dataset](https://www.yelp.com/dataset/documentation/main)
- [Yelp's Academic Dataset Examples](https://github.com/Yelp/dataset-examples)
- [Spark Tips & Tricks](https://gist.github.com/dusenberrymw/30cebf98263fae206ea0ffd2cb155813)
