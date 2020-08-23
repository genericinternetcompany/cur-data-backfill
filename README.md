# cur-data-backfill

This project can be used for converting csv CUR Reports into Parquet. Is can handle duplcate columns and name them accordingly. 

## Use Case

Cur Reporting for parquet files goes through a column naming consolidation so Athena can process the columns. The only acceptable characters for database names, table names, and column names are lowercaseletters, numbers, and the underscore character. There is also no posiblility of duplcate fields for obvious reasons.

This scrpt tries to fix this. 


## Usage Instructions

1. Create Ec2 Instance(I used t3a.2xlarge running Amazon Linux 2 AMI)
2. close this repo
3. Store the access key information in environment variables. They will need to have access to the s3 bucket you stored the csv cur reports into:

``` bash
$ sudo amazon-linux-extras install -y python3.8
$ sudo yum install -y git
$ export AWS_ACCESS_KEY_ID="GET FROM CONSOLE"
$ export AWS_SECRET_ACCESS_KEY="GET FROM CONSOLE"
$ export AWS_SESSION_TOKEN="GET FROM SSO CONSOLE, BLANK FOR NON-SSO"
$ git clone https://github.com/genericinternetcompany/cur-data-backfill.git
$ cd cur-data-backfill
$ sudo su
$ pip3.8 install -r requirements.txt
$ python3.8 main.py {bucketnameforcurreport} {bucketprefixforbaseofreport}
```

## Athena Usage


    I used Glue to crawl the new Parquet Files and from there you can use Athena to query or QuickSight to Buiild dashboards. Something I changed in my crawl settings was changing the following setting in the "Configure the crawler's output" screen:

    Grouping behavior for S3 data (optional)
    Check Create a single schema for each S3 path
