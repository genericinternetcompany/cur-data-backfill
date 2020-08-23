import pandas as pd
import awswrangler as wr
import boto3
import os

bucketname = "gic-cust-cost-reports"
bucketprefix = "Raw"
localpath = "s3"

s3_client = boto3.client('s3', os.environ.get('aws_access_key_id'), os.environ.get('aws_secret_access_key'), os.environ.get('aws_session_token'))


def download_dir(prefix, local, bucket, client=s3_client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket': bucket,
        'Prefix': prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        print("Starting to Download " + k)
        client.download_file(bucket, k, dest_pathname)
        print("Finished Downloading " + k)


def getdownloadedfiles(path):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv.gz' in file:
                files.append(os.path.join(r, file))

    return files


print("Downloading Files")
download_dir(bucketprefix, localpath, bucketname)
print("Listing Files")
filelist = getdownloadedfiles(localpath)
print("Beginning File Conversion")
for file in filelist:
    print("Reading: " + file)
    df = pd.read_csv(file, compression='gzip', error_bad_lines=False)
    for col in df.columns:
        col2 = col.replace("/", "_")
        col2 = col2.replace(":", "_")
        col2 = col2.replace(" ", "2")
        list1 = list(col2)
        new_list = []
        for i in list1:
            if i.isupper():
                i = "_" + i.lower()
            new_list.append(i)
        col2 = ''.join(new_list)
        col2 = col2.replace("__", "_")
        df.rename(columns={col: col2}, inplace=True)
    print("Converting: " + file)
    os.makedirs(os.path.dirname(file.replace("Raw", "Processed")))
    wr.s3.to_parquet( df=df, path=file.replace("Raw", "Processed"), dataset=True, mode="overwrite")


print("Script Execution Complete")
