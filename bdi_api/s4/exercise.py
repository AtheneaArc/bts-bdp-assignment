from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings
import os
import re
import boto3
import requests
import time
from pathlib import Path

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    #Base url of the ADS-B dataset for the selected day
    base_url = settings.source_url + "/2023/11/01/" 
    #Name of the S3 bucket obtained from enviroment variable BDI_S3_BUCKET
    s3_bucket = settings.s3_bucket
    #Folder inside the bucket where raw files will be stored 
    s3_prefix_path = "raw/day=20231101/"

    #Create AWS S3 client with region to match the bucket region
    s3=boto3.client('s3', region_name="us-east-1")
    #Counter of successfully upload files
    downloaded = 0
    #We generate filenames sequentially every 5 seconds because the dataset url format is "HHMMSSZ"
    seconds = 0

    #Continue until the requested number of files is uploaded
    while downloaded < file_limit:
        #Convert seconds -> hours, minutes, seconds
        hh = seconds // 3600
        mm = (seconds % 3600) // 60
        ss = seconds % 60

        #Construct filename like "000000Z.json.gz"
        filename = f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz"
        #Complete download URL
        file_url = base_url + filename

        print("Downloading:", filename)

        try:
            #Request file from ADS-B Exchange
            r = requests.get(file_url, timeout=20)

            #Some timestamps do not exist -> skip them
            if r.status_code != 200:
                print("Skipping (not found):", filename)
                seconds += 5
                continue
            
            #Save temporarily on disk before uploading
            with open(filename, "wb") as f:
                f.write(r.content)

            print("Uploading to S3:", filename)
            #Upload file to S3 bucket inside raw/day=20231101/
            s3.upload_file(
                Filename=filename,
                Bucket=s3_bucket,
                Key=s3_prefix_path + filename
            )
            #Remove temporary local file
            Path(filename).unlink()
            #Increase counter only if upload was succesful 
            downloaded += 1

        except Exception as e:
            #Network/AWS errors are logged but program continues
            print("Error:", e)
        #Move to next 5-second timestamp
        seconds += 5

    return "OK"



@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    #Create S3 client with the same region as the bucket
    s3=boto3.client("s3",region_name="us-east-1")
    #Bucket name obtained from environment variabel BDI_S3_BUCKET
    bucket=settings.s3_bucket
    #Path inside the bucket where raw files were uploaded
    prefix="raw/day=20231101/"
    #Local directory where raw files will be downloaded
    local_raw_dir=os.path.join(settings.raw_dir,"day=20231101")
    #Create directory if it doesn't exist
    os.makedirs(local_raw_dir,exist_ok=True)
    #List all objects stored in S3 under the raw folder
    response=s3.list_objects_v2(Bucket=bucket,Prefix=prefix)

    #If buckee is empty return message
    if "Contents" not in response:
       return "No files in bucket"
    #Iterate through all objects in S3
    for obj in response["Contents"]:
       #Full path in S3
       key=obj["Key"]
       #Extract filename
       filename=key.split("/")[-1]
        #Local destination path 
       local_path=os.path.join(local_raw_dir,filename)
       print("Downloading from S3:",filename)
       #Download each file from S3 to local disk
       s3.download_file(bucket,key,local_path)
    return "OK"
