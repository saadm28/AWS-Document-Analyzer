import boto3
import pandas as pd
from fuzzywuzzy import fuzz
from sample_list import SAMPLE_LIST
from datetime import datetime
import time
import csv
from dotenv import load_dotenv
import os


# ENTER LOCAL PATH OF PDF FILE HERE
PDF_PATH = 'PDF Files/CMACGM_Ningbo.pdf'

# S3 bucket name
load_dotenv()
BUCKET_NAME = os.getenv('BUCKET_NAME')

# get filename of PDF file from path
# timestamp to make all filenames unique
TIMESTAMP = datetime.now().strftime("%m%d%H%M%S")
FILENAME = f"{TIMESTAMP}_{PDF_PATH.split('/')[-1]}"
CSV_FILE_NAME = f"{FILENAME.split('.')[0]}_extracted.csv"

# folders inside s3 bucket
UPLOAD_FOLDER = 'async-doc-text'  # contains the uploaded pdf files in s3
DOWNLOAD_FOLDER = 'csv'  # contains the csv files in s3 to be downloaded

# CSV folders
INITIAL_CSV_FOLDER = 'UNFILTERED CSV FILES'  # contains files downloaded from s3
FINAL_CSV_FOLDER = 'FINAL CSV FILES'  # contains the filtered csv files

# create s3 client
s3 = boto3.client('s3')


# function to upload pdf to s3 bucket
def upload_pdf_to_s3():
    # upload pdf to s3 bucket
    # function parameters: local file name, bucket_name, s3 file name
    s3.upload_file(PDF_PATH, BUCKET_NAME, UPLOAD_FOLDER + '/' + FILENAME)
    print(f"\n\n{FILENAME} UPLOADED TO S3 BUCKET '{BUCKET_NAME}'")


# function to download pdf from s3 bucket
def download_pdf_from_s3():
    # download pdf from s3 bucket
    # function parameters: bucket_name, s3 file name, local file name
    s3.download_file(BUCKET_NAME, f"{DOWNLOAD_FOLDER}/{CSV_FILE_NAME}",
                     f'{INITIAL_CSV_FOLDER}/{CSV_FILE_NAME}')
    print(f"\n\n{CSV_FILE_NAME} DOWNLOADED FROM S3 BUCKET '{BUCKET_NAME}' TO '{INITIAL_CSV_FOLDER}' FOLDER")


def final_pairs_extraction():
    print("\n\nEXTRACTING RELEVENT KEY VALUE PAIRS...")

    keys = []
    values = []

    with open(f'{INITIAL_CSV_FOLDER}/{CSV_FILE_NAME}', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            keys.append(row['Key'])
            values.append(row['Value'])

    df = pd.DataFrame({'Key': keys, 'Value': values})

    # Find similar keys using fuzzywuzzy
    similar_keys = []
    for str1 in keys:
        for str2 in SAMPLE_LIST:
            # check if the strings are similar
            match_percentage = fuzz.token_set_ratio(str1, str2)
            # 50 is the threshold for similarity (higher the number, more similar the strings)
            if match_percentage > 50:
                # print(f"{str1} is similar to {str2}")
                similar_keys.append(str1)
    final_keys_list = list(set(similar_keys))

    # Create a new dataframe with keys from final_key_list only
    new_df = df[df['Key'].isin(final_keys_list)]

    # print dataframe and number of key value pairs found
    # print(new_df)
    print(f"\n\n{len(final_keys_list)} KEY VALUE PAIRS FOUND")

    # Save the table to a csv file
    formatted_file_name = f"{FILENAME.split('.')[0]}_filtered.csv"
    new_df.to_csv(f'{FINAL_CSV_FOLDER}/{formatted_file_name}', index=False)
    print(
        f"FINAL CSV FILE SAVED TO '{FINAL_CSV_FOLDER}' FOLDER AS '{formatted_file_name}'")


# main function
def main():
    # handle user input
    print("\nConfirm the following information to extract key-value pairs:\n")
    choice = input(
        f"Would you like to upload '{FILENAME}' to S3 bucket '{BUCKET_NAME}'? (Enter 'y' to confirm or any other key to exit): ").lower()

    # Record the start time of the script
    start_time = time.time()

    if choice == 'y':
        # call function to upload pdf to s3 bucket
        upload_pdf_to_s3()
        print("\nLambda function is running...")
        # keep checking and download file after lambda function is done
        # Check if the file is in the bucket every 5 seconds for a minute (12 times)
        while time.time() - start_time < 60:
            try:
                s3.head_object(Bucket=BUCKET_NAME,
                               Key=f'{DOWNLOAD_FOLDER}/{CSV_FILE_NAME}')
                # call function to download pdf from s3 bucket
                download_pdf_from_s3()
                # call function to filter relevent key value pairs and save to csv file
                final_pairs_extraction()
                break
            except:
                print(f'Analyzing document, please wait...')
                time.sleep(5)
        else:
            print(
                f'Timed out after 1 minute. File {CSV_FILE_NAME} not found in S3 bucket {BUCKET_NAME}.')

        # Record the end time of the script
        end_time = time.time()
        COMPLETION_TIME = end_time - start_time
        print(f"\n\nCompleted In: {COMPLETION_TIME:.4f} seconds.")
    else:
        print("\n\nPlease enter your PDF file path in the 'PDF_PATH' variable then run this file again.")


# MAIN FUCNTION CALL
main()
