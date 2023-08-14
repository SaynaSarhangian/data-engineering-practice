import boto3
import os
import gzip
import shutil
from io import BytesIO

# create a function that initializes a Boto3 session and S3 client using the boto3.Session()
# and boto3.client('s3') calls.
# This session and client can then be used to interact with AWS S3 to download files.
def session_initialize():
    session = boto3.Session()
    s3 = session.client('s3')
    return s3

def download_file_s3(s3_client, bucket, key, filename):
    try:
        #filename = key.split('/')[-1]
        s3_client.download_file(bucket, key, filename)
        print(f'{filename} download completed from {key}')
        return filename

    except Exception as e:
        print(f"Error downloading '{key}' from S3: {e}")


def extract_and_open(filename, extracted_file):
    with gzip.open(filename, 'rb') as file_in:
        with open(extracted_file, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)
            print(f'{extracted_file} created from {filename}')
            return extracted_file


def get_first_line(f):
    with open(f, 'rt', encoding='utf-8') as file:
        # Iterate through each line in the file
        lines = file.readlines()
        first_line = lines[0].strip()
        print(f'the first line of {f} is (key) :{first_line}')
        return first_line


def stream_gzip_from_s3(bucket, key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)

        # Create a GzipFile object to decompress the content
        with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as file:
            for line in file:
                decoded_line = line.decode('utf-8')
                yield decoded_line.strip()  # instead of printing each line directly, the function yields the lines using the yield keyword. its memonry efficient.
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    bucket = 'commoncrawl'
    key1='crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    s3_session = session_initialize()
    # download, extract and save the first file locally.


    file = download_file_s3(s3_session, bucket, key1, key1.split('/')[-1])
    extracted_file = extract_and_open(file, file.replace('.gz', ''))
    key = get_first_line(extracted_file)
    #stream the second file directly from remote .gzip file and print line by line
    file_stream = stream_gzip_from_s3(bucket, key)
    for line in file_stream:
        print(line)

# reference:
# https://www.radishlogic.com/aws/s3/how-to-download-files-from-s3-bucket-using-boto3-and-python/
# key example = 'crawl-data/CC-MAIN-2022-05/segments/1642320306346.64/wet/CC-MAIN-20220128212503-20220129002503-00719.warc.wet.gz'
# URL for downloading a file from commoncrawl using an HTTP download agent: add the full path
# to the prefix https://data.commoncrawl.org/, e.g:
# https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/segments/1642320306346.64/wet/CC-MAIN-20220128212503-20220129002503-00719.warc.wet.gz


# ____________

# CHECK KEY EXISTS IN A GIVEN BUCKET
# def check_s3_key_exists(bucket, key, max_retries=10, base_delay=1):
#     retries = 0
#     s3 = boto3.client('s3')
#     while retries < max_retries:
#         try:
#             response = s3.head_object(Bucket=bucket, Key=key)
#             return True  # Key exists
#         except ClientError as e:
#             if e.response['Error']['Code'] == '404':
#                 return False  # Key does not exist
#             elif 'SlowDown' in str(e):
#                 retries += 1
#                 delay = base_delay * (2 ** retries)
#                 print(f'Retry #{retries}: SlowDown error. Retrying in {delay:.2f} seconds...')
#                 time.sleep(delay)
#             else:
#                 raise  # Something else went wrong
#     return False

# key_exists = check_s3_key_exists('commoncrawl',
#                                 'crawl-data/CC-MAIN-2022-05/segments/1642320306346.64/wet/CC-MAIN-20220128212503-20220129002503-00719.warc.wet.gz')
# if key_exists:
#     print(f'Key "{key}" exists in bucket "{bucket}".')
# else:
#     print(f'Key "{key}" does not exist in bucket "{bucket}".')
