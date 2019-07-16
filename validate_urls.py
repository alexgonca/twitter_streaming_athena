import boto3
from datetime import datetime
import re
import configparser
import os
import requests
from urllib.parse import urlparse
import csv
import logging
import bz2
from shutil import copyfileobj
import urllib3
import time


TIMEOUT = 5


def main():
    # Configure logging module to save on log file and present messages on the screen too
    log_directory = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_directory, exist_ok=True)
    logging.basicConfig(filename=os.path.join(log_directory, 'urls.log'),
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    # Directory that will be used for temporary files
    temp_dir = os.path.join(os.path.dirname(__file__), 'temp')
    logging.info('Directory for temporary files: %s', temp_dir)

    # Access configuration file
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    logging.info('Accessed configuration file')

    # Initialize AWS session that will be used to access S3 and Athena
    session = boto3.Session(
        aws_access_key_id=config['aws']['aws_access_key_id'],
        aws_secret_access_key=config['aws']['aws_secret_access_key'],
        region_name=config['aws']['region']
    )
    logging.info('Created a session for AWS.')

    # Connect to Athena
    athena = session.client('athena', region_name=config['aws']['region'])

    # Create a dummy table in case a real one does not exist in order to allow the following SELECT statement
    query_string = """
            CREATE EXTERNAL TABLE IF NOT EXISTS validated_url (url string)
                LOCATION 's3://internetscholar-temp/validated_url/'      
        """.replace('s3://internetscholar-temp/validated_url/',
                    "s3://{}/validated_url/".format(config['aws']['s3_bucket_temp']))
    execution = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/validated_url'.format(config['aws']['s3_bucket_temp'])}
    )
    execution_id = execution['QueryExecutionId']
    logging.info('Sent query to Athena to create dummy table if not exists')

    # Wait until query ends or timeout
    state = 'RUNNING'
    elapsed_time = 0
    while elapsed_time <= TIMEOUT and state in ['RUNNING']:
        elapsed_time = elapsed_time + 1
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        state = response.get('QueryExecution', {}).get('Status', {}).get('State')
        if state not in ['SUCCEEDED', 'FAILED']:
            time.sleep(1)

    # Query URLs that have not been validated yet
    execution = athena.start_query_execution(
        QueryString="""
                select distinct url.expanded_url
                from twitter_stream, unnest(entities.urls) as t(url)
                where url.display_url not like 'twitter.com/%' and
                url.expanded_url not in (select url from validated_url)
        """,
        QueryExecutionContext={'Database': config['aws']['athena_database']},
        ResultConfiguration={'OutputLocation': 's3://{}/validated_url'.format(config['aws']['s3_bucket_temp'])}
    )
    execution_id = execution['QueryExecutionId']
    logging.info('Sent query to Athena')

    # Wait until query ends or timeout
    state = 'RUNNING'
    elapsed_time = 0
    response = None
    while elapsed_time <= TIMEOUT and state in ['RUNNING']:
        elapsed_time = elapsed_time + 1
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        state = response.get('QueryExecution', {}).get('Status', {}).get('State')
        if state not in ['SUCCEEDED', 'FAILED']:
            time.sleep(1)

    # if timeout or failed, log the error
    if state != 'SUCCEEDED':
        logging.info("Error! %s", state)
    # otherwise download file and process it
    else:
        logging.info('Query succeeded')

        # download result file
        s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        filename_s3 = re.findall('.*/(.*)', s3_path)[0]
        unvalidated_file_s3 = "validated_url/{}".format(filename_s3)
        unvalidated_file = os.path.join(temp_dir, 'unvalidated_urls.csv')
        os.makedirs(temp_dir, exist_ok=True)
        s3 = session.resource('s3')
        s3.Bucket(config['aws']['s3_bucket_temp']).download_file(unvalidated_file_s3, unvalidated_file)
        logging.info('Downloaded result file from S3')

        # delete result file on S3
        s3.Bucket(config['aws']['s3_bucket_temp']).objects.filter(Prefix="validated_url/").delete()
        logging.info('Deleted result file on S3')

        # Open both the file with unvalidated URLs (read) and the one for validated URLs (write)
        validated_file = os.path.join(temp_dir, 'validated_url.csv')
        with open(unvalidated_file, newline='') as csv_reader:
            with open(validated_file, 'w') as csv_writer:
                reader = csv.DictReader(csv_reader)
                writer = csv.DictWriter(
                    csv_writer,
                    fieldnames=['url', 'validated_url', 'status_code', 'content_type', 'content_length', 'created_at'],
                    dialect='unix'
                )
                # to avoid unnecessary warnings on the log file
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                # requests will be sent with a "real" user agent to avoid denial of response
                user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0'}
                # a counter of the name of records to be used in the S3 identifier for this file
                num_records = 0
                # for each one of the unvalidated URLs...
                for url in reader:
                    num_records = num_records + 1
                    logging.info('%d - %s', num_records, url['expanded_url'])
                    # if protocol is not HTTP or HTTPS, no need to get it
                    if urlparse(url['expanded_url']).scheme not in ['https', 'http']:
                        record = {'url': url['expanded_url'],
                                  'validated_url': url['expanded_url'],
                                  'status_code': 601,   # Code for scheme different from https and http
                                  'content_length': 0,
                                  'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                  }
                        writer.writerow(record)
                    else:
                        # tries to get the URL information
                        try:
                            # uses head instead of get to avoid downloading the whole file
                            r = requests.head(url['expanded_url'], headers=user_agent,
                                              allow_redirects=True, verify=False, timeout=15)
                        except Exception as e:
                            # if anything goes wrong, log the error
                            logging.info('Exception! %s', repr(e))
                            record = {'url': url['expanded_url'],
                                      'validated_url': url['expanded_url'],
                                      'status_code': 600,   # Code for exception during URL validation.
                                      'content_length': 0,
                                      'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                      }
                            writer.writerow(record)
                        else:
                            # otherwise, save the URL...
                            record = {'url': r.url,
                                      'validated_url': r.url,
                                      'status_code': r.status_code,
                                      'content_type': r.headers.get('content-type', ''),
                                      'content_length': r.headers.get('content-length', 0),
                                      'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                      }
                            writer.writerow(record)

                            # ...and its history of re-directions
                            if len(r.history) != 0:
                                for history_element in r.history:
                                    record = {'url': history_element.url,
                                              'validated_url': r.url,
                                              'status_code': history_element.status_code,
                                              'content_type': history_element.headers.get('content-type', ''),
                                              'content_length': history_element.headers.get('content-length', 0),
                                              'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                              }
                                    writer.writerow(record)

        os.remove(unvalidated_file)
        logging.info('Deleted %s', unvalidated_file)

        # compresses validated URLs to send them to S3
        compressed_file = os.path.join(temp_dir, 'validated_url.csv.bz2')
        with open(validated_file, 'rb') as input_file:
            with bz2.BZ2File(compressed_file, 'wb', compresslevel=9) as output_file:
                copyfileobj(input_file, output_file)
        logging.info('New compressed file %s', compressed_file)

        # remove uncompressed file with validated URLs
        os.remove(validated_file)
        logging.info('Deleted %s', validated_file)

        # upload compressed file with validated URLs to S3
        filename_s3 = 'validated_url/{}-{}.csv.bz2'.format(
            time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime()), num_records)
        s3.Bucket(config['aws']['s3_bucket_raw']).upload_file(compressed_file, filename_s3)
        logging.info('Uploaded file %s to bucket %s as %s',
                     compressed_file,
                     config['aws']['s3_bucket_raw'],
                     filename_s3)

        # deletes temporary compressed file
        os.remove(compressed_file)
        logging.info('Deleted %s', compressed_file)

        # create a table for raw data in case one does not exist
        athena.start_query_execution(
            QueryString="""
                CREATE EXTERNAL TABLE IF NOT EXISTS validated_url_raw (
                url string,
                validated_url string,
                status_code string,
                content_type string,
                content_length string,
                created_at string)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                WITH SERDEPROPERTIES ("separatorChar"=",", "escapeChar" = "\\\\")
                LOCATION 's3://internetscholar-raw/validated_url/'
            """.replace('s3://internetscholar-raw/validated_url/',
                        "s3://{}/validated_url/".format(config['aws']['s3_bucket_raw'])),
            QueryExecutionContext={'Database': config['aws']['athena_database']},
            ResultConfiguration={'OutputLocation': 's3://{}/validated_url'.format(config['aws']['s3_bucket_temp'])}
        )
        logging.info('Created new raw table of validated URLs on Athena')

        # in order to created a table with validated URLs through CTAS, we need to empty the prefix (folder)
        s3.Bucket(config['aws']['s3_bucket']).objects.filter(Prefix="validated_url/").delete()
        logging.info('Emptied prefix (folder) where table of validated URLs will be stored')

        # queue a command to drop the existing validated_url table on Athena
        athena.start_query_execution(
            QueryString="drop table if exists validated_url",
            QueryExecutionContext={'Database': config['aws']['athena_database']},
            ResultConfiguration={'OutputLocation': 's3://{}/validated_url'.format(config['aws']['s3_bucket_temp'])}
        )
        logging.info('Deleted existing table of validated URLs on Athena')

        # queue a command to CTAS a new validated_url table on Athena
        # the unusual select structure eliminates duplicated rows
        athena.start_query_execution(
            QueryString="""
                CREATE TABLE validated_url
                WITH (external_location = 's3://internetscholar/validated_url/',
                      format = 'PARQUET',
                      bucketed_by = ARRAY['url'],
                      bucket_count=1) AS
                select url, validated_url, cast(trim(status_code) as integer) as status_code,
                       content_type, cast(trim(content_length) as integer) as content_length,
                       cast(trim(created_at) as timestamp) as created_at
                from (select t.*,
                             row_number() over (partition by url order by created_at) as seqnum
                      from validated_url_raw t
                     ) t
                where seqnum = 1
            """.replace('s3://internetscholar/validated_url/',
                        "s3://{}/validated_url/".format(config['aws']['s3_bucket'])),
            QueryExecutionContext={'Database': config['aws']['athena_database']},
            ResultConfiguration={'OutputLocation': 's3://{}/validated_url'.format(config['aws']['s3_bucket_temp'])}
        )
        logging.info('Created new table of validated URLs on Athena')

        # delete result files on S3 (just a log of the previous commands)
        s3.Bucket(config['aws']['s3_bucket_temp']).objects.filter(Prefix="validated_url/").delete()
        logging.info('Deleted result file on S3 for CTAS commands')


if __name__ == '__main__':
    main()
