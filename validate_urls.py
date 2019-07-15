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


# todo: comments on this file
def main():
    # Configures logging package
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    session = boto3.Session(
        aws_access_key_id=config['aws']['aws_access_key_id'],
        aws_secret_access_key=config['aws']['aws_secret_access_key'],
        region_name=config['aws']['region']
    )

    client = session.client('athena', region_name=config['aws']['region'])
    execution = client.start_query_execution(
        QueryString="select distinct url.expanded_url "
                    "from twitter_stream, unnest(entities.urls) as t(url) "
                    "where url.display_url not like 'twitter.com/%' and "
                    "url.expanded_url not in (select url from validated_url) "
                    "limit 50",
        QueryExecutionContext={
            'Database': 'internetscholar'
        },
        ResultConfiguration={
            'OutputLocation': 's3://internetscholar-temp/unvalidated_urls'
        }
    )
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
    elapsed_time = 0

    while elapsed_time <= TIMEOUT and state in ['RUNNING']:
        elapsed_time = elapsed_time + 1
        response = client.get_query_execution(QueryExecutionId=execution_id)

        state = response.get('QueryExecution', {}).get('Status', {}).get('State')
        if state == 'SUCCEEDED':
            s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            filename = re.findall('.*\/(.*)', s3_path)[0]
        elif state != 'FAILED':
            time.sleep(1)

    if state != 'SUCCEEDED':
        logging.info("Error!")
    else:
        s3 = session.resource('s3')
        s3.Bucket('internetscholar-temp').download_file("unvalidated_urls/{}".format(filename),
                                                        "./unvalidated_urls.csv")

        with open('./unvalidated_urls.csv', newline='') as csv_reader:
            with open('./validated_urls.csv', 'w') as csv_writer:
                reader = csv.DictReader(csv_reader)
                writer = csv.DictWriter(
                    csv_writer,
                    fieldnames=['url', 'validated_url', 'status_code', 'content_type', 'content_length', 'created_at'],
                    dialect='unix'
                )
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0'}
                num_records = 0
                for url in reader:
                    num_records = num_records + 1
                    logging.info('%d - %s', num_records, url['expanded_url'])
                    if urlparse(url['expanded_url']).scheme not in ['https', 'http']:
                        record = {'url': url['expanded_url'],
                                  'validated_url': url['expanded_url'],
                                  'status_code': 601,   # Code for scheme different from https and http
                                  'content_length': 0,
                                  'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                  }
                        writer.writerow(record)
                    else:
                        try:
                            r = requests.head(url['expanded_url'], headers=user_agent,
                                              allow_redirects=True, verify=False, timeout=15)
                        except Exception as e:
                            logging.info('Exception! %s', repr(e))
                            record = {'url': url['expanded_url'],
                                      'validated_url': url['expanded_url'],
                                      'status_code': 600,   # Code for exception during URL validation.
                                      'content_length': 0,
                                      'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                      }
                            writer.writerow(record)
                        else:
                            record = {'url': r.url,
                                      'validated_url': r.url,
                                      'status_code': r.status_code,
                                      'content_type': r.headers.get('content-type', ''),
                                      'content_length': r.headers.get('content-length', 0),
                                      'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                                      }
                            writer.writerow(record)

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

        os.remove('./unvalidated_urls.csv')

        logging.info('Going to compress Json file...')
        with open('./validated_urls.csv', 'rb') as input_file:
            with bz2.BZ2File('./validated_urls.csv.bz2', 'wb', compresslevel=9) as output_file:
                copyfileobj(input_file, output_file)
        logging.info('Going to delete temporary Json file...')
        os.remove('./validated_urls.csv')

        # connect to S3 and upload the file
        logging.info('Going to connect to S3...')
        session = boto3.Session(
            aws_access_key_id=config['aws']['aws_access_key_id'],
            aws_secret_access_key=config['aws']['aws_secret_access_key'],
            region_name=config['aws']['region']
        )
        s3 = session.resource('s3')
        filename_s3 = 'validated_url/{}-{}.csv.bz2'.format(
            time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime()), num_records)
        logging.info('Going to upload project Json file to bucket %s as %s...',
                     config['aws']['s3_bucket_raw'],
                     filename_s3)
        s3.Bucket(config['aws']['s3_bucket_raw']).upload_file('./validated_urls.csv.bz2', filename_s3)

        # deletes temporary file
        logging.info('Going to delete temporary bz2 file...')
        os.remove('./validated_urls.csv.bz2')

        s3.Bucket(config['aws']['s3_bucket']).objects.filter(Prefix="validated_url/").delete()

        athena = session.client('athena', region_name=config['aws']['region'])
        athena.start_query_execution(
            QueryString="drop table if exists validated_url",
            QueryExecutionContext={'Database': 'internetscholar'},
            ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/validated_url'}
        )
        athena.start_query_execution(
            QueryString="""
                CREATE TABLE validated_url
                WITH (external_location = 's3://internetscholar/validated_url/',
                      format = 'PARQUET',
                      bucketed_by = ARRAY['url'],
                      bucket_count=1) AS
                select url, validated_url, status_code, content_type, content_length, cast(created_at as timestamp) as created_at
                from (select t.*,
                             row_number() over (partition by url order by created_at) as seqnum
                      from validated_url_raw t
                     ) t
                where seqnum = 1
            """,
            QueryExecutionContext={'Database': 'internetscholar'},
            ResultConfiguration={'OutputLocation': 's3://internetscholar-temp/validated_url'}
        )


if __name__ == '__main__':
    main()