import logging
import os
import configparser
import boto3
import time
import json
import re
import bz2
from shutil import copyfileobj


class InternetScholar:
    ATHENA_TIMEOUT = 15

    def _init_(self, prefix):
        self.prefix = prefix

        # Configure logging module to save on log file and present messages on the screen too
        log_directory = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_directory, exist_ok=True)
        logging.basicConfig(filename=os.path.join(log_directory, '{}.log'.format(self.prefix)),
                            filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        logging.getLogger().addHandler(console)

        # Access configuration file
        conf_file = os.path.join(os.path.dirname(__file__), 'config.ini')
        logging.info('Configuration file: %s', conf_file)
        self.config = configparser.ConfigParser()
        self.config.read(conf_file)

        # Initialize AWS session that will be used to access S3 and Athena
        logging.info('Boto session at %s', self.config['aws']['region'])
        self.session = boto3.Session(
            aws_access_key_id=self.config['aws']['aws_access_key_id'],
            aws_secret_access_key=self.config['aws']['aws_secret_access_key'],
            region_name=self.config['aws']['region']
        )

        # Connect to Athena
        logging.info('Create client for Athena')
        self.athena = self.session.client('athena', region_name=self.config['aws']['region'])

        self.s3 = self.session.resource('s3', region_name=self.config['aws']['region'])

        # Directory that will be used for temporary files
        self.temp_dir = os.path.join(os.path.dirname(__file__), 'temp')
        os.makedirs(self.temp_dir, exist_ok=True)
        logging.info('Directory for temporary files: %s', self.temp_dir)

        # copy content of config file to variables just to make code more legible
        self.s3_temp = self.config['aws']['s3_bucket_temp']
        self.s3_official = self.config['aws']['s3_bucket']
        self.s3_raw = self.config['aws']['s3_bucket_raw']
        self.s3_temp_full = "s3://{}/{}/".format(self.s3_temp, prefix)
        self.s3_official_full = "s3://{}/{}/".format(self.s3_official, prefix)
        self.s3_raw_full = "s3://{}/{}/".format(self.s3_raw, prefix)
        self.athena_db = self.config['aws']['athena_database']

    def query_athena(self, query_string):
        logging.info("Query to Athena database '%s'. Query string: %s", self.athena_db, query_string)
        execution = self.athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': self.athena_db},
            ResultConfiguration={'OutputLocation': self.s3_temp_full})
        execution_id = execution['QueryExecutionId']
        logging.info("Execution ID: %s", execution_id)
        return execution_id

    def query_athena_and_wait(self, query_string):
        execution_id = self.query_athena(query_string)

        # Wait until query ends or timeout
        state = 'RUNNING'
        elapsed_time = 0
        response = None
        while elapsed_time <= self.ATHENA_TIMEOUT and state in ['RUNNING']:
            elapsed_time = elapsed_time + 1
            response = self.athena.get_query_execution(QueryExecutionId=execution_id)
            state = response.get('QueryExecution', {}).get('Status', {}).get('State')
            if state not in ['SUCCEEDED', 'FAILED']:
                logging.info("Waiting for response: sleep for 1 second")
                time.sleep(1)

        # if timeout or failed
        if state != 'SUCCEEDED':
            logging.error("Error executing query. Current state: '%s', Response: %s", state, json.dumps(response))
            raise Exception("Error executing query. Read log to see Athena's response.")
        else:
            # obtain file name
            logging.info("Query succeeded: %s", json.dumps(response))
            s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            filename_s3 = re.findall('.*/(.*)', s3_path)[0]
            logging.info("Filename on S3: %s", filename_s3)
            return filename_s3

    def query_athena_and_download(self, query_string, filename):
        filename_s3 = self.query_athena_and_wait(query_string)
        filepath_s3 = "{}/{}".format(self.prefix, filename_s3)
        local_filepath = os.path.join(self.temp_dir, filename)
        logging.info("Download file '%s' from bucket %s. Local path: '%s'", filepath_s3, self.s3_temp, local_filepath)
        self.s3.Bucket(self.s3_temp).download_file(filepath_s3, local_filepath)
        logging.info("Clean all files on bucket %s at prefix %s", self.s3_temp, self.prefix)
        self.s3.Bucket(self.s3_temp).objects.filter(Prefix="{}/".format(self.prefix)).delete()
        return local_filepath

    @staticmethod
    def compress(filename, delete_original=False, compresslevel=9):
        filename_bz2 = "{}.bz2".format(filename)
        logging.info("Compress file %s. New file: %s. Compression level: %d. Delete original? %s",
                     filename, filename_bz2, compresslevel, delete_original)
        with open(filename, 'rb') as input_file:
            with bz2.BZ2File(filename_bz2, 'wb', compresslevel=compresslevel) as output_file:
                copyfileobj(input_file, output_file)
        if delete_original:
            os.remove(filename)
        return filename_bz2

    def upload_raw_file(self, local_filename, s3_filename, delete_original=False):
        compressed_file = self.compress(local_filename, delete_original=delete_original)
        logging.info("Upload raw file to bucket '%s'. Local filename: %s. S3 filename: %s. Delete original? %s",
                     self.s3_raw, local_filename, s3_filename, delete_original)
        self.s3.Bucket(self.s3_raw).upload_file(compressed_file, s3_filename)

    @staticmethod
    def prepare(text, placeholder=None):
        text = ' '.join(text.split())
        if placeholder is not None:
            text = text.format(placeholder)
        return text
