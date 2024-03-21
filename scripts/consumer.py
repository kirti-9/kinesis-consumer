import uuid

import boto3
import json
import logging
from botocore.exceptions import ClientError


class KinesisConsumer:
    def __init__(self, stream_name, region_name):
        self.stream_name = stream_name
        self.region_name = region_name
        self.kinesis_client = boto3.client('kinesis', region_name=self.region_name)
        self.logger = logging.getLogger('KinesisConsumer')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def consume_and_transform(self):
        try:
            # Get the shard iterator for the latest record in the stream
            shard_iterator_response = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId='shardId-000000000000',  # Assuming we're using the first shard
                ShardIteratorType='LATEST'
            )
            shard_iterator = shard_iterator_response['ShardIterator']

            while True:
                # Get records from the stream
                records_response = self.kinesis_client.get_records(ShardIterator=shard_iterator)
                records = records_response['Records']

                for record in records:
                    # Decode and parse the record
                    data = json.loads(record['Data'])
                    try:
                        # Transform user_id from string to int
                        data['user']['user_id'] = int(data['user']['user_id'])
                    except ValueError as e:
                        self.logger.error(f"Failed to transform user_id: {e}")

                    # Warehouse the transformed data into S3
                    self.warehouse_data(data)

                # Get the next shard iterator
                shard_iterator = records_response.get('NextShardIterator')

        except ClientError as e:
            self.logger.error(f"An error occurred: {e}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

    def warehouse_data(self, data):
        # Assuming S3 bucket name and key prefix
        s3_bucket = 'aforb-consumer-data'
        key_prefix = 'mandir-event-records/'

        # Serialize the data as JSON
        json_data = json.dumps(data)

        # Upload data to S3
        s3_client = boto3.client('s3')
        try:
            s3_client.put_object(Bucket=s3_bucket, Key=f"{key_prefix}{uuid.uuid4()}.json", Body=json_data)
        except ClientError as e:
            self.logger.error(f"Failed to warehouse data to S3: {e}")


def main():
    stream_name = 'AForB_callevents'
    region_name = 'eu-north-1'

    # Initialize consumer
    consumer = KinesisConsumer(stream_name, region_name)

    # Start consuming and transforming data
    consumer.consume_and_transform()


if __name__ == "__main__":
    main()
