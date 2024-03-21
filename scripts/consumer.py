import uuid
import boto3
import json
import logging
from botocore.exceptions import ClientError
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.aws_config import S3Config, DataStreamConfig

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


class KinesisConsumer:
    def __init__(self, stream_name, region_name):
        self.stream_name = stream_name
        self.region_name = region_name
        self.kinesis_client = boto3.client('kinesis', region_name=self.region_name)
        self.logger = logging.getLogger('KinesisConsumer')

    # consume_and_transform reads records from each shard, transforms and sends it for warehousing.
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
                self.logger.info(f'Processing shard {shard_iterator["ShardIteratorId"]}')
                # Get records from the stream
                records_response = self.kinesis_client.get_records(ShardIterator=shard_iterator)
                records = records_response['Records']

                for record in records:
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

    # warehouse_data uploads the data to s3
    def warehouse_data(self, data):
        config = S3Config()
        # Serialize the data as JSON
        json_data = json.dumps(data)

        # Upload data to S3
        s3_client = boto3.client('s3')
        try:
            self.logger.info(f"Warehousing data to s3 with key {config.key_prefix}{uuid.uuid4()}")
            s3_client.put_object(Bucket=config.s3_bucket, Key=f"{config.key_prefix}{uuid.uuid4()}.json", Body=json_data)
        except ClientError as e:
            self.logger.error(f"Failed to warehouse data to S3: {e}")


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    config = DataStreamConfig()

    # Initialize consumer
    consumer = KinesisConsumer(config.stream_name, config.region)

    # Start consuming and transforming data continuously
    consumer.consume_and_transform()


if __name__ == "__main__":
    main()
