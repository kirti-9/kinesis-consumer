class DataStreamConfig:
    def __init__(self):
        # name of AWS Kinesis DataStream
        self.stream_name = 'AForB_callevents'
        # aws region where the data stream is to be created/read from.
        self.region = 'eu-north-1'


class S3Config:
    def __init__(self):
        # s3 bucket name
        self.s3_bucket = 'aforb-consumer-data'
        # key prefix
        self.key_prefix = 'mandir-event-records/'
