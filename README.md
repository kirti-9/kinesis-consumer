# AWS Kinesis Data Streams Producer and Consumer Development

##  Introduction 
This project consists of separate repositories for the producer and consumer components of an AWS Kinesis Data Streams application. This README provides comprehensive instructions for configuring and executing both the producer and consumer, along with an overview of the project architecture.

## Kinesis Producer
Github Repository: https://github.com/kirti-9/kinesis-producer

### Objective 
The Producer continuously sends data to AWS Kinesis Data Stream distributing it equally across 4 different shards. (1 record per second)

### Components 
#### 1. Data Stream Manager 
The data stream manager verifies if the datastream `AForB_callevents` is present. If absent, it initiates data stream creation with 4 shards and waits until it gets provisioned, i.e., StreamStatus = 'ACTIVE'. 

#### 2. Event Record Generator
The event record generator generates event records that the producer pushes into the data stream.

Sample Event Record
> {"event": {"property": {"call_id": "2549aa03-91a9-42c9-b94f-c16c4aeaf7bf", "call_duration": "236", "call_status": "ongoing"}, "super_property": {"source": "mandir", "type": "purchase", "producer": "user", "name": "astrology_session_purchase", "timestamp": "9385135074663"}}, "user": {"user_id": "550", "state": {"coins": "196", "is_logged_in": "false", "language": "fr", "language_mode": "fr", "country_code": "US", "tz": "Europe/London"}, "device_segment": "38"}, "platform": {"version": {"integer": "486", "string": "4.9.5"}, "code": "com.mandir", "type": "iOS"}, "geo_location": null, "device": {"a_id": "elkcj07a1e6dii9srd7hrnsdx54zxoqe3afc", "state": {"is_background": "true", "is_online": "false", "is_playing_music": "true"}, "hardware": {"model_name": "iPhone 12", "brand_name": "Apple", "type": "Mobile"}, "software": {"mobile": {"version": "14", "name": "iOS"}, "web": null}, "ip": {"ipv4": "192.168.231.119", "ipv6": "2401:db00:2110:3004:0:0:0:7076"}, "system_language": "Spanish", "system_id": "nbdrizx006fjuaxh"}, "session": {"number": "3", "id": "xd5f0t9zhktazzxvkucuosx1ud9jjkns"}, "referral": {"user_id": "522", "user_code": "LAY2PBH"}}

#### 3. Producer Script
The producer script operates continuously with a 1-second sleep interval to transmit data into the data stream. The sleep interval is defined to fulfill the objective of pushing data every one second. It retrieves data from the event record generator, creates a partition key using a hash function to ensure even data distribution across all four shards. And then executes the `put_record` operation with the help of data and partition key.

## Kinesis Consumer
Github Repository: https://github.com/kirti-9/kinesis-consumer

### Objective
The Consumer retrieves data from AWS Kinesis Data Streams, transforms it, and then warehouses it in an S3 bucket. 

### Consumer Script
The consumer script reads a shard iterator, presuming the use of the first shard. It processes each shard iterator, reads records, performs data transformation (converting user_id from string to int), and warehouses the data into an S3 bucket `aforb-consumer-data` with key_prefix `mandir-event-records/`. 

## Setup and Execution Instructions
> [!Note]
> We've used the region `eu-north-1` for this project. If you're using any other region, make sure to change it in the code config.
1. **Create IAM Role:** Create an IAM role with the following policies: 

    - For AWS Kinesis Access
      > {"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "VisualEditor0",
			"Effect": "Allow",
			"Action": [
				"kinesis:PutRecord",
				"kinesis:CreateStream",
				"kinesis:GetShardIterator",
				"kinesis:GetRecords",
				"kinesis:DescribeStream"
			],
			"Resource": "arn:aws:kinesis:eu-north-1:076957296148:stream/AForB_callevents"
		}
	]
}

    - For Amazon S3 Access
      > {
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "VisualEditor0",
			"Effect": "Allow",
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::*/*"
		}
	]
}
3. **Launch EC2 Instance:** Deploy an EC2 instance and associate the IAM role with it.
4. **Create S3 Bucket:** Create an S3 bucket named `aforb-consumer-data`.
5. **Clone Repositories:** Clone both the kinesis-producer and kinesis-consumer repositories onto the EC2 instance.
6. **Run Producer Script:** Execute the producer script in one instance tab: 
    | python3 kinesis-producer/scripts/producer.py.
7. **Run Consumer Script:** Execute the consumer script in another instance tab: 
    | python3 kinesis-consumer/scripts/consumer.py.
8. **Monitor Logs:** Observe the logs in the terminal for both producer and consumer scripts.
9. **Verify Results:** Inspect the AWS Kinesis Data Stream data viewer to witness data population in each shard. Additionally, examine the S3 bucket for created objects under the prefix `mandir-event-records/`, and confirm the conversion of user_id from string to int in the JSON files.

## Architecture 

### High Level Design 
![High Level Design](https://github.com/kirti-9/kinesis-consumer/assets/136445435/e89e100d-d9b6-40ca-b5bb-7eece8fcd0e8)

### Low Level Design
![Untitled Diagram drawio (1)](https://github.com/kirti-9/kinesis-consumer/assets/136445435/e25e93d6-f225-4d35-9879-3d4404068004)

## Screenshots

### AWS Kinesis Data Stream
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/5fe2f9bf-142a-41f8-bc6a-97d234453806)

### Records in Shard 1
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/832dc324-11ea-469e-997d-e8afe621fa75)

### Records in Shard 2
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/2b3a3fad-6c15-4d97-84e3-abb861aec5b6)

### An event record in Shard
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/fad20e36-cff7-42a5-8b84-241712d320ee)

### Objects Populated in S3 Bucket
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/33e3fa1a-b5f0-4fae-a3fa-6a9d18bc7d88)

### An event record in JSON from S3 
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/37e25b55-f7f2-442c-8037-7a50743d4932)

### Producer Logs
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/a023b1a4-8b39-49e7-a460-58b1caa6fab8)


### Consumer Logs
![image](https://github.com/kirti-9/kinesis-consumer/assets/136445435/13d6e703-cb21-4207-85b2-00fb6e417ded)




