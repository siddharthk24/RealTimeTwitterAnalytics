import boto3
import pandas as pd
import kinesis_producer
import sys
import os
import pathlib
import settings

def get_kinesis_connection():
    kinesis_object = boto3.client('kinesis', aws_access_key_id=settings.access_key_id,
                                  aws_secret_access_key=settings.aws_secret_access_key,
                                  region_name='ap-south-1'
                                  )
    return kinesis_object


def send_data_to_stream(kinesis_object):
    print(kinesis_object)
    # print(kinesis_object.list_streams())
    productCSV = pd.read_csv('/home/siddharth_k/Downloads/Product.tsv', sep='\t')

    for _, row in productCSV.iterrows():
        values = '|'.join(str(value) for value in row)
        partitioning_key = row.CreatedByName

        kinesis_object.put_record(
            StreamName='test-shard',
            Data=values,
            PartitionKey=partitioning_key
        )
    print('success')


def main():

     kinesis_object = get_kinesis_connection()

     send_data_to_stream(kinesis_object)


if __name__ == '__main__':
    main()
