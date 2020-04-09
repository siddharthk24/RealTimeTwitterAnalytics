import boto3
import pandas as pd


def get_kinesis_connection():
    kinesis_object = boto3.client('kinesis', aws_access_key_id='AKIAQJD4QQKXM7LQKOP3',
                                 aws_secret_access_key='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',
                                 region_name='ap-south-1'
                                 )
    return kinesis_object


def send_data_to_stream(kinesis_object):
    
    print (kinesis_object)
    #print(kinesis_object.list_streams())
    productCSV = pd.read_csv('/home/siddharth_k/Downloads/Product.tsv',sep='\t')

    for _, row in productCSV.iterrows():
        values = '|'.join(str(value) for value in row)
        partitioning_key = row.CreatedByName

        kinesis_object.put_record(
            StreamName = 'test-shard',
            Data = values,
            PartitionKey = partitioning_key
        )
    print('success')


def main():
    kinesis_object = get_kinesis_connection()

    send_data_to_stream(kinesis_object)

if __name__ == '__main__':
    main()