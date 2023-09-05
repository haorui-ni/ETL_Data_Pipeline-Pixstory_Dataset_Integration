import pandas as pd
import glob
import csv
import os
import boto3
from io import StringIO


aws_access_key_id = 'AKIA2J6KBGMPYV6EDB3L'
aws_secret_access_key = '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'

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

bucket_name = 'haorui-pixstory'
object_key = '/pixstory_ori.csv'  # Include the desired path within the bucket


def chunk_file(input_file):

    chunk_size = 1000

    for i, chunk in enumerate(pd.read_csv(input_file, chunksize = chunk_size)):
        output_file = f'output_1_{i+1}.csv'
        chunk.to_csv(output_file, index=False, header=True)


def remove_unsupported_chars(s):
    return s.encode('ascii', 'ignore').decode('ascii')

def remove(input, output, col):
    
        df = pd.read_csv(input)  
        df[col] = df[col].str.replace('<.*?>', '', regex=True)
        pattern = r'[<>.&#=+-/@{}()[\]"\'*0-9;\n]'   
        df[col] = df[col].str.replace(pattern, '')
        df[col] = df[col].str.replace('�', '')
        df[col] = df[col].str.replace('\n+', ' ')
        df[col] = df[col].str.replace('.', '')
        df.to_csv(output, index=False)

def combine(directory, output_file):
    
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        header_written = False 
        data = []
        for filename in sorted(os.listdir(directory), key=lambda x: int(x.split('.')[0])):
            if filename.endswith('.csv'):
                with open(os.path.join(directory, filename), 'r') as infile:
                    reader = csv.reader(infile)
                    if not header_written:
                        header_row = next(reader)
                        writer.writerow(header_row)
                        header_written = True
                    else:
                        next(reader, None)
                    for row in reader:
                        row.insert(0, filename.split('.')[0])
                        data.append(row)
        for row in data:
            writer.writerow(row)

def add_pk(input,output):
    df = pd.read_csv(input)
    df.insert(0, "pk_id", range(1, len(df) + 1))
    df.to_csv(output, index=False)

remove("datasets/pixstory.csv", "narrative")
remove("datasets/pixstory.csv", "title")
add_pk("datasets/image_rec.csv")
add_pk("datasets/pixstory.csv")
