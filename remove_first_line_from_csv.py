# Credit to Eric Fultz from ProductOps @ http://productops.com/

# Requisites include:
# Only tested this with python3. You’ll need `pip3 install boto3 botocore` 
# The script will pull your credentials from the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY 
# This script does not re-compress the file before uploading. Due to low volume of data with Cost and Usage Report, this shouldn't impact performance much.
# This script does not delete the file it creates on your local filesystem

import boto3
import re
import json
import zlib
import io
import time
import uuid

s3 = boto3.client('s3', 'us-west-2')

srcBucketName = 'po-cost-reports2'
srcKeyPrefix = '/efultztest'

dstBucketName = 'po-cost-reports3'
dstKeyPrefix = 'efultztest2/efultztest2'


def get_all_object_keys(prefix=''):
    again=True
    token=None
    while again:
        if token:
            objectsResponse = s3.list_objects_v2(Bucket=srcBucketName, Prefix=prefix, ContinuationToken=token)
        else:
            objectsResponse = s3.list_objects_v2(Bucket=srcBucketName, Prefix=prefix)
        again = objectsResponse['IsTruncated']
        if again:
            token = objectsResponse['ContinuationToken']
        for content in objectsResponse['Contents']:
            yield content['Key']


manifestRe = re.compile('^'+srcKeyPrefix+'/\d{8}-\d{8}/efultztest-Manifest.json$')
def filter_manifests(stuff):
    for el in stuff:
        if(manifestRe.match(el)):
            yield el


manifestKey = sorted(filter_manifests(get_all_object_keys(srcKeyPrefix+'/')), reverse=True)[0]
manifestData = s3.get_object(Bucket=srcBucketName, Key=manifestKey)

def iterable_to_stream(iterable, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """
    Credit goes to 'Mechanical snail' at https://stackoverflow.com/questions/6657820/python-convert-an-iterable-to-a-stream

    Lets you use an iterable (e.g. a generator) that yields bytestrings as a read-only
    input stream.

    The stream implements Python 3's newer I/O API (available in Python 2's io module).
    For efficiency, the stream is buffered.
    """
    class IterStream(io.RawIOBase):
        def __init__(self):
            self.leftover = None

        def readable(self):
            return True

        def readinto(self, b):
            try:
                l = len(b)  # We're supposed to return at most this much
                chunk = self.leftover or next(iterable)
                output, self.leftover = chunk[:l], chunk[l:]
                b[:len(output)] = output
                return len(output)
            except StopIteration:
                return 0    # indicate EOF
    return io.BufferedReader(IterStream(), buffer_size=buffer_size)


def decompress_chunk_generator(stream):
    d = zlib.decompressobj(32+zlib.MAX_WBITS)

    buff = stream.read(1024)
    # print(buff)
    while buff:
        chunk = d.decompress(buff)
        # print(chunk)
        yield chunk
        buff = stream.read(1024)


for reportObjKey in json.load(manifestData['Body'])['reportKeys']:
    d = zlib.decompressobj(zlib.MAX_WBITS)
    reportObj = s3.get_object(Bucket=srcBucketName, Key=reportObjKey)
    stream = reportObj['Body']

    data = iterable_to_stream(decompress_chunk_generator(stream))
    print("First line removed:")
    print(data.readline())
    print("Decompressing file...")

    tmpFilename = str(uuid.uuid1())+".csv"
    tmpFile = open("./"+tmpFilename, "wb")
    for chunk in data:
        tmpFile.write(chunk)
    tmpFile.close()

    destKey = dstKeyPrefix+'/%s/%s'%(time.strftime("%Y/%m/%d"), tmpFilename)
    print("Uploading file " + destKey + " ...")

    putResponse = s3.put_object(Bucket=dstBucketName, Key=destKey, Body=open("./"+tmpFilename, "rb"))
