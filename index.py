###
# index-images
# based on https://aws.amazon.com/blogs/machine-learning/automatically-extract-text-and-structured-data-from-documents-with-amazon-textract/
# pip3 install elasticsearch==7.13.4


from boto3 import client as BotoClient, Session as BotoSession
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from sys import argv


def get_text(bucket_name, object_name, debug=True):
    textract = BotoClient('textract')

    if debug:
        print("Detecting text of " + object_name)

    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_name
            }
        })

    text = ""
    for item in response["Blocks"]:
        if item["BlockType"] == "LINE":
            text += item["Text"]
    if debug:
        print(text)
    return text


def index_bucket(bucket_name, elasticsearch_host, debug=True):
    s3_client = BotoClient("s3")
    paginator = s3_client.get_paginator('list_objects_v2')
    result_pages = paginator.paginate(Bucket=bucket_name)

    file_object_names = []
    for page in result_pages:
        for s3_object in page['Contents']:
            if s3_object['Size'] > 0:
                file_object_names.append(s3_object['Key'])

    if debug:
        print("Detected " + str(len(file_object_names)) + " objects for indexing.")

    return [
        index_document(
            bucket_name, elasticsearch_host, file_object_name, get_text(bucket_name, file_object_name)
        )
        for file_object_name in file_object_names
    ]


def index_document(bucket_name, elasticsearch_host, object_name, text):
    if text:
        boto_session = BotoSession()
        credentials = boto_session.get_credentials()
        region = boto_session.region_name

        awsauth = AWS4Auth(
            credentials.access_key, credentials.secret_key,
            region, 'es', session_token=credentials.token
        )

        elastic_search = Elasticsearch(
            hosts=[{'host': elasticsearch_host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        document = {
            "name": "{}".format(object_name),
            "bucket": "{}".format(bucket_name),
            "content": text
        }

        elastic_search.index(index="textract", doc_type="document", id=object_name, body=document)

        return "Indexed document: {}".format(object_name)
    else:
        return False


print("S3BucketName ElasticSearchHost")
if argv[1:]:
    print(index_bucket(argv[1], argv[2]))
