from boto3 import client as BotoClient, Session as BotoSession
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from sys import argv


def get_text(bucket_name, object_name):
    textract = BotoClient('textract')

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
    return text


def index_bucket(bucket_name, elasticsearch_host):
    s3_client = BotoClient("s3")
    api_response = s3_client.list_objects_v2(Bucket=bucket_name)
    object_list = api_response.get("Contents")

    file_object_names = [object['Key'] for object in object_list if object['Size'] > 0]
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
            hosts = [{'host': elasticsearch_host, 'port': 443}],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )

        document = {
            "name": "{}".format(object_name),
            "bucket" : "{}".format(bucket_name),
            "content" : text
        }

        elastic_search.index(index="textract", doc_type="document", id=object_name, body=document)

        return "Indexed document: {}".format(object_name)
    else:
        return False


print("S3BucketName ElasticSearchHost")
if argv[1:]:
    print(index_bucket(argv[1], argv[2]))
