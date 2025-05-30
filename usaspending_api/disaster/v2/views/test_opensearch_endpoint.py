from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3


class OpenSearchTestViewSet(APIView):

    def get(self, request: Request) -> Response:

        region = 'us-gov-west-1'  # Replace with your region
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key,
                        credentials.secret_key,
                        region,
                        service,
                        session_token=credentials.token)
        # Elasticsearch/OpenSearch endpoint (no trailing slash, no http:// if using https)
        host = 'vpc-usaspending-api-sandbox-iajj2p5jmmdvhedupgtx7se62i.us-gov-west-1.es.amazonaws.com'
        # Create client
        es = Elasticsearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        # Test read: get cluster health
        res = es.cluster.health()
        
        return Response(
            {
                "Cluster Health": res
            }
        )