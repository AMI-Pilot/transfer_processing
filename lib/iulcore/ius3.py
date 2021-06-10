"""
API for interacting with the Hitachi Content Platform S3 at IU
"""
import boto3
import boto3.session
from botocore.utils import fix_s3_host
from botocore.client import Config
import hashlib
import base64


class IUS3:
    def __init__(self, username, password, hostname, bucket):
        url = f"https://{hostname}/"
        secret = hashlib.md5(password.encode()).hexdigest()
        id = base64.b64encode(username.encode()).decode()        
        config = Config(s3={'addressing_style': 'path',
                            'payload_signing_enabled': 'yes'},
                            signature_version='s3v4')
        session = boto3.session.Session(
            aws_access_key_id=id,
            aws_secret_access_key=secret,
            region_name=None
        )
        self.s3 = session.resource('s3', endpoint_url=url,                            
                                   config=config)
        self.s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)     
        self.bucket = self.s3.Bucket(bucket)


    def list_objects(self, prefix=None):
        """Get objects with a given prefix"""
        for x in self.bucket.objects.filter(Prefix=prefix):
            yield {'bucket_name': x.bucket_name, 
                   'key': x.key,
                   'last_modified': x.last_modified,
                   'e_tag': x.e_tag,
                   'size': x.size}

    def get(self, objectname, handle):
        "get a file using a file-like object"
        self.bucket.download_fileobj(handle, objectname)

    def put(self, objectname, handle):
        "put a file using a file-like object"
        self.bucket.upload_fileobj(handle, objectname)

    def delete(self, objectname):
        "delete an object"
        raise NotImplementedError("This is a preservation project")






