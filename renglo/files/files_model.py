import boto3
import uuid
from datetime import datetime
from renglo.logger import get_logger

class FilesModel:

    # HTTP GETs redirect to a signed S3 URL. Signing is local HMAC; no GetObject.
    PRESIGN_EXPIRES_IN = 3600

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.logger = get_logger()
        self.valid_types = {
            'image/jpeg':'jpg', 
            'image/png':'png', 
            'image/svg+xml':'svg', 
            'application/pdf':'pdf', 
            'text/plain':'txt', 
            'text/csv':'csv',
            'application/json':'json'
        }
 
    
    def _bucket_name(self):
        bucket_name = self.config.get('S3_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME configuration is required")
        return bucket_name

    def _s3_client(self):
        return boto3.client('s3')

    def presign_get(self, file_path, expires_in=None):
        """Return a time-limited S3 GET URL. Does not check that the object exists."""
        if expires_in is None:
            expires_in = self.PRESIGN_EXPIRES_IN
        try:
            url = self._s3_client().generate_presigned_url(
                'get_object',
                Params={'Bucket': self._bucket_name(), 'Key': file_path},
                ExpiresIn=expires_in,
            )
            return {'success': True, 'url': url, 'path': file_path}
        except Exception as e:
            self.logger.error(f"Error presigning {file_path}: {str(e)}")
            return {'success': False, 'error': 'Error generating download URL'}

    def a_b_c_presign(self, portfolio, org, ring, filename, expires_in=None):
        file_path = f'_files/{portfolio}/{org}/{ring}/{filename}'
        return self.presign_get(file_path, expires_in)

    def user_thumbnail_presign(self, handle, expires_in=None):
        return self.presign_get(self.user_thumbnail_key(handle), expires_in)

    def tmp_presign(self, portfolio, org, entity, date, object_id, expires_in=None):
        file_path = f'_tmp/{portfolio}/{org}/{entity}/{date}/{object_id}'
        return self.presign_get(file_path, expires_in)

    def a_b_post(self,portfolio, org, ring, raw_doc, type, name):
        if not name:
            name = str(uuid.uuid4())

        s3_client = boto3.client('s3')
        bucket_name = self._bucket_name()
        filename = f'{name}.{self.valid_types[type]}'
        file_path = f'_files/{portfolio}/{org}/{ring}/{filename}'

        content_type = {
            'image/jpeg': 'image/jpeg',
            'image/png': 'image/png',
            'image/svg+xml': 'image/svg+xml',
            'application/pdf': 'application/pdf',
            'application/json': 'application/json',
            'text/plain': 'text/plain',
            'text/csv': 'text/csv'
        }.get(type, 'application/octet-stream')

        try:
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=file_path,
                Body=raw_doc,
                ContentType=content_type
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return {'success': True, 'path': file_path, 'id': name}
            return {'success': False}
        except Exception as e:
            self.logger.error(f"Error uploading file {file_path}: {str(e)}")
            return {'success': False}

    USER_THUMBNAIL_PREFIX = 'auth/thumbnails'

    def user_thumbnail_key(self, handle: str) -> str:
        return f'{self.USER_THUMBNAIL_PREFIX}/{handle}.png'

    def user_thumbnail_post(self, handle, raw_doc):
        s3_client = boto3.client('s3')
        bucket_name = self._bucket_name()
        file_path = self.user_thumbnail_key(handle)

        try:
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=file_path,
                Body=raw_doc,
                ContentType='image/png',
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return {'success': True, 'path': file_path, 'id': handle}
            return {'success': False}
        except Exception as e:
            self.logger.error(f"Error uploading user thumbnail {file_path}: {str(e)}")
            return {'success': False}

    def user_thumbnail_get(self, handle):
        file_path = self.user_thumbnail_key(handle)
        s3_client = boto3.client('s3')
        bucket_name = self._bucket_name()

        try:
            document = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            content = document['Body'].read()
            return {
                'success': True,
                'content': content,
                'content_type': document.get('ContentType', 'image/png'),
            }
        except s3_client.exceptions.NoSuchKey:
            return {'success': False, 'error': 'File not found'}
        except Exception as e:
            self.logger.error(f"Error retrieving user thumbnail: {str(e)}")
            return {'success': False, 'error': 'Error retrieving file'}
    
    
    def a_b_c_get(self, portfolio, org, ring, filename):
        
        file_path = f'_files/{portfolio}/{org}/{ring}/{filename}'
    
        s3_client = boto3.client('s3')
        bucket_name = self._bucket_name()
        
        # Define a mapping of file extensions to content types
        content_type_mapping = {
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'svg': 'image/svg+xml',
            'pdf': 'application/pdf',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'json': 'application/json'
        }
        
        try:
            print(f's3: Getting Object:{file_path}')
            document = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            content_type = document['ContentType']  # Get the content type from the response
            
            # Check if content type is binary/octet-stream and set it based on the file extension
            if content_type == 'binary/octet-stream':
                file_extension = filename.split('.')[-1].lower()  # Get the file extension
                content_type = content_type_mapping.get(file_extension, 'application/octet-stream')  # Default to application/octet-stream if not found
            
            self.logger.info(f"Content Type: {content_type}")
            content = document['Body'].read()  # Read the content as binary
            return {'success': True, 'content': content, 'content_type': content_type}
        
        except s3_client.exceptions.NoSuchKey:
            #current_app.logger.error(f"File not found: {file_path}")
            return {'success': False, 'error': 'File not found'}  # Return error object
        except Exception as e:
            self.logger.error(f"Error retrieving file: {str(e)}")
            return {'success': False, 'error': 'Error retrieving file'}  # Return error object
        
        
        
        
    def tmp_post(self,portfolio, org, entity, raw_doc):
        
        # Create key from a concatenation of date and sufix 
        date = datetime.now().strftime("%Y-%m-%d")
        object_id = str(uuid.uuid4())

        s3_client = boto3.client('s3')
        bucket_name = self._bucket_name()
        file_path = f'_tmp/{portfolio}/{org}/{entity}/{date}/{object_id}'
        
        print(f'@tmp_post: Saving to {file_path} : {raw_doc}')

        # tmp uploads are JSON (validated in FilesController.tmp_post); do not use
        # undefined `type` here — that accidentally resolved to builtin `type`.
        content_type = 'application/json'

        try:
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=file_path,
                Body=raw_doc,
                ContentType=content_type
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return {
                    'success': True,
                    'path': file_path,
                    'key': f'{portfolio}/{org}/{entity}/{date}/{object_id}',
                }
            return {'success': False}
        except Exception as e:
            self.logger.error(f"Error uploading file {file_path}: {str(e)}")
            return {'success': False}
        
        
        
    def tmp_get(self, portfolio, org, entity, date, object_id):
        
        file_path = f'_tmp/{portfolio}/{org}/{entity}/{date}/{object_id}'
    
        s3_client = boto3.client('s3')
        bucket_name = self._bucket_name()
        
        # Define a mapping of file extensions to content types
        content_type_mapping = {
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'svg': 'image/svg+xml',
            'pdf': 'application/pdf',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'json': 'application/json'
        }
        
        try:
            print(f's3: Getting Object:{file_path}')
            document = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            content_type = document['ContentType']  # Get the content type from the response
            
            # Check if content type is binary/octet-stream and set it based on extension or path
            if content_type == 'binary/octet-stream':
                last = file_path.rsplit('/', 1)[-1]
                if '.' in last:
                    file_extension = last.rsplit('.', 1)[-1].lower()
                    content_type = content_type_mapping.get(
                        file_extension, 'application/octet-stream'
                    )
                else:
                    # tmp objects are stored without extension; treat as JSON
                    content_type = 'application/json'
            
            self.logger.info(f"Content Type: {content_type}")
            content = document['Body'].read()  # Read the content as binary
            return {'success': True, 'content': content, 'content_type': content_type}
        
        except s3_client.exceptions.NoSuchKey:
            #current_app.logger.error(f"File not found: {file_path}")
            return {'success': False, 'error': 'File not found'}  # Return error object
        except Exception as e:
            self.logger.error(f"Error retrieving file: {str(e)}")
            return {'success': False, 'error': 'Error retrieving file'}  # Return error object

        
