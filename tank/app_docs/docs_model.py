from flask import  jsonify, current_app, make_response

import boto3
import uuid

class DocsModel:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.valid_types = {
            'image/jpeg':'jpg', 
            'image/png':'png', 
            'image/svg+xml':'svg', 
            'application/pdf':'pdf', 
            'text/plain':'txt', 
            'text/csv':'csv',
            'application/json':'json'
        }
 
    
    def a_b_post(self,portfolio, org, ring, raw_doc, type, name):
        
        if not name:
            name = str(uuid.uuid4())
        
        s3_client = boto3.client('s3')
        bucket_name = current_app.config['S3_BUCKET_NAME']  
        filename = f'{name}.{self.valid_types[type]}'
        file_path = f'_docs/{portfolio}/{org}/{ring}/{filename}'
        
        # Determine the content type based on the file type
        content_type = {
            'image/jpeg': 'image/jpeg',
            'image/png': 'image/png',
            'image/svg+xml': 'image/svg+xml',
            'application/pdf': 'application/pdf',
            'application/json':'application/json',
            'text/plain': 'text/plain',
            'text/csv': 'text/csv'
        }.get(type, 'application/octet-stream')  # Default to application/octet-stream if not found

        # Upload to S3 with the specified content type
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=raw_doc,
            ContentType=content_type  # Set the content type here
        )
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        
            result = {}
            result['success'] = True
            result['path'] = file_path 
            result['id'] = name
            
            return result
    
        return {'success': False}
    
    
    def a_b_c_get(self, portfolio, org, ring, filename):
        
        file_path = f'_docs/{portfolio}/{org}/{ring}/{filename}'
    
        s3_client = boto3.client('s3')
        bucket_name = current_app.config['S3_BUCKET_NAME']  
        
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
            document = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            content_type = document['ContentType']  # Get the content type from the response
            
            # Check if content type is binary/octet-stream and set it based on the file extension
            if content_type == 'binary/octet-stream':
                file_extension = filename.split('.')[-1].lower()  # Get the file extension
                content_type = content_type_mapping.get(file_extension, 'application/octet-stream')  # Default to application/octet-stream if not found
            
            current_app.logger.info(f"Content Type: {content_type}")
            content = document['Body'].read()  # Read the content as binary
            
            # Create a response object
            response = make_response(content)
            response.headers.set('Content-Type', content_type)  # Set the correct content type
            
            return {'success': True, 'content': response}  # Return success and content
        
        except s3_client.exceptions.NoSuchKey:
            current_app.logger.error(f"File not found: {file_path}")
            return {'success': False, 'error': 'File not found'}  # Return error object
        except Exception as e:
            current_app.logger.error(f"Error retrieving file: {str(e)}")
            return {'success': False, 'error': 'Error retrieving file'}  # Return error object

        
        
        
        

