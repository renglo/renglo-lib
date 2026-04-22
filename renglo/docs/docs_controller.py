import json

from flask import current_app, jsonify
from renglo.docs.docs_model import DocsModel

class DocsController:

    def __init__(self, config=None, tid=None, ip=None):
        self.config = config or {}
        self.DCM = DocsModel(config=self.config)
        
        self.valid_types = {
            'image/jpeg':'jpg', 
            'image/png':'png', 
            'image/svg+xml':'svg', 
            'application/pdf':'pdf', 
            'application/json':'json', 
            'text/plain':'txt', 
            'text/csv':'csv'
        }

    @staticmethod
    def _file_contents_is_valid_json(file):
        """Return True if *file* decodes as UTF-8 and parses as JSON. Rewinds file-like objects after read."""
        try:
            if isinstance(file, (bytes, bytearray)):
                payload = bytes(file)
            elif isinstance(file, str):
                payload = file.encode("utf-8")
            else:
                payload = file.read()
                if hasattr(file, "seek"):
                    file.seek(0)
            json.loads(payload.decode("utf-8"))
            return True
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError, OSError, ValueError):
            return False

    def a_b_post(self,portfolio,org,ring,file,type,name):
        
        # file needs to come in binary format already
        current_app.logger.info("Uploading a DOC")
        if file:    
            if type in self.valid_types:
                # Further verification logic can be added here
                current_app.logger.info("File type is valid.")
                
                #response = upload_doc_to_s3(portfolio,org,ring,raw_content,up_file_type) 
                response = self.DCM.a_b_post(portfolio,org,ring,file,type,name)  
                
                if response['success']:    
                    return response 
                else:
                    return {'success':False, 'message':response}
                
            else:
                current_app.logger.warning("Invalid file type received.")
                return {'success':False, 'message':'Invalid file type'}
            
        return {'success':False, 'message':'No file'}
    
    
    def a_b_c_get(self,portfolio,org,ring,filename):
        
        response = self.DCM.a_b_c_get(portfolio,org,ring,filename)
        
        return response
    
    
    def tmp_post(self,portfolio,org,entity,file):
        
        #Uploading to /tmp space
        current_app.logger.info("Uploading a doc to transient storage")
        if file:    
            if self._file_contents_is_valid_json(file):
                current_app.logger.info("Upload body is valid JSON.")
                
                #response = upload_doc_to_s3(portfolio,org,ring,raw_content,up_file_type) 
                
                response = self.DCM.tmp_post(portfolio, org, entity, file)
                
                print(f'tmp_post response: {response}')
                
                if response['success']:    
                    return response 
                else:
                    return {'success':False, 'message':response}
                
            else:
                current_app.logger.warning("Upload body is not valid JSON.")
                return {'success':False, 'message':'Invalid JSON'}
            
        return {'success':False, 'message':'No file'}
    
    
    def tmp_get(self,portfolio,org,entity,date,id):
        
        #Getting from /tmp space
        current_app.logger.info("Retrieving a doc from transient storage")
        
        response = self.DCM.tmp_get(portfolio,org,entity,date,id)
        
        return response
        
        
        
        
        
    
    