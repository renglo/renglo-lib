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
        
        
    
    