from flask import current_app

from renglo.data.data_controller import DataController
from renglo.docs.docs_controller import DocsController
from renglo.blueprint.blueprint_controller import BlueprintController

from renglo.schd.schd_loader import SchdLoader
from renglo.schd.schd_model import SchdModel

from datetime import datetime

import json

class SchdController:

    def __init__(self, config=None):
        self.config = config or {}
        self.DAC = DataController(config=self.config)
        self.DCC = DocsController(config=self.config)
        self.BPC = BlueprintController(config=self.config)
        self.SHM = SchdModel(config=self.config)
        self.SHL = SchdLoader()
        


    
    
    
    def find_rule(self,portfolio,org,timer):
        
        rule_name = "cron_"+portfolio+"_"+org+"_"+timer        
        result = self.SHM.find_rule(rule_name)
        
        return result
        
    
    def create_rule(self,portfolio,org,name,schedule_expression,payload):
        '''
        Function used to create the cronjob
        '''
        
        rule_name = "cron_"+portfolio+"_"+org+"_"+name

        result = self.SHM.create_https_target_event(
            rule_name=rule_name,
            schedule_expression=schedule_expression,
            payload=payload
        )

        
        return result
    

    def remove_rule(self,portfolio,org,name):
        '''
        Function used to create the cronjob
        '''
        
        rule_name = "cron_"+portfolio+"_"+org+"_"+name
        
        result = self.SHM.delete_https_target_event(rule_name)
        
        return result
        
        
    def verify_rule(self,portfolio,org,timer):
        
        rule_name = "cronjob_"+portfolio+"_"+org+"_"+timer        
        result = self.SHM.find_rule(rule_name)
        
        return result
    
   
    # COMPLETE  
    def create_job_run(self,portfolio,org,payload):
        '''
        Function that is called by the cronjob 
        '''
        
        current_app.logger.debug('Action: create_job_run:')
        
        result = []
        action = 'create_job_run'
        #1. Check if the job exists. Get the document
        if 'schd_jobs_id' not in payload:
            return {'success': False,'action':action,'input':payload,'message': 'No Job Id'}, 400  
        else:
            response_1 = self.DAC.get_a_b_c(portfolio,org,'schd_jobs',payload['schd_jobs_id'])
            if 'error' not in response_1:
                jobdoc = response_1
            else:
                result.append(response_1)
                return {'success': False,'action':'get_job_document' ,'message': 'Error getting job','input':payload,'output':response_1}, 400 
        
        result.append({'success':True,'action':action,'input':payload,'output':response_1})      
        current_app.logger.debug('Job document check:',response_1)
        
        #2. Create the schd_runs document 
        action = 'create_run'  
        # Check that payload['trigger'] is one of these: [manual, call, cron]
        if payload.get('trigger') not in ['manual', 'call', 'cron']:
            result.append({'success': False,'action':action,'input':payload,'message': 'Invalid trigger value'})
            return result, 400  
        
        if 'author' not in payload:
            payload['author'] = ''
        
        payload['status'] = 'new'
        payload['time_queued'] = str(int(datetime.now().timestamp()))
        payload['time_executed'] = '.'
        payload['output'] = '.'
        
        response_2, status = self.DAC.post_a_b(portfolio,org,'schd_runs',payload)
        
        #{'success': True, 'message': 'Item saved', 'path': '160c4e266ea3/5e7f29c29084/schd_runs/f15c58e4-aa2d-4780-8616-bd1834ca777c'}
        
        current_app.logger.debug('Create the schd_runs document:')
        current_app.logger.debug(response_2)
        result.append({'success':True,'action':action,'input':payload,'output':response_2})
        
        
        #3. Run  the handler indicated in the schd_jobs document
            #NOTICE: This indicates a Synchronous process which is not ideal
            # Ideally, this route should only store the schd_runs document and an asychronous process
            # should pick up and execute one at a time (or in parallel if you use many workers).
            
            
            
        action = 'call_handler'
        
        response_3 = {'success':False,'output':[]}
        
        
        if not 'handler' in jobdoc:
            
            result.append({'success':False,'action':action,'handler':'','message':'No handler in the job document'})
            return result, 400
        
        else:
            
            handler_name = jobdoc['handler']  
        
            # You could send anything coming in the payload
            handler_input_data = {'portfolio': portfolio,'org':org,'handler':handler_name}
            response_3 = self.SHL.load_and_run(handler_name, payload = handler_input_data)
             
            #current_app.logger.debug(f'Handler output:{response_3}')
            
            
            if not response_3['success']:
                status = 400
                result.append({'success':False,'action':action,'handler':handler_name,'input':handler_input_data,'output':response_3})
                #response_3b = self.DCC.a_b_post(portfolio,org,'schd_runs',json.dumps(response_3),'application/json',False)
                #return result, 400
            else:  
                status = 200
                result.append({'success':True,'action':action,'handler':handler_name,'input':handler_input_data,'output':response_3})
          
            #UP FROM HERE , OK   
             
        #Save response_3 to S3, You'll store the s3 url in the change['output']
        iso_date = datetime.now().strftime('%Y-%m-%d')
        response_3b = self.DCC.a_b_post(portfolio,org,f'schd_runs/{iso_date}',json.dumps(response_3),'application/json',False)
        

        # Check s3 Response
        if response_3b['success']:
            if 'path' in response_3b:
                output_doc = response_3b['path']
            else:
                output_doc = 'Could not store in S3..'
        else:
            output_doc = 'Could not store in S3.'
        
           
        
            
        
        #4. Record the results from the handler run in the schd_runs document, return 
        
        action = 'record_results'
        run_id = response_2['path'].split('/')[-1]
        changes = {}
    
        changes['output'] = output_doc
        changes['status'] = 'executed'
        changes['time_executed'] = str(int(datetime.now().timestamp()))
            
        response_4, status = self.DAC.put_a_b_c(portfolio,org,'schd_runs',run_id,changes)
        
        current_app.logger.debug(f'Record handler output in run document:{response_4}')
        result.append({'action':action,'input':changes,'output':response_4})
        
        #self.DAC.refresh_s3_cache(portfolio, org, 'schd_runs', None)
        
        
        return result, status
        
        
    
    def direct_run(self,handler,payload):
           
        result = []

        action = 'direct_run'
        
        print(f'Calling handler:{handler}, payload:{payload}')
             
        response = {'success':False,'output':[]}
        
        # A way to limit the calls to this endpoint is to make each one of these runs have the same name as a blueprint. 
        # And before every run, we could fetch the blueprint. It if doesn't exist we abort the call. 
        # It makes sense that there is a blueprint for every RPC as it shows the inputs of the call. 
        # We could store every call to the RPC as a document. The ring itself is the name of the blueprint. 
        tool, handler_name = payload['handler'].split('/')
        if tool != '_action':
            blueprint = self.BPC.get_blueprint('irma',handler_name,'last')
        
            if 'fields' not in blueprint:
                result.append({'success':False,'action':action,'handler':'','message':'No valid handler'}) 
                return result, 400
            
        response = self.SHL.load_and_run(handler, payload = payload)
        
        #print(f'Handler output:{response}')
        
        
        if not response['success']:
            result.append({'success':False,'action':action,'handler':handler_name,'input':payload,'output':response})
            return result, 400
        
        result.append({'success':True,'action':action,'handler':handler_name,'input':payload,'output':response})

        return result, 200
    
    
    
    def handler_call(self,portfolio,org,tool,handler,payload):
        action = 'handler_call'
        
        print(f'Calling handler:{handler}, payload:{payload}')
        
        try:        
            # We override portfolio, org and tool that might come in the payload.
            payload['portfolio'] = portfolio
            payload['org'] = org
            payload['tool'] = tool

                
            response = {'success':False,'output':[]}
            
            response = self.SHL.load_and_run(f'{tool}/{handler}', payload = payload)
            
            if not response['success']:
                return {'success':False,'action':action,'handler':handler,'input':payload,'output':response}
                      
            canonical = response['output']['output']
            return {'success':True,'action':action,'handler':handler,'input':payload,'output':canonical,'stack':response}
        
        except Exception as e:
            print(f'Error @handler_call: {str(e)}')
            return {'success':False,'action':action,'handler':handler,'input':payload,'output':f'Error @handler_call: {str(e)}'}


    

    def delete_rule(self, rule_name):
        try:
            # List rules before deletion
            rules_before = eventbridge.list_rules(NamePrefix=rule_name)
            logger.info(f"Rules before deletion: {rules_before}")
            
            # Delete the rule
            response = eventbridge.delete_rule(Name=rule_name)
            
            # List rules after deletion to confirm
            rules_after = eventbridge.list_rules(NamePrefix=rule_name)
            logger.info(f"Rules after deletion: {rules_after}")
            
            return response
        except Exception as e:
            logger.error(f"Error deleting rule: {str(e)}")
            raise
