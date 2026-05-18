import urllib.parse
import requests
from datetime import datetime
import uuid
import re
from renglo.blueprint.blueprint_model import BlueprintModel
from renglo.auth.auth_controller import AuthController
from renglo.logger import get_logger
from renglo.runtime import get_request_args, get_request_json, get_session_value


class BlueprintController:

    def __init__(
        self,
        config=None,
        tid=False,
        ip=False,
        *,
        dynamodb_resource=None,
        region_name=None,
    ):
        self.config = config or {}
        self.logger = get_logger()
        self.BPM = BlueprintModel(
            config=self.config,
            dynamodb_resource=dynamodb_resource,
            region_name=region_name,
        )
        self.AUC = AuthController(config=self.config, tid=tid, ip=ip)
        

    def create_blueprint(self, data, user_handle=None):

        data['_id'] = str(uuid.uuid4())
        data['added'] = datetime.now().isoformat()
        # It should only allow the creator's handle. Override
        data['handle'] = user_handle or get_session_value("current_user")
        if not data['handle']:
            return {'success': False, 'message': 'Missing current user handle', 'status': 400}

        data['irn'] = 'irn:blueprint:' + data['handle'] +':'+ data['name']
        if 'version' not in data:
            data['version'] = "1.0.0"

        return self.BPM.put_blueprint(data)


    def get_blueprint(self,handle,name,v):

        return self.BPM.get_blueprint(handle,name,v)


    def update_blueprint(self, handle, name, data=None):
        data = data if isinstance(data, dict) else get_request_json({})
        data['handle'] = handle
        data['name'] = name
    
        return self.BPM.update_blueprint(data)


    def delete_blueprint(self,handle,name,v):

        return self.BPM.delete_blueprint(handle,name,v)
    


    def is_valid_semver(self,version):
        """
        Check if the provided string is a valid semantic version.
        """
        semver_pattern = r'^(\d+\.\d+\.\d+)$'
        return re.match(semver_pattern, version) is not None
    

    def validate_blueprint_string(self,input_str):
        """
        Validate that the input string meets the specified criteria:
        - First position is '_blueprint'
        - Last position is a valid semantic version or 'last'
        """
        parts = input_str.split('/')

        # Check if the first part is '_blueprint'
        if parts[1] != '_blueprint':
            return False

        # Check if the last part is a valid semantic version or 'last'
        last_part = parts[-1]
        if last_part != 'last' and not self.is_valid_semver(last_part):
            return False
        
        return True
    

    def extract_blueprint_data(self,blueprint):
        '''
        Adds ring to userdoc

        @IN: 
          blueprint = (URI)

        @OUT:
          True (fixed)

        @COLLATERAL
          -Adds ring to userdoc
          -Makes a local copy of the blueprint (a branch)
        '''

        #1. Call the URI in Blueprint and retrieve its JSON document

        urlparts = urllib.parse.urlparse(blueprint)

        #1b. Simple check to validate URL
        # scheme='https', netloc='renglo1.helloirma.com', path='/_blueprint/irma/metrics', params='', query='v=1.0.1', fragment=''
        if self.validate_blueprint_string(urlparts.path):
            blueprint_origin=urllib.parse.urlunparse(('https', urlparts.netloc, urlparts.path , '', '', ''))
        else:
            return {'error':True,'message':'Invalid Blueprint URL:'+blueprint}
        
   

        try:
            # Send a GET request to the URL
            response = requests.get(blueprint_origin)

            # Raise an exception if the request was unsuccessful
            response.raise_for_status()

            # Parse the JSON response
            # TO-DO
            #There should be a check here to figure out if this is a real irma blueprint
            # We could use a JSON-SCHEMA validator for that. 
            data = response.json()

            if data['status'] != 'final':
                return {"error":True,"message":"Status:"+data['status']+". This blueprint can't be branched:"+blueprint_origin}
            
            #We need to change the handle and the name to whatever has been indicated
            # in the function input


            # Replace last with real version
            parts = urlparts.path.split('/')
            if parts[-1] == 'last':
                parts[-1] = data['version']
                new_path = '/'.join(parts)
                data['blueprint_origin']=urllib.parse.urlunparse(('https', urlparts.netloc, new_path , '', '', ''))
            else:
                data['blueprint_origin'] = blueprint_origin

            
            #Figure out if the Blueprint is external
            #if (parts[2] != handle) or (urlparts.netloc != request.host) :
            #    data['blueprint_external'] = True


            return data

            
        except requests.RequestException as e:
            # Handle any exceptions that occur during the request
            
            return {'error':True,'message':'Could not find blueprint_origin:'+ blueprint_origin}
        

    def extract_arguments(self, query_params=None):
        data = query_params if isinstance(query_params, dict) else get_request_args()
        required = ('name', 'blueprint', 'version')
        for key in required:
            if key not in data:
                return {'success': False, 'message': f'Incomplete data:{key}', 'status': 400}
        return {
            'success': True,
            'handle': get_session_value("current_user"),
            'name': data['name'],
            'blueprint': data['blueprint'],
            'version': data['version'],
            'tags': data.get('tags', []),
        }


    def branch_blueprint(self):

        return {'error':True,'message':'Not implemented'}
    


    def clone_blueprint(self, user_handle=None, query_params=None):


        handle = user_handle or get_session_value("current_user")
        if not handle:
            return {'error': True, 'message': 'Missing current user handle'}

        deserialized_data = query_params if isinstance(query_params, dict) else get_request_args()

        if 'name' in deserialized_data:
            name = deserialized_data["name"]
        else:
            return {"error": True, "message": "Incomplete data:name"}
        
        if 'blueprint' in deserialized_data:
            blueprint = deserialized_data["blueprint"]
        else:
            return {"error": True, "message": "Incomplete data:blueprint"}
        
        '''
        if 'version' in deserialized_data:
            version = deserialized_data["version"]
        else:
            return jsonify(message="Incomplete data:version")
        '''
        
        if 'tags' in deserialized_data:
            tags = deserialized_data["tags"]
        else:
            tags = []


        self.logger.info('Cloning a Blueprint')
        data = self.extract_blueprint_data(blueprint)

        data['handle'] = handle
        data['name'] = name
        data['label'] = name
        base_url = self.config.get('BASE_URL', '')
        data['uri'] = base_url+"/_blueprint"+"/"+handle+"/"+name+"/"+data['version']

       
        #return data
        if('error' in data):
            return data

        # Only final blueprints can be cloned
        if (data['status'] != 'final'):
            return {"message":"Status:"+data['status']+". This blueprint can't be cloned because it is not final:"+ data['blueprint_origin']}

        
        #Store it in the Blueprint Table
        data['_id'] = str(uuid.uuid4())
        data['added'] = datetime.now().isoformat()
        data['irn'] = 'irn:blueprint:' + handle +':'+ name
        if 'version' not in data:
            data['version'] = "1.0.0"

        self.BPM.put_blueprint(data)

        
        # Register new Ring in User Document
        doc = self.AUC.get_user(handle)

        new_ring = {
            "name": name,
            "blueprint_origin" : data['blueprint_origin'],
            "blueprint" : data['uri'],
            "version": data['version'], 
            "status" : "final",
            "count": 0,
            "added": datetime.now().isoformat(),
            "tags" : [tags]
        }

        doc['rings'].insert(0,new_ring)

        result = self.AUC.update_user(handle,doc)

        return result


    