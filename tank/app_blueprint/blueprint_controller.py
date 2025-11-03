from flask import redirect,url_for, jsonify, current_app, session, request
import urllib.parse
import requests
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid
from decimal import Decimal
from tank.app_blueprint.blueprint_model import BlueprintModel
from tank.app_auth.auth_controller import AuthController


class BlueprintController:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.BPM = BlueprintModel(config=self.config)
        self.AUC = AuthController(config=self.config, tid=tid, ip=ip)
        

    def create_blueprint(self,data):

        data['_id'] = str(uuid.uuid4())
        data['added'] = datetime.now().isoformat()
        # It should only allow the creator's handle. Override
        data['handle'] = session["current_user"]

        data['irn'] = 'blueprint:' + data['handle'] +':'+ data['name']
        if 'version' not in data:
            data['version'] = "1.0.0"

        return self.BPM.put_blueprint(data)


    def get_blueprint(self,handle,name,v):

        return self.BPM.get_blueprint(handle,name,v)


    def update_blueprint(self,handle,name):
        data = request.json
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
        # scheme='https', netloc='tank1.helloirma.com', path='/_blueprint/irma/metrics', params='', query='v=1.0.1', fragment=''
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
        

    def extract_arguments(self):

        #return BPC.branch_blueprint(handle,name,v)
        result = {}

        handle = session["current_user"]

        # Get all query parameters
        args = request.args

        # Deserialize query parameters
        deserialized_data = {}
        for key in args.keys():
            values = args.getlist(key)
            # If there's only one value, store it as a single value, otherwise store the list
            if len(values) == 1:
                deserialized_data[key] = values[0]
            else:
                deserialized_data[key] = values

        if 'name' in deserialized_data:
            name = deserialized_data["name"]
        else:
            return jsonify(message="Incomplete data:name")
        
        if 'blueprint' in deserialized_data:
            blueprint = deserialized_data["blueprint"]
        else:
            return jsonify(message="Incomplete data:blueprint")
        
        if 'version' in deserialized_data:
            version = deserialized_data["version"]
        else:
            return jsonify(message="Incomplete data:version")
        
        if 'tags' in deserialized_data:
            tags = deserialized_data["tags"]
        else:
            tags = []
         
        result = {'handle':'x','name':'y','blueprint':blueprint,'version':version,'tags':tags}
        #return result
        #return handle,name,blueprint,version,tags
        


    def branch_blueprint(self):

        return {'error':True,'message':'Not implemented'}
    


    def clone_blueprint(self):


        handle = session["current_user"]

        # Get all query parameters
        args = request.args

        # Deserialize query parameters
        deserialized_data = {}
        for key in args.keys():
            values = args.getlist(key)
            # If there's only one value, store it as a single value, otherwise store the list
            if len(values) == 1:
                deserialized_data[key] = values[0]
            else:
                deserialized_data[key] = values

        if 'name' in deserialized_data:
            name = deserialized_data["name"]
        else:
            return jsonify(message="Incomplete data:name")
        
        if 'blueprint' in deserialized_data:
            blueprint = deserialized_data["blueprint"]
        else:
            return jsonify(message="Incomplete data:blueprint")
        
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


        current_app.logger.info('Cloning a Blueprint')
        data = self.extract_blueprint_data(blueprint)

        data['handle'] = handle
        data['name'] = name
        data['label'] = name
        data['uri'] = current_app.config['TANK_BASE_URL']+"/_blueprint"+"/"+handle+"/"+name+"/"+data['version']

       
        #return data
        if('error' in data):
            return data

        # Only final blueprints can be cloned
        if (data['status'] != 'final'):
            return {"message":"Status:"+data['status']+". This blueprint can't be cloned because it is not final:"+ data['blueprint_origin']}

        
        #Store it in the Blueprint Table
        data['_id'] = str(uuid.uuid4())
        data['added'] = datetime.now().isoformat()
        data['irn'] = 'blueprint:' + handle +':'+ name
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


    