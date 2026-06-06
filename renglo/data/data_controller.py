#data_controller.py
import urllib.parse
from datetime import datetime
import uuid
import re
import json, collections
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

from renglo.data.data_model import DataModel
from renglo.blueprint.blueprint_controller import BlueprintController
from renglo.auth.auth_controller import AuthController
from renglo.search.search_controller import SearchController
from renglo.graph.graph_controller import GraphController
from renglo.logger import get_logger
from renglo.logger import get_logger


# Add this custom JSON encoder class at the top level of your file
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


def convert_js_to_json(js_string):
    """
    Convert JavaScript object syntax to proper JSON format.
    Handles unquoted property names and single quotes.
    """
    if not isinstance(js_string, str):
        return js_string
    
    print(f"Converting JS to JSON - Input: {js_string}")
    
    # Replace single quotes with double quotes
    js_string = js_string.replace("'", '"')
    
    # More robust regex to handle nested objects and arrays
    # This pattern matches property names that aren't already quoted
    # and handles various whitespace scenarios
    import re
    
    # First, let's handle the most common case: simple property names
    # This regex looks for word characters that are followed by a colon
    # but not preceded by a quote
    pattern = r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:'
    replacement = r'\1"\2":'
    js_string = re.sub(pattern, replacement, js_string)
    
    # Handle property names at the start of the string (for root objects)
    pattern = r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:'
    replacement = r'"\1":'
    js_string = re.sub(pattern, replacement, js_string)
    
    print(f"Converting JS to JSON - Output: {js_string}")
    
    return js_string


def convert_js_to_json_advanced(js_string):
    """
    More advanced JavaScript to JSON converter that handles complex cases.
    """
    if not isinstance(js_string, str):
        return js_string
    
    print(f"Advanced JS to JSON - Input: {js_string}")
    
    # Replace single quotes with double quotes
    js_string = js_string.replace("'", '"')
    
    import re
    
    # Handle property names in various contexts
    # This is a more comprehensive approach
    
    # Step 1: Handle property names after opening braces and commas
    js_string = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', js_string)
    
    # Step 2: Handle property names at the very beginning (for root objects)
    js_string = re.sub(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'"\1":', js_string)
    
    # Step 3: Handle property names after array elements
    js_string = re.sub(r'([\]}])\s*,\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1,"\2":', js_string)
    
    print(f"Advanced JS to JSON - Output: {js_string}")
    
    return js_string


def convert_js_to_json_robust(js_string):
    """
    Robust JavaScript to JSON converter that handles whitespace, newlines, and formatting issues.
    """
    if not isinstance(js_string, str):
        return js_string
    
    print(f"Robust JS to JSON - Input: {js_string}")
    
    # Replace single quotes with double quotes
    js_string = js_string.replace("'", '"')
    
    import re
    
    # Step 1: Handle property names in various contexts
    js_string = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', js_string)
    js_string = re.sub(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'"\1":', js_string)
    js_string = re.sub(r'([\]}])\s*,\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1,"\2":', js_string)
    
    # Step 2: Clean up whitespace and formatting
    # Remove trailing commas before closing braces/brackets
    js_string = re.sub(r',(\s*[}\]])', r'\1', js_string)
    
    # Clean up excessive whitespace and newlines, but preserve time formats
    js_string = re.sub(r'\s+', ' ', js_string)
    
    # Clean up commas and colons, but be careful with time strings
    # Don't add spaces around colons in time strings (HH:MM:SS or HH:MM)
    js_string = re.sub(r'\s*,\s*', ', ', js_string)
    
    # Only add spaces around colons that are property separators, not time separators
    # This regex looks for colons that are followed by a space or closing brace/bracket
    js_string = re.sub(r'\s*:\s*(?=[^"]*["\d])', ': ', js_string)
    
    # Step 3: Ensure proper array/object formatting
    js_string = re.sub(r'\[\s*{', '[{', js_string)
    js_string = re.sub(r'}\s*\]', '}]', js_string)
    js_string = re.sub(r'}\s*,', '},', js_string)
    
    print(f"Robust JS to JSON - Output: {js_string}")
    
    return js_string


def convert_js_to_json_simple(js_string):
    """
    Simple and reliable JavaScript to JSON converter.
    """
    if not isinstance(js_string, str):
        return js_string
    
    print(f"Simple JS to JSON - Input: {js_string}")
    
    # Replace single quotes with double quotes
    js_string = js_string.replace("'", '"')
    
    import re
    
    # Step 1: Handle property names - only target property names, not values
    # Look for word characters followed by colon that are not inside quotes
    js_string = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', js_string)
    js_string = re.sub(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'"\1":', js_string)
    
    # Step 2: Remove trailing commas
    js_string = re.sub(r',(\s*[}\]])', r'\1', js_string)
    
    # Step 3: Clean up excessive whitespace but preserve formatting
    js_string = re.sub(r'\s+', ' ', js_string)
    
    print(f"Simple JS to JSON - Output: {js_string}")
    
    return js_string


class DataController:

    def _run_graph_operation(self, op_name, operation):
        if not self.graph_db_enabled:
            return {'success': True, 'skipped': True, 'reason': 'Graph DB disabled by GRAPH_DB_ENABLED'}
        if not self.GRC:
            return {'success': True, 'skipped': True, 'reason': 'Graph controller not configured'}

        try:
            return operation()
        except ClientError as exc:
            error_code = exc.response.get('Error', {}).get('Code')
            if error_code == 'ResourceNotFoundException':
                self.logger.warning(f"Graph operation '{op_name}' skipped: graph table not found. {str(exc)}")
                return {'success': True, 'skipped': True, 'reason': 'Graph table not found', 'error': str(exc)}

            self.logger.error(f"Graph operation '{op_name}' failed with ClientError: {str(exc)}")
            return {'success': False, 'skipped': True, 'reason': 'Graph operation failed', 'error': str(exc)}
        except Exception as exc:
            self.logger.error(f"Graph operation '{op_name}' failed: {str(exc)}")
            return {'success': False, 'skipped': True, 'reason': 'Graph operation failed', 'error': str(exc)}

    def __init__(self, config=None, tid=None, ip=None):
        self.config = config or {}
        self.logger = get_logger()
        self.DAM = DataModel(config=self.config, tid=tid, ip=ip)
        self.BPC = BlueprintController(config=self.config, tid=tid, ip=ip)
        self.AUC = AuthController(config=self.config, tid=tid, ip=ip)
        self.search_controller = SearchController(config=self.config)
        self.graph_db_enabled = self.config.get('GRAPH_DB_ENABLED', True)
        if not isinstance(self.graph_db_enabled, bool):
            raise ValueError("GRAPH_DB_ENABLED must be a boolean (True/False)")
        self.GRC = None
        if self.graph_db_enabled and self.config.get('DYNAMODB_GRAPH_TABLE'):
            self.GRC = GraphController(config=self.config)
        
            
        
    def refresh_s3_cache(self,portfolio, org, ring, sort=None):
    
        s3_client = boto3.client('s3')
        bucket_name = self.config.get('S3_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME not found in config")
        self.logger.debug(f'Refreshing s3 cache')
        # Proceed to regenerate the document
        response = []  # Initialize response
        # Simulate regeneration logic
        max_iterations = 50
        limit = 249
        iterations = 0
        lastkey = None
        
        file_path = f'data/{portfolio}/{org}/{ring}'
        
        while True:
            iterations += 1
            self.logger.debug("Iteration:" + str(iterations))
            
            partial_response = self.get_a_b(portfolio, org, ring, limit, lastkey, sort)
            response.extend(partial_response['items'])
            lastkey = partial_response.get('last_id')
            
            if lastkey is None or iterations >= max_iterations:
                break
        
        result = {
            "items": response,
            "last_id": None,
            "success": True
        }
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=json.dumps(result, cls=DecimalEncoder)
        )
        
        return result, 201
    
    
    
    def sanitize(self,obj):
        '''
        Avoids Floats being sent to DynamoDB
        '''
        if isinstance(obj, list):
            return [self.sanitize(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self.sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            # Convert Decimal to int if it's a whole number, otherwise float
            return int(obj) if obj % 1 == 0 else float(obj)
        elif isinstance(obj, float):
            # Keep as Decimal for DynamoDB numeric compatibility
            return Decimal(str(obj))
        elif isinstance(obj, int):
            # Keep integers as is
            return obj
        else:
            return obj
        
            
    
    def generate_index_string_x(self,blueprint,item_values):
        # Check if blueprint has an "indexes" key
        indexes = blueprint.get('indexes')
        if indexes is None:
            return False  # No index string needs to be generated if "indexes" doesn't exist

        # Ensure "indexes" has a "path" key and it is a list
        path = indexes.get('path')
        if not isinstance(path, list):
            return False  # Invalid indexes object, exit with False

        # Check if all fields in the path exist in the blueprint's fields
        valid_fields = {field['name'] for field in blueprint.get('fields', [])}
        for field_name in path:
            if field_name not in valid_fields:
                # Log an error and exit gracefully
                print(f"Error: Field '{field_name}' does not exist in the blueprint.")
                return False

        # Start building the index string with the constant prefix
        index_string = "irn:h_index:"

        # Iterate through the path list and construct the index string
        for field_name in path:
            # Determine the resource name by removing the '_id' suffix
            resource_name = field_name.replace('_id', '')

            # Append resource name to the index string
            index_string += resource_name + ":"

            # Get the value of the field from item_values
            if field_name not in item_values:
                # Log an error and exit gracefully
                print(f"Error: Field '{field_name}' not found in item_values.")
                return False
            
            index_value = item_values[field_name]
            index_string += str(index_value) + ":"

        # Remove the trailing colon from the constructed index string
        index_string = index_string.rstrip(":")

        # Append the blueprint name (current resource name) to the index string
        index_string += f":{blueprint.get('name')}"

        # Check if there's a "time" key in the indexes dictionary
        
        time_fields = indexes.get('time', [])
        if isinstance(time_fields, list):
            safe_values = []

            # Compare and validate each field in the time list
            for time_field in time_fields:
                if time_field in valid_fields and time_field in item_values:
                    # Get the value from item_values
                    field_value = item_values[time_field]

                    # Make the field value safe by allowing only alphanumeric characters and replacing spaces with underscores
                    safe_value = re.sub(r'[^a-zA-Z0-9]', '_', field_value)
                    safe_values.append(safe_value)
                else:
                    safe_values.append('_')

            # If we have valid time field values, concatenate them
            if safe_values:
                # Join values with an underscore if more than one
                concatenated_values = ":".join(safe_values)

                # Get the current timestamp in Unix epoch (seconds)
                current_timestamp = int(datetime.now().timestamp())

                # Create the final timestamp string with field values and the current timestamp
                timestamp_string = f"{concatenated_values}:{current_timestamp}"

                # Concatenate the timestamp string to the index string with a dot
                index_string += f".{timestamp_string}"

        return index_string
    
   
    def generate_index_string(self,blueprint,org,item_values):
        # Check if blueprint has an "indexes" key
        indexes = blueprint.get('indexes')
        if indexes is None:
            return False  # No index string needs to be generated if "indexes" doesn't exist

        # Ensure "indexes" has a "path" key and it is a list
        path = indexes.get('path')
        if not isinstance(path, list):
            return False  # Invalid indexes object, exit with False

        # Check if all fields in the path exist in the blueprint's fields
        valid_fields = {field['name'] for field in blueprint.get('fields', [])}
        for field_name in path:
            if field_name not in valid_fields:
                # Log an error and exit gracefully
                print(f"Error: Field '{field_name}' does not exist in the blueprint.")
                return False

        # Start building the index string with the constant prefix
        index_string = "irn:h_index:"   
        index_string += f"{org}:"  
        index_string += f"{blueprint.get('name')}:"

        # Iterate through the path list and construct the index string
        for field_name in path:

            # Get the value of the field from item_values
            if field_name not in item_values:
                # Log an error and exit gracefully
                print(f"Error: Field '{field_name}' not found in item_values.")
                return False
            
            index_value = item_values[field_name]
            index_string += str(index_value) + ":"

        # Remove the trailing colon from the constructed index string
        index_string = index_string.rstrip(":")

        return index_string
    

    def _is_multiple_cardinality(self, field):
        cardinality = str(field.get('cardinality', 'single')).strip().lower()
        return cardinality in ('multiple', 'multi')

    def _is_object_source_definition(self, field):
        source = field.get('source')
        if not isinstance(source, dict):
            return False
        target = source.get('target')
        return isinstance(target, str) and target.strip()

    def _extract_reference_value(self, item):
        if isinstance(item, dict):
            candidates = [
                item.get('value'),
                item.get('id'),
                item.get('_id'),
            ]
            target_obj = item.get('target')
            if isinstance(target_obj, dict):
                candidates.extend(
                    [
                        target_obj.get('id'),
                        target_obj.get('_id'),
                        target_obj.get('value'),
                    ]
                )
            for candidate in candidates:
                if candidate is None or isinstance(candidate, (dict, list)):
                    continue
                candidate_str = str(candidate).strip()
                if candidate_str:
                    return candidate_str
            return ''

        if item is None:
            return ''
        item_str = str(item).strip()
        return item_str

    def _normalize_reference_object(self, field, item):
        source = field.get('source') if isinstance(field, dict) else {}
        source = source if isinstance(source, dict) else {}
        qualifier_keys = source.get('qualifiers') if isinstance(source.get('qualifiers'), list) else []
        qualifier_keys = [str(key).strip() for key in qualifier_keys if str(key).strip()]
        source_labels_raw = source.get('label')
        source_labels = (
            [str(label).strip() for label in source_labels_raw if str(label).strip()]
            if isinstance(source_labels_raw, list)
            else (
                [token.strip() for token in source_labels_raw.split(',') if token and token.strip()]
                if isinstance(source_labels_raw, str) and source_labels_raw.strip()
                else []
            )
        )

        ref_value = self._extract_reference_value(item)
        if not ref_value:
            return None

        normalized = dict(item) if isinstance(item, dict) else {}
        normalized['value'] = ref_value
        if source_labels:
            normalized.setdefault('label', source_labels[:2])

        incoming_qualifiers = normalized.get('qualifiers')
        qualifiers = incoming_qualifiers if isinstance(incoming_qualifiers, dict) else {}
        for qualifier_key in qualifier_keys:
            qualifiers.setdefault(qualifier_key, '')
        if qualifiers or qualifier_keys:
            normalized['qualifiers'] = qualifiers

        return normalized

    def _normalize_source_reference_value(self, field, parsed_value):
        if not self._is_object_source_definition(field):
            return parsed_value

        if self._is_multiple_cardinality(field):
            values = parsed_value if isinstance(parsed_value, list) else [parsed_value]
            normalized_values = []
            for value in values:
                normalized_item = self._normalize_reference_object(field, value)
                if normalized_item is not None:
                    normalized_values.append(normalized_item)
            return normalized_values

        normalized_item = self._normalize_reference_object(field, parsed_value)
        return normalized_item if normalized_item is not None else {}

    def _normalize_multiple_input(self, raw_value, field_type):
        if raw_value is None:
            return []

        if isinstance(raw_value, list):
            if field_type == 'array':
                if len(raw_value) == 0:
                    return []
                # Backward compatible: raw list can be either one array value or list of arrays.
                if any(isinstance(entry, list) for entry in raw_value):
                    return raw_value
                return [raw_value]
            return raw_value

        if isinstance(raw_value, str):
            stripped = raw_value.strip()
            if stripped == '':
                return []
            if field_type == 'array':
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list) and (len(parsed) == 0 or any(isinstance(entry, list) for entry in parsed)):
                        return parsed
                except Exception:
                    pass

        return [raw_value]

    def _parse_scalar_field_value(self, field, raw_value, strict_numbers=False):
        field_type = field.get('type')

        if field_type == 'object':
            try:
                if isinstance(raw_value, dict):
                    return raw_value
                if raw_value is None:
                    return {}
                return json.loads(str(raw_value).strip())
            except Exception:
                if raw_value in ('', None):
                    return {}
                return str(raw_value).strip()

        if field_type == 'array':
            try:
                if isinstance(raw_value, list):
                    return raw_value
                if raw_value is None or (isinstance(raw_value, str) and not str(raw_value).strip()):
                    return []
                return json.loads(str(raw_value).strip())
            except json.JSONDecodeError:
                if raw_value is None or (isinstance(raw_value, str) and not str(raw_value).strip()):
                    return []
                try:
                    converted_json = convert_js_to_json(str(raw_value).strip())
                    return json.loads(converted_json)
                except Exception:
                    try:
                        converted_json = convert_js_to_json_advanced(str(raw_value).strip())
                        return json.loads(converted_json)
                    except Exception:
                        try:
                            converted_json = convert_js_to_json_robust(str(raw_value).strip())
                            return json.loads(converted_json)
                        except Exception:
                            try:
                                converted_json = convert_js_to_json_simple(str(raw_value).strip())
                                return json.loads(converted_json)
                            except Exception:
                                return str(raw_value).strip()
            except Exception:
                if raw_value is None or (isinstance(raw_value, str) and not str(raw_value).strip()):
                    return []
                return str(raw_value).strip()

        if field_type == 'timestamp':
            if raw_value is None:
                return None
            raw_str = str(raw_value).strip()
            if raw_str == '':
                return None
            try:
                float_value = float(raw_str)
                timestamp_value = int(float_value)
                if timestamp_value >= 0:
                    if timestamp_value < 1000000000:
                        timestamp_value *= 1000
                    return timestamp_value
                return None
            except (ValueError, OverflowError):
                try:
                    date_value = datetime.strptime(raw_str, '%Y-%m-%d')
                    return int(date_value.timestamp() * 1000)
                except ValueError:
                    return None

        if field_type == 'string':
            # For source-governed relationship fields, allow clients to submit
            # structured objects (value/label/qualifiers). Keep plain string
            # ids fully backward compatible.
            if self._is_object_source_definition(field) and isinstance(raw_value, dict):
                return raw_value
            return str(raw_value).strip() if raw_value not in (None, '') else ''

        if field_type in ('number', 'integer', 'float'):
            if raw_value is None or (isinstance(raw_value, str) and not str(raw_value).strip()):
                return None
            try:
                raw_str = str(raw_value).strip()
                if field_type == 'integer':
                    return int(float(raw_str))
                if field_type == 'float':
                    return Decimal(raw_str)
                return Decimal(raw_str) if '.' in raw_str else int(raw_str)
            except Exception:
                if strict_numbers:
                    raise ValueError('Invalid number format')
                return None

        return str(raw_value).strip() if raw_value not in (None, '') else None

    def _parse_field_value(self, field, raw_value, strict_numbers=False):
        parsed_output = None
        if self._is_multiple_cardinality(field):
            normalized_values = self._normalize_multiple_input(raw_value, field.get('type'))
            parsed_values = [
                self.sanitize(self._parse_scalar_field_value(field, value, strict_numbers=strict_numbers))
                for value in normalized_values
            ]
            parsed_output = parsed_values
        else:
            parsed_output = self.sanitize(
                self._parse_scalar_field_value(field, raw_value, strict_numbers=strict_numbers)
            )

        parsed_output = self._normalize_source_reference_value(field, parsed_output)
        return self.sanitize(parsed_output)

    def construct_post_item(self,portfolio,org,ring,payload):
        '''
        Creates a new item following the blueprint fields and data submitted via the request.

        @IN:
          
          portfolio = (string)
          org = (string)
          ring= (string)
          payload = (dict)

        @OUT:
          ok:(item_id)
          ko:False

        @COLLATERAL:
          - Create new document in DB
          - Increase item count in userdoc

        '''

        version = 'last'

        blueprint = self.BPC.get_blueprint('irma',ring,version)

        if not isinstance(blueprint, dict) or blueprint.get('success') is False or 'fields' not in blueprint:
            missing = (
                blueprint.get('message', 'unknown')
                if isinstance(blueprint, dict)
                else 'not a blueprint document'
            )
            raise ValueError(
                f"Blueprint for ring '{ring}' not found or invalid ({missing}). "
                f"Expected irn:blueprint:irma:{ring} with a 'fields' array in {self.config.get('DYNAMODB_BLUEPRINT_TABLE', 'blueprints table')}."
            )

        item_values = {}
        #rich_values = {}
        #history_values = {}
        #flag_values = {}
        fields = blueprint['fields']

        
        self.logger.debug("post_a_b raw arguments from the Form fields:"+str(payload))


        for field in fields:
       
            #DATA

            self.logger.debug(field['name'])

            #Verify submitted field exists in the blueprint
            new_raw = ''
            if field['name'] in payload:
                new_raw = payload.get(field['name'])
                self.logger.debug('Using: '+str(field['name'])+':'+str(new_raw))        
            else:
                self.logger.debug('Inserting default value for field '+str(field['name'])+': '+str(field['default']))
                # Instead of skipping we put the field's default value
                new_raw = field['default']
                

            item_values[field['name']] = self._parse_field_value(field, new_raw)


        item = {}

        item['added'] = datetime.now().isoformat()
        item['modified'] = datetime.now().isoformat()
        item['license'] = 'CC BY'
        item['public'] = False
        item['blueprint'] = blueprint['uri']
        item['portfolio'] = portfolio
        item['org'] = org
        item['ring'] = ring
        item['blueprint_version'] = blueprint['version']

        if 'singleton' in blueprint and blueprint['singleton'] is True:
            item['_id'] = "00000000-0000-0000-0000-000000000000"
        else:   
            item['_id'] = str(uuid.uuid4())
            
        item['attributes'] = item_values  
        
        
        index_string = self.generate_index_string(blueprint,org,item_values)    
        if index_string:
            item['path_index'] = index_string
        elif 'path_index' in item:
            del item['path_index']
        

        return item



    def construct_put_item(self,portfolio,org,ring,idx,payload):
        '''
        Creates an updated item based on an existing item following the blueprint .
        Notice that the payload contains only the fields that have been changed.
        This function completes the rest of the item based on the existing document.

        @NOTES:
          - "request.url" are the arguments that come via url
          - "request.form" are the arguments that come via form


        @IN:
          request.url = {(string):(string),}
          request.form = {(string):(string),}
          portfolio = (string)
          org = (string)
          ring = (string)
          idx = (string)

        @OUT:
          ok:(item_id)
          ko:False

        @COLLATERAL:
          - Update document in DB

        '''
        print(f'Running construct_put_item for {portfolio}/{org}/{ring}/{idx}. Payload {payload}')

        #1. Pull the document that we need to update
        updated_item = self.DAM.get_a_b_c(portfolio,org,ring,idx) 
        #self.logger.debug('Item from DB:'+str(updated_item))

        #2. Pull the Blueprint listed in that document
  
        version = 'last'

        blueprint = self.BPC.get_blueprint('irma',ring,version)
        if not isinstance(blueprint, dict) or blueprint.get('success') is False or 'fields' not in blueprint:
            missing = (
                blueprint.get('message', 'unknown')
                if isinstance(blueprint, dict)
                else 'not a blueprint document'
            )
            raise ValueError(
                f"Blueprint for ring '{ring}' not found or invalid ({missing}). "
                f"Expected irn:blueprint:irma:{ring} with a 'fields' array."
            )
        fields = blueprint['fields']

        #3. Convert incoming request payload to JSON
   
        #self.logger.debug('CPI Payload:'+str(payload))
        #print(f"CPI TYPE:{type(payload).__name__}")
        #self.logger.debug(blueprint['fields']) 

        #4. Check that the payload follows the Blueprint
        putNeeded = False

        # Normalize legacy scalar values when blueprint now expects cardinality=multiple.
        attributes = updated_item.get('attributes')
        if not isinstance(attributes, dict):
            updated_item['attributes'] = {}
            attributes = updated_item['attributes']

        for field in fields:
            if not self._is_multiple_cardinality(field):
                continue
            field_name = field.get('name')
            if field_name not in attributes:
                continue
            existing_value = attributes.get(field_name)
            if existing_value is None or isinstance(existing_value, list):
                continue
            attributes[field_name] = [self.sanitize(existing_value)]
            putNeeded = True

        for field in fields:
            self.logger.debug('>>:'+field['name']) 
            if field['name'] in payload:
                self.logger.debug('Found:'+field['name']) 
                # Attribute exists in the blueprint
                new_raw = payload.get(field['name'])
                is_multiple = self._is_multiple_cardinality(field)
                normalized_values = self._normalize_multiple_input(new_raw, field.get('type')) if is_multiple else None

                if field.get('required'):
                    if is_multiple:
                        if len(normalized_values) == 0:
                            self.logger.debug('Attribute is required:'+field['name'])
                            return {'error':'Attribute is required'}
                    else:
                        if new_raw is None or (isinstance(new_raw, str) and len(new_raw.strip()) == 0):
                            self.logger.debug('Attribute is required:'+field['name'])
                            return {'error':'Attribute is required'}

                try:
                    updated_item['attributes'][field['name']] = self._parse_field_value(
                        field,
                        new_raw,
                        strict_numbers=True
                    )
                    putNeeded = True
                except ValueError as parse_error:
                    return {'error': str(parse_error)}
                  
        if not putNeeded:
            return {'error':'Attributes not recognized'}
        
        
        # DEPRECATED (Update the index string.)
        # YOU CAN'T UPDATE THE LSI
        '''
        index_string = self.generate_index_string(blueprint, updated_item['attributes'] )
        self.logger.debug('(GIS) > Index string:'+str(index_string))
        if index_string:
            updated_item['path_index'] = index_string
        elif 'path_index' in updated_item:
            del updated_item['path_index']
        '''

        #6. Return to save document to DB
        #updated_item['modified'] = datetime.now().isoformat()
        #print('CPI > OUTPUT:')
        #print(updated_item)
        
        updated_item = self.sanitize(updated_item)

        return updated_item
    
    #DEPRECATED
    def get_a_index(self,portfolio,prefix_path):
        
        items = []
        
        result = self.DAM.get_a_index(portfolio,prefix_path)
        self.logger.debug('get_a_index results:' + json.dumps(result))  # Convert result to string
        
        if 'error' in result:
            self.logger.error(result['error'])
            
            result['success'] = False
            result['message'] = 'Items could not be retrieved (@get_a_index)'
            result['error'] = result['error']
            status = 400
            return result

        i=0
        for row in result['items']:

            i += 1
            '''
            i += 1
            if lastkey and i==1:
                #If lastkey was sent, ignore first item 
                #as it was the last item in the last page
                continue
            '''

            item = {}
            item = row['attributes']
            item['_id'] = row['_id']

            if 'modified' in row:
                item['_modified'] = row['modified']
            else:
                item['_modified'] = ''
                
            if 'path_index' in row:
                item['_index'] = row['path_index']
            else:
                item['_index'] = ''

            if item:
                items.append(item)

        '''       
        if len(items)>1 and sort:

            self.sort = sort
            items = sorted(items, key=self.sort_item_list, reverse=sort_reverse)
        '''
        self.logger.debug('NUMBER OF ITEMS:'+str(i))

        #return items,result['lastkey']
        return items
    
    
    #DEPRECATED
    def get_a_b_index(self,portfolio,prefix_path):
        
        items = []
        
        result = self.DAM.get_a_b_index(portfolio,prefix_path)
        self.logger.debug('get_a_b_index results:' + json.dumps(result))  # Convert result to string
        
        if 'error' in result:
            self.logger.error(result['error'])
            
            result['success'] = False
            result['message'] = 'Items could not be retrieved (@get_a_b_index)'
            result['error'] = result['error']
            status = 400
            return result

        i=0
        for row in result['items']:

            i += 1
            '''
            i += 1
            if lastkey and i==1:
                #If lastkey was sent, ignore first item 
                #as it was the last item in the last page
                continue
            '''

            item = {}
            item = row['attributes']
            item['_id'] = row['_id']

            if 'modified' in row:
                item['_modified'] = row['modified']
            else:
                item['_modified'] = ''
                
            if 'path_index' in row:
                item['_index'] = row['path_index']
            else:
                item['_index'] = ''

            if item:
                items.append(item)

        '''       
        if len(items)>1 and sort:

            self.sort = sort
            items = sorted(items, key=self.sort_item_list, reverse=sort_reverse)
        '''
        self.logger.debug('NUMBER OF ITEMS:'+str(i))

        #return items,result['lastkey']
        return items
        
        
    def get_a_b_query(self,query):
        
        '''
        Incoming query object must have the following shape
            {
            'portfolio':<portfolio_id>,
            'org':<org_id>,
            'ring':<ring_id>,
            'operator':<begins_with|chrono|greater_than|less_than|equal_to>,
            'value':<value>,
            'filter':{
                   'operator':<greater_than|less_than>,
                   'field':<field_to_filter_on>,
                   'value':<value_filter_uses_on_the_field>
                },
            'limit':<page_limit>,
            'lastkey':<page_lastkey>,
            'sort': <asc|desc>
            }
        '''
        
        
        #prefix = f'irn:h_index:{org}:{ring}:{index_tail}'
        
        if 'operator' not in query or not query['operator']:
            return {'success':False,'message':'No query'}
            
        operator = query['operator']
        #portfolio_index = f'irn:data:{query["portfolio"]}'
           
        # SWITCH   
        # The index begins with ...         
        if operator=='begins_with':
            
            response = self.DAM.get_a_b_beginswith(query)
            
        # The index is a timestamp, return results in chronological order
        if operator=='chrono':
            
            response = self.DAM.get_a_b_beginswith(query)
        
        # The index is numeric, return anything greater than ...
        if operator=='greater_than':
            
            response = self.DAM.get_a_b_greaterthan(query)
        
        # The index is numeric, return anything less than ...
        if operator=='less_than':
            
            response = self.DAM.get_a_b_lessthan(query)
        
        # The index is equal to ...
        if operator=='equal_to':
            
            response = self.DAM.get_a_b_equalto(query)
            
            
        items = []
        #response = self.DAM.get_a_b(portfolio,org,ring,limit=limit,lastkey=lastkey)
        result = {}
        if 'error' in response:
            self.logger.error(response['error'])
            
            result['success'] = False
            result['message'] = 'Items could not be retrieved(@get_a_b_query)'
            result['error'] = response['error']
            status = 400
            return result

        i=0
        for row in response['items']:

            i += 1
               
            if query['lastkey'] and i==1:
                #If lastkey was sent, ignore first item 
                #as it was the last item in the last page
                continue
            
            item = {}
            item = row['attributes']
            item['_id'] = row['_id']

            if 'modified' in row:
                item['_modified'] = row['modified']
            else:
                item['_modified'] = ''
                
            if 'path_index' in row:
                item['_index'] = row['path_index']
            else:
                item['_index'] = ''

            if item:
                items.append(item)
                
        
        last_id = response['lastkey']
                       
        
        result['success'] = True
        result['items'] = items
        result['last_id'] = last_id
        
        self.logger.debug('NUMBER OF ITEMS (QUERY):'+str(i))
        
        return result
        
        

    def get_a_b(self,portfolio,org,ring,limit=1000,lastkey=None,sort=None):
        '''
        Get page of items

        @NOTES:
          - The "human" parameter determines whether ids or labels are returned as keys

        @IN:
          portfolio = (string)
          org = (string)
          ring = (string)
          limit = (integer)
          lastkey = (string)
          endkey = (string)
          sort = (string)

        @OUT:
          [{(item)}]

        '''
       
        items = []

        response = self.DAM.get_a_b(portfolio,org,ring,limit=limit,lastkey=lastkey)
        
        #self.logger.debug(f'RRR2: {result}')

        result = {}
        if 'error' in response:
            self.logger.error(response['error'])
            
            result['success'] = False
            result['message'] = 'Items could not be retrieved (@get_a_b)'
            result['error'] = response['error']
            status = 400
            return result

        i=0
        for row in response['items']:

            i += 1
               
            if lastkey and i==1:
                #If lastkey was sent, ignore first item 
                #as it was the last item in the last page
                continue
            
            item = {}
            item = row['attributes']
            item['_id'] = row['_id']

            if 'modified' in row:
                item['_modified'] = row['modified']
            else:
                item['_modified'] = ''
                
            if 'path_index' in row:
                item['_index'] = row['path_index']
            else:
                item['_index'] = ''

            if item:
                items.append(item)
                
        '''result = {
                'items': items,
                'lastkey': endkey  # This will be passed as 'lastkey' in the next call if needed
            }'''
                    
        last_id = response['last_id']
                      
        if len(items)>1 and sort:
            items = sorted(items, key=lambda item: item[sort], reverse=True)
            
        
        result['success'] = True
        result['items'] = items
        result['last_id'] = last_id
        
        self.logger.debug('NUMBER OF ITEMS:'+str(i))
        
        return result
    


    def post_a_b(self,portfolio,org,ring,payload):
        '''
        Creates new item
        '''

        try:
            item = self.construct_post_item(portfolio,org,ring,payload)
            
            self.logger.debug('Prepared Item:'+str(item))

            response = self.DAM.post_a_b(portfolio,org,ring,item)

            result = {}
            status = 0

            if 'error' not in response:
                result['success'] = True
                result['message'] = 'Item saved (POST)'
                result['path'] = str(portfolio+'/'+org+'/'+ring+'/'+item['_id'])
                result['item'] = item
                status = 200
                self.search_controller.index_document(portfolio, org, ring, item)
                result['graph'] = self._run_graph_operation(
                    'sync_document_graph_edges (POST)',
                    lambda: self.GRC.sync_document_graph_edges(
                        portfolio,
                        org,
                        ring,
                        item['_id'],
                        item.get('attributes', {}),
                    ),
                )

            else:
                result['success'] = False
                result['message'] = 'Item could not be saved'
                result['error'] = response['error']
                status = 400
            
            self.logger.debug('Returned object:'+str(result))
            
            return result, status
            
        except Exception as e:
            self.logger.error(f'Error in post_a_b: {str(e)}')
            result = {
                'success': False,
                'message': 'Item could not be saved due to an exception',
                'error': str(e)
            }
            status = 500
            return result, status


    
    def get_a_b_c(self,portfolio,org,ring,idx):
        '''
        Gets an existing item
        '''   
        self.logger.debug('IDX:'+str(idx))
        
        response = self.DAM.get_a_b_c(portfolio,org,ring,idx)

        result = {}

        if 'error' in response:                    
            result['success'] = False
            result['message'] = 'Item could not be retrieved'
            result['error'] = response['error']
            status = 400

        elif 'attributes' not in response:
            result['success'] = False
            result['message'] = 'Item could not be retrieved, No Attributes'
            result['error'] = response['error']
            status = 400

        else:
            result = response['attributes']
            result['_id'] = response['_id']
            
            if 'modified' in response:
                result['_modified'] = response['modified']
            else:
                result['_modified'] = ''
                
            if 'path_index' in response:
                result['_index'] = response['path_index']
            else:
                result['_index'] = ''
            
            
            if 'modified' in response:
                result['_modified'] = response['modified']
            else:
                result['_modified'] = ''
        
        

        self.logger.debug('Returned object:'+str(result))
        
        return result
    


    def put_a_b_c(self,portfolio,org,ring,idx,payload):
        '''
        Partial updates to an existing document. 
        FE only needs to send the field to be updated. No need to send the entire document.
        '''
        #1. 

        result = {}

        #self.logger.debug('Icoming put object:'+str(payload))
        item = self.construct_put_item(portfolio,org,ring,idx,payload)

        if 'error' in item:
            self.logger.debug(str(item))
            result['success'] = False
            result['message'] = 'Item could not be saved'
            result['error'] = item['error']
            status = 400
            return result, status
    
        self.logger.debug('Updating Item:'+str(item))
        response = self.DAM.put_a_b_c(portfolio,org,ring,idx,item)
        
        #self.logger.debug('Update response:'+str(response))


        if 'error' not in response:
            result['success'] = True
            result['message'] = 'Item saved (PUT)'
            result['path'] = str(portfolio+'/'+org+'/'+ring+'/'+idx)
            status = 200
            self.logger.debug('Returned object:'+str(result))
            self.search_controller.index_document(portfolio, org, ring, item)
            result['graph'] = self._run_graph_operation(
                'sync_document_graph_edges (PUT)',
                lambda: self.GRC.sync_document_graph_edges(
                    portfolio,
                    org,
                    ring,
                    idx,
                    item.get('attributes', {}),
                ),
            )

            return result, status

        else:
            result['success'] = False
            result['message'] = 'Item could not be saved'
            result['error'] = response['error']
            status = 500
            self.logger.debug('Returned object:'+str(result))

            return result, status

    def delete_a_b_c(self, portfolio, org, ring, idx):
        '''
        Delete an existing document.
        '''
        
        self.logger.debug('Item to delete:'+str(idx))

        doc_before_delete = self.get_a_b_c(portfolio, org, ring, idx)
        graph_attrs = {}
        if isinstance(doc_before_delete, dict) and doc_before_delete.get('success') is not False:
            graph_attrs = doc_before_delete

        response = self.DAM.delete_a_b_c(portfolio,org,ring,idx)

        result = {}

        if 'error' not in response:
            result['success'] = True
            result['message'] = 'Item deleted'
            result['path'] = str(portfolio+'/'+org+'/'+ring+'/'+idx)
            status = 200
            self.logger.debug('Returned object:'+str(result))
            self.search_controller.delete_document(portfolio, org, ring, idx)
            result['graph'] = self._run_graph_operation(
                'remove_document_graph_edges (DELETE)',
                lambda: self.GRC.remove_document_graph_edges(
                    portfolio,
                    org,
                    ring,
                    idx,
                    graph_attrs,
                ),
            )

            return result, status

        else:
            result['success'] = False
            result['message'] = 'Item could not be deleted'
            result['error'] = response['error']
            status = 500
            self.logger.debug('Returned object:'+str(result))

            return result, status

        
        

        
        
        