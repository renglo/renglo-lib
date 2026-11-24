import boto3
from decimal import Decimal
from renglo.config_names import setting
from renglo.logger import get_logger
from botocore.exceptions import ClientError


class BlueprintModel:

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
        resolved_region = region_name or setting(self.config, "REGION", "us-east-1")
        self.dynamodb = dynamodb_resource or boto3.resource('dynamodb', region_name=resolved_region)
        table_name = setting(self.config, "BLUEPRINT_TABLE", "default_blueprint_table")
        self.blueprints_table = self.dynamodb.Table(table_name)
            

    
    def put_blueprint(self,data):

        try:
            self.blueprints_table.put_item(Item=data)
            return {"success": True, "message": "Document created", "document": data, "status": 201}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}


    def get_blueprint(self,handle,name,v):

        irn = 'irn:blueprint:' + handle +':'+ name
        legacy_irn = 'blueprint:' + handle +':'+ name

        self.logger.debug('Get Blueprint '+irn+' v:'+v)
        

        try:
            if v == 'last':
                response = self.blueprints_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('irn').eq(irn),
                    ScanIndexForward=False # Show latest blueprint versions first
                )
                items = response.get('Items', [])
                if len(items) == 0:
                    # Backward compatibility for legacy key prefix without leading "irn:"
                    response = self.blueprints_table.query(
                        KeyConditionExpression=boto3.dynamodb.conditions.Key('irn').eq(legacy_irn),
                        ScanIndexForward=False
                    )
                    items = response.get('Items', [])
                
                if len(items)==0:
                    return {"success":False,"message": "Document not found"}
                item = items[0]
                #current_app.logger.info('items from DB:'+str(items))
                       
            else:
                response = self.blueprints_table.get_item(Key={'irn': irn, 'version': v})        
                item = response.get('Item')
                if not item:
                    response = self.blueprints_table.get_item(Key={'irn': legacy_irn, 'version': v})
                    item = response.get('Item')

            if item:
                return item
            else:
                return {"success":False,"message": "Document not found"}
        except ClientError as e:
            return {"error": e.response['Error']['Message']}


    def list_versions(self, handle, name):
        """Every stored version of one schema. The partition key is the schema irn."""
        irn = 'irn:blueprint:' + handle + ':' + name
        items = []
        start_key = None
        try:
            while True:
                kwargs = {
                    'KeyConditionExpression': boto3.dynamodb.conditions.Key('irn').eq(irn),
                }
                if start_key:
                    kwargs['ExclusiveStartKey'] = start_key
                response = self.blueprints_table.query(**kwargs)
                items.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey')
                if not start_key:
                    break
            return {"success": True, "items": items, "status": 200}
        except ClientError as e:
            return {"success": False, "message": e.response['Error']['Message'], "status": 500}


    def list_names(self, handle):
        """Schema names stored under one portfolio handle."""
        prefix = 'irn:blueprint:' + handle + ':'
        names = []
        seen = set()
        start_key = None
        try:
            while True:
                kwargs = {
                    'FilterExpression': boto3.dynamodb.conditions.Attr('irn').begins_with(prefix),
                    'ProjectionExpression': 'irn',
                }
                if start_key:
                    kwargs['ExclusiveStartKey'] = start_key
                response = self.blueprints_table.scan(**kwargs)
                for item in response.get('Items', []):
                    irn = str((item or {}).get('irn') or '')
                    if not irn.startswith(prefix):
                        continue
                    name = irn[len(prefix):]
                    if name and name not in seen:
                        seen.add(name)
                        names.append(name)
                start_key = response.get('LastEvaluatedKey')
                if not start_key:
                    break
            return {"success": True, "items": sorted(names), "status": 200}
        except ClientError as e:
            return {"success": False, "message": e.response['Error']['Message'], "status": 500}
        

    def update_blueprint(self,data):

        try:
            self.blueprints_table.put_item(Item=data)
            return {"success": True, "message": "Document updated", "document": data, "status": 200}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}
        
    
    def delete_blueprint(self,handle,name,v):
        
        pk = 'irn:blueprint:' + handle +':'+ name
        sk = v
        
        try:
            self.blueprints_table.delete_item(Key={'irn': pk, 'version': sk})
            return {"success": True, "message": "Document deleted", "status": 200}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}


    def put_ontology(self, data):
        """Store one ontology version. The irn prefix keeps it apart from schemas."""
        try:
            item = _decimals(data)
            self.blueprints_table.put_item(Item=item)
            return {"success": True, "message": "Document created", "document": _plain(item), "status": 201}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}


    def get_ontology(self, handle, name, v):
        irn = 'irn:ontology:' + handle + ':' + name
        self.logger.debug('Get Ontology ' + irn + ' v:' + v)
        try:
            if v == 'last':
                response = self.blueprints_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('irn').eq(irn),
                    ScanIndexForward=False
                )
                items = response.get('Items', [])
                if len(items) == 0:
                    return {"success": False, "message": "Document not found"}
                return _plain(items[0])
            response = self.blueprints_table.get_item(Key={'irn': irn, 'version': v})
            item = response.get('Item')
            if item:
                return _plain(item)
            return {"success": False, "message": "Document not found"}
        except ClientError as e:
            return {"error": e.response['Error']['Message']}


    def list_ontology_versions(self, handle, name):
        """Every stored version of one ontology."""
        irn = 'irn:ontology:' + handle + ':' + name
        items = []
        start_key = None
        try:
            while True:
                kwargs = {
                    'KeyConditionExpression': boto3.dynamodb.conditions.Key('irn').eq(irn),
                }
                if start_key:
                    kwargs['ExclusiveStartKey'] = start_key
                response = self.blueprints_table.query(**kwargs)
                items.extend(_plain(item) for item in response.get('Items', []))
                start_key = response.get('LastEvaluatedKey')
                if not start_key:
                    break
            return {"success": True, "items": items, "status": 200}
        except ClientError as e:
            return {"success": False, "message": e.response['Error']['Message'], "status": 500}


    def list_ontology_names(self, handle):
        """Ontology names stored under one portfolio handle."""
        prefix = 'irn:ontology:' + handle + ':'
        names = []
        seen = set()
        start_key = None
        try:
            while True:
                kwargs = {
                    'FilterExpression': boto3.dynamodb.conditions.Attr('irn').begins_with(prefix),
                    'ProjectionExpression': 'irn',
                }
                if start_key:
                    kwargs['ExclusiveStartKey'] = start_key
                response = self.blueprints_table.scan(**kwargs)
                for item in response.get('Items', []):
                    irn = str((item or {}).get('irn') or '')
                    if not irn.startswith(prefix):
                        continue
                    found = irn[len(prefix):]
                    if found and found not in seen:
                        seen.add(found)
                        names.append(found)
                start_key = response.get('LastEvaluatedKey')
                if not start_key:
                    break
            return {"success": True, "items": sorted(names), "status": 200}
        except ClientError as e:
            return {"success": False, "message": e.response['Error']['Message'], "status": 500}


    def delete_ontology(self, handle, name, v):
        pk = 'irn:ontology:' + handle + ':' + name
        try:
            self.blueprints_table.delete_item(Key={'irn': pk, 'version': v})
            return {"success": True, "message": "Document deleted", "status": 200}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}


    def put_dictionary(self, data):
        """Store one dictionary version. The irn prefix keeps it apart from schemas and ontologies."""
        try:
            item = _decimals(data)
            self.blueprints_table.put_item(Item=item)
            return {"success": True, "message": "Document created", "document": _plain(item), "status": 201}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}


    def get_dictionary(self, handle, name, v):
        irn = 'irn:dictionary:' + handle + ':' + name
        self.logger.debug('Get Dictionary ' + irn + ' v:' + v)
        try:
            if v == 'last':
                response = self.blueprints_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('irn').eq(irn),
                    ScanIndexForward=False
                )
                items = response.get('Items', [])
                if len(items) == 0:
                    return {"success": False, "message": "Document not found"}
                return _plain(items[0])
            response = self.blueprints_table.get_item(Key={'irn': irn, 'version': v})
            item = response.get('Item')
            if item:
                return _plain(item)
            return {"success": False, "message": "Document not found"}
        except ClientError as e:
            return {"error": e.response['Error']['Message']}


    def list_dictionary_versions(self, handle, name):
        """Every stored version of one dictionary."""
        irn = 'irn:dictionary:' + handle + ':' + name
        items = []
        start_key = None
        try:
            while True:
                kwargs = {
                    'KeyConditionExpression': boto3.dynamodb.conditions.Key('irn').eq(irn),
                }
                if start_key:
                    kwargs['ExclusiveStartKey'] = start_key
                response = self.blueprints_table.query(**kwargs)
                items.extend(_plain(item) for item in response.get('Items', []))
                start_key = response.get('LastEvaluatedKey')
                if not start_key:
                    break
            return {"success": True, "items": items, "status": 200}
        except ClientError as e:
            return {"success": False, "message": e.response['Error']['Message'], "status": 500}


    def list_dictionary_names(self, handle):
        """Dictionary names stored under one portfolio handle."""
        prefix = 'irn:dictionary:' + handle + ':'
        names = []
        seen = set()
        start_key = None
        try:
            while True:
                kwargs = {
                    'FilterExpression': boto3.dynamodb.conditions.Attr('irn').begins_with(prefix),
                    'ProjectionExpression': 'irn',
                }
                if start_key:
                    kwargs['ExclusiveStartKey'] = start_key
                response = self.blueprints_table.scan(**kwargs)
                for item in response.get('Items', []):
                    irn = str((item or {}).get('irn') or '')
                    if not irn.startswith(prefix):
                        continue
                    found = irn[len(prefix):]
                    if found and found not in seen:
                        seen.add(found)
                        names.append(found)
                start_key = response.get('LastEvaluatedKey')
                if not start_key:
                    break
            return {"success": True, "items": sorted(names), "status": 200}
        except ClientError as e:
            return {"success": False, "message": e.response['Error']['Message'], "status": 500}


    def delete_dictionary(self, handle, name, v):
        pk = 'irn:dictionary:' + handle + ':' + name
        try:
            self.blueprints_table.delete_item(Key={'irn': pk, 'version': v})
            return {"success": True, "message": "Document deleted", "status": 200}
        except ClientError as e:
            return {"success": False, "error": e.response['Error']['Message'], "status": 500}


def _decimals(value):
    """DynamoDB rejects float. Ontology weights are stored as Decimal."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_decimals(item) for item in value]
    if isinstance(value, dict):
        return {key: _decimals(item) for key, item in value.items()}
    return value


def _plain(value):
    """Turn Decimal weights back into JSON numbers."""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value
