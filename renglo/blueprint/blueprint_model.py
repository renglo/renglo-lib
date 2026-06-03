from flask import redirect, url_for, jsonify, session, request

import boto3
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
        resolved_region = region_name or self.config.get("AWS_REGION", "us-east-1")
        self.dynamodb = dynamodb_resource or boto3.resource('dynamodb', region_name=resolved_region)
        table_name = self.config.get('DYNAMODB_BLUEPRINT_TABLE', 'default_blueprint_table')
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
