import boto3
from botocore.exceptions import ClientError
from renglo.logger import get_logger


class StateModel:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.logger = get_logger()
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Adjust region if needed
        table_name = self.config.get('DYNAMODB_BLUEPRINT_TABLE', 'default_blueprint_table')
        self.state_table = self.dynamodb.Table(table_name)
            

    def get_state(self,name,v):

        irn = 'irn:state:irma:'+ name

        self.logger.debug('Get State '+irn+' v:'+v)
        

        try:
            if v == 'last':
                response = self.state_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('irn').eq(irn),
                    ScanIndexForward=False # Show latest state versions first
                )
                items = response.get('Items', [])
                
                if len(items)==0:
                    return {"success":False,"message": "Document not found"}
                item = items[0]
                #current_app.logger.info('items from DB:'+str(items))
                       
            else:
                response = self.state_table.get_item(Key={'irn': irn, 'version': v})
                item = response.get('Item')

            if item:
                return item
            else:
                return {"success":False,"message": "Document not found"}
        except ClientError as e:
            return {"error": e.response['Error']['Message']}
        
