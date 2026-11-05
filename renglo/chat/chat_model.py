# chat_model.py

from decimal import Decimal
import json
from renglo.config_names import setting
from renglo.document_stores.select import StoreError, open_store
from renglo.logger import get_logger

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

class ChatModel:

    def __init__(self, config=None, tid=False, ip=False, *, store=None):
        """
        Initialize ChatModel with configuration.
        
        Args:
            config (dict): Configuration dictionary containing CHAT_TABLE
            tid: Transaction ID (optional)
            ip: IP address (optional)
        """
        self.config = config or {}
        self.logger = get_logger()
        
        table_name = setting(self.config, "CHAT_TABLE", "default_chat_table")
        self.table_name = table_name
        self.store = store or open_store(
            self.config,
            "chat",
            table_name,
            "index",
            "entity_index",
            region_name="us-east-1",
            use_profile=False,
        )  

    def sanitize(self, obj):
        if isinstance(obj, list):
            return [self.sanitize(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self.sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            # Convert Decimal to int if it's a whole number, otherwise float
            return int(obj) if obj % 1 == 0 else float(obj)
        elif isinstance(obj, (int, float)):
            # Keep numbers as is - DynamoDB will handle them
            return obj
        else:
            return obj
        
        
    
    def query_chat(self,index,entity_index_prefix,limit=50,lastkey=None,sort='asc'):
        
        result = {}
        all_items = []

        try:
            # DynamoDB Limit is per request, not total; paginate with LastEvaluatedKey. Cap per page (API max 1000).
            page_limit = max(1, min(int(limit), 1000))
            exclusive_start_key = lastkey
            max_pages = 500
            page = 0
            while page < max_pages:
                page += 1
                items, exclusive_start_key = self.store.query(
                    index,
                    prefix=entity_index_prefix or None,
                    limit=page_limit,
                    continuation=exclusive_start_key,
                    descending=sort != "asc",
                )
                all_items.extend(items)
                
                if not exclusive_start_key:
                    break

            # Build the result with all items
            result['success'] = True
            result['items'] = all_items
            
            return result

        except StoreError as e:
            
            result['success'] = False
            result['message'] = 'Items could not be retrieved'
            result['items'] = all_items
            result['error'] = str(e)
            status = 400
            return result
        
        

    def list_chat(self,index,entity_index,limit=50,lastkey=None,sort='asc'):
        
        result = {}
        all_items = []

        try:
            while True:
                items, lastkey = self.store.query(
                    index,
                    equals=entity_index,
                    limit=limit,
                    continuation=lastkey,
                    descending=sort != "asc",
                )
                all_items.extend(items)
                
                # If there's no more pages, break the loop
                if not lastkey:
                    break

            # Build the result with all items
            result['success'] = True
            result['items'] = all_items
            
            return result

        except StoreError as e:
            
            result['success'] = False
            result['message'] = 'Items could not be retrieved'
            result['items'] = all_items
            result['error'] = str(e)
            status = 400
            return result
        
    
    def get_chat(self,index,entity_index,message_id):
        
        result = {}
        
        self.logger.debug(f'get_chat: {index} > {message_id}')
        
        try:
            # Build the query parameters with KeyConditionExpression

            
            self.logger.debug(f'Query index={index} entity_index={entity_index} _id={message_id}')
            items, _token = self.store.query(
                index,
                equals=entity_index,
                filter_field="_id",
                filter_value=message_id,
            )
            response = {"Items": items}
            #current_app.logger.debug(f'Raw DynamoDB response: {response}')
            
            # Extract items
            items = response.get('Items', [])
            #current_app.logger.debug(f'Extracted items: {items}')
            
            if not items:
                self.logger.debug(f'No items found for index: {index} and message_id: {message_id}')                
                result['success'] = False
                result['message'] = 'Item not found'
                return result
            
            #print(f'CHM:get_chat > {items[0]}')
            
            # Build the result
            result['success'] = True
            result['item'] = items[0]  # Return single item
            
            return result

        except Exception as e:
            self.logger.error(f"Error in get_chat: {str(e)}")
            result['success'] = False
            result['message'] = 'Item could not be retrieved'
            result['error'] = str(e)
            return result
        
        
        
    def create_chat(self,data):
        
        print(f'create_chat > input:{data}')

        try:
            # Sanitize data before storing
            sanitized_data = self.sanitize(data)
            status = self.store.put(sanitized_data)
            self.logger.debug('MODEL: Created chat successfully:'+str(sanitized_data))
            return {
                "success":True, 
                "message": "Chat created", 
                "document": sanitized_data,
                "status" : status
                }
        except StoreError as e:
            print(f'create_chat > error:{e}')
            return {
                "success":False, 
                "message": e.message,
                "document": data,
                "status" : e.status
                }
        
        
        
    def update_chat(self,data):


        try:
            # Sanitize data before storing
            sanitized_data = self.sanitize(data)
            status = self.store.put(sanitized_data)
            return {
                "success":True, 
                "message": "Chat updated", 
                "document": sanitized_data,
                "status" : status
                }
        except StoreError as e:
            return {
                "success":False, 
                "message": e.message,
                "document": data,
                "status" : e.status
                }

      
    # NOT USED  
    def delete_chat(self,**data):

        keys = {
            'irn': data['irn'],
            'time': data['time']
        }

        try:
            status = self.store.delete(None, None, key=keys)
            self.logger.debug('MODEL: Deleted Chat:' + str(data))
            return {
                "success":True,
                "message": "Entity deleted", 
                "document": data,
                "status" : status
                }
        
        except StoreError as e:
            return {
                "success":False,
                "message": e.message,
                "document": data,
                "status" : e.status
                }

            
    
