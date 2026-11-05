# session_model.py

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

class SessionModel:

    def __init__(self, config=None, tid=False, ip=False, *, store=None):
        """
        Initialize SessionModel with configuration.
        
        Args:
            config (dict): Configuration dictionary containing SESSION_TABLE
            tid: Transaction ID (optional)
            ip: IP address (optional)
        """
        self.config = config or {}
        self.logger = get_logger()
        
        region = setting(self.config, "REGION", "us-east-1")
        table_name = setting(self.config, "SESSION_TABLE", "default_session_table")
        self.table_name = table_name
        self.store = store or open_store(
            self.config,
            "session",
            table_name,
            "index",
            "entity_index",
            region_name=region,
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
        
        
    
    def query_session(self,index,entity_index_prefix,limit=50,lastkey=None,sort='asc'):
        
        result = {}
        all_items = []

        try:
            while True:
                items, lastkey = self.store.query(
                    index,
                    prefix=entity_index_prefix or None,
                    limit=limit,
                    continuation=lastkey,
                    descending=sort != "asc",
                )
                all_items.extend(items)
                if not lastkey:
                    break

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
        
        

    def list_session(self,index,entity_index,limit=50,lastkey=None,sort='asc'):
        
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
                if not lastkey:
                    break

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
        
    
    def get_session(self,index,entity_index,message_id):
        
        result = {}
        
        self.logger.debug(f'get_session: {index} > {message_id}')
        
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
            
            #print(f'CHM:get_session > {items[0]}')
            
            # Build the result
            result['success'] = True
            result['item'] = items[0]  # Return single item
            
            return result

        except Exception as e:
            self.logger.error(f"Error in get_session: {str(e)}")
            result['success'] = False
            result['message'] = 'Item could not be retrieved'
            result['error'] = str(e)
            return result
        
        
        
    def create_session(self,data):
        
        print(f'create_session > input:{data}')

        try:
            # Sanitize data before storing
            sanitized_data = self.sanitize(data)
            status = self.store.put(sanitized_data)
            self.logger.debug('MODEL: Created session successfully:'+str(sanitized_data))
            return {
                "success":True, 
                "message": "Session created", 
                "document": sanitized_data,
                "status" : status
                }
        except StoreError as e:
            print(f'create_session > error:{e}')
            return {
                "success":False, 
                "message": e.message,
                "document": data,
                "status" : e.status
                }
        
        
        
    def _for_append(self, obj):
        """Drop None and turn floats into strings. DynamoDB rejects both."""
        if isinstance(obj, list):
            return [self._for_append(item) for item in obj]
        if isinstance(obj, dict):
            return {
                key: self._for_append(value)
                for key, value in obj.items()
                if value is not None
            }
        if isinstance(obj, float):
            return str(obj)
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else str(obj)
        return obj

    def append_events(self, index, entity_index, documents):
        """Append documents onto a turn without rewriting the rest of the item.

        A full put loses events another writer added after this process read
        the turn. list_append keeps both.
        """
        if isinstance(documents, dict):
            documents = [documents]
        cleaned = self._for_append(list(documents or []))
        try:
            self.store.append(index, entity_index, "events", cleaned)
            return {
                "success": True,
                "message": "Session updated",
                "document": cleaned,
            }
        except StoreError as e:
            return {
                "success": False,
                "message": e.message,
                "document": cleaned,
                "status": e.status,
            }

    def update_session(self,data):


        try:
            # Sanitize data before storing
            sanitized_data = self.sanitize(data)
            status = self.store.put(sanitized_data)
            return {
                "success":True, 
                "message": "Session updated", 
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
    def delete_session(self,**data):

        keys = {
            'irn': data['irn'],
            'time': data['time']
        }

        try:
            status = self.store.delete(None, None, key=keys)
            self.logger.debug('MODEL: Deleted Session:' + str(data))
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

    def put_continuation_lock(self, index, entity_index, holder, now, lease):
        self.store.put(
            {
                "index": index,
                "entity_index": entity_index,
                "holder": holder,
                "lease_until": lease,
            },
            absent_or_less=("lease_until", now),
        )

    def delete_continuation_lock(self, index, entity_index, holder):
        self.store.delete(index, entity_index, equals=("holder", holder))
