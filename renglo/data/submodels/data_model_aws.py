#data_model.py

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from renglo.config_names import setting
from renglo.auth.auth_controller import AuthController
from renglo.blueprint.blueprint_controller import BlueprintController


def _dynamodb_resource(config, region, dynamodb_resource=None):
    if dynamodb_resource is not None:
        return dynamodb_resource
    profile = str(setting(config, "PROFILE") or "").strip()
    if profile:
        session = boto3.session.Session(profile_name=profile)
        return session.resource("dynamodb", region_name=region)
    return boto3.resource("dynamodb", region_name=region)


class DataModel:

    def __init__(self, config=None, tid=False, ip=False, region_name=None, dynamodb_resource=None, bind_auth=True):
        self.config = config or {}

        if bind_auth:
            self.AUC = AuthController(config=self.config, tid=tid, ip=ip)
            self.BPC = BlueprintController(config=self.config, tid=tid, ip=ip)
        else:
            self.AUC = None
            self.BPC = None

        # Callers that omit region_name keep the historical us-east-1 client.
        resolved_region = region_name or "us-east-1"
        self.region = resolved_region
        self.dynamodb = _dynamodb_resource(self.config, resolved_region, dynamodb_resource)
        table_name = setting(self.config, "DATA_TABLE")
        if not table_name:
            raise ValueError("DATA_TABLE configuration is required but not found")
        self.data_table = self.dynamodb.Table(table_name)
        self.table_name = table_name  
            
    
   
    def post_a_b(self,portfolio,org,ring,item):

       
        
        item['portfolio_index'] = 'irn:data:'+portfolio
        item['doc_index'] = org+':'+ring+':'+item['_id'] # _id was generated in the controller
        
        try:
            self.data_table.put_item(Item=item)
            #self.increase_item_count(handle,ring)  #TO-DO: Implement increase_item_count!
            return {"message": "Item created", "document": item}
        except ClientError as e:
            return {"error": e.response['Error']['Message']}
        

      
    
    def get_a_b(self, portfolio, org, ring, limit=10000, lastkey=None):
        # Construct the partition key and sort key prefix
        portfolio_index = f'irn:data:{portfolio}'  # This will be used as the partition key (PK)
        
        # Sort key (SK) prefix for querying documents based on org, ring, and a generated prefix
        prefix_doc_index = f'{org}:{ring}:'  # This is the prefix we will use in begins_with for SK
        
        #print('portfolio_index:',portfolio_index)
        #print('prefix_doc_index:',prefix_doc_index)
                
        try:
            # Build the query parameters with KeyConditionExpression
            query_params = {
                'TableName': self.table_name,
                'KeyConditionExpression': Key('portfolio_index').eq(portfolio_index) & Key('doc_index').begins_with(prefix_doc_index),
                'Limit': limit
            }
            
            #print('Data Model > query_params:',query_params)

            # Add the ExclusiveStartKey to the query parameters if provided (for pagination)
            if lastkey:                  
                query_params['ExclusiveStartKey'] = {
                        'doc_index': f'{org}:{ring}:{lastkey}',
                        'portfolio_index': f'irn:data:{portfolio}'
                    }
 
            # Query DynamoDB to get items with matching PK and SK prefix
            response = self.data_table.query(**query_params)
            
            # Extract items and pagination key
            items = response.get('Items', [])
            endkey = response.get('LastEvaluatedKey')  # Pagination key for next query
            
            # Build the result
            result ={}
            result['items'] = items
            if endkey:
                result['last_id'] = endkey['doc_index'].split(':')[-1]
            else:
                result['last_id'] = None

            return result

        except (BotoCoreError, ClientError) as e:
            return {"error": str(e)}   
        
        
        
    def get_a_b_batch(self, portfolio, org, ring, limit=10000, lastkey=None):
        # Construct the partition key and sort key prefix
        portfolio_index = f'irn:data:{portfolio}'  # This will be used as the partition key (PK)
        
        # Sort key (SK) prefix for querying documents based on org, ring, and a generated prefix
        prefix_doc_index = f'{org}:{ring}'  # This is the prefix we will use in begins_with for SK
                
        try:
            # Build the query parameters with KeyConditionExpression
            query_params = {
                'TableName': self.table_name,
                'KeyConditionExpression': Key('portfolio_index').eq(portfolio_index) & Key('doc_index').begins_with(prefix_doc_index),
                'Limit': limit
            }

            # Add the ExclusiveStartKey to the query parameters if provided (for pagination)
            if lastkey:                  
                query_params['ExclusiveStartKey'] = {
                        'doc_index': f'{org}:{ring}:{lastkey}',
                        'portfolio_index': f'irn:data:{portfolio}'
                    }
 
            # Query DynamoDB to get items with matching PK and SK prefix
            response = self.data_table.query(**query_params)
            
            # Extract items and pagination key
            items = response.get('Items', [])
            endkey = response.get('LastEvaluatedKey')  # Pagination key for next query
            
            # Build the result
            result ={}
            result['items'] = items
            if endkey:
                result['last_id'] = endkey['doc_index'].split(':')[-1]
            else:
                result['last_id'] = None

            return result

        except (BotoCoreError, ClientError) as e:
            return {"error": str(e)}   
        
        
     
        

    def get_a_b_c(self,portfolio,org,ring,idx):

        #irn = 'irn:data:'+portfolio+':'+org+':'+ring+':*'
        portfolio_index = 'irn:data:'+portfolio
        doc_index = org+':'+ring+':'+idx # _id was generated in the controller      
        
  
        try:
            
            response = self.data_table.get_item(Key={'portfolio_index': portfolio_index, 'doc_index': doc_index})        
            item = response.get('Item')

            if item:
                return item
            else:
                return {"error": "Document not found"}
        except ClientError as e:
            return {"error": e.response['Error']['Message']}
        
        
    
        
    def put_a_b_c(self, portfolio, org, ring, idx, item):
        # Construct the new primary key (partition key) and sort key
                                                                                                                                             
        # Set the partition and sort keys in the item
        portfolio_index = 'irn:data:'+portfolio
        doc_index = org+':'+ring+':'+item['_id']
        
        
        new_item = item.get('attributes', {})  # Retrieve the new attributes to update

        if not all([portfolio_index, doc_index, new_item]):
            return {'error': 'Missing portfolio_index, doc_index, or items'}

        try:
            # Fetch the current item based on the new partition key and sort key
            response_1 = self.data_table.get_item(Key={'portfolio_index': portfolio_index, 'doc_index': doc_index})
            if 'Item' not in response_1:
                return {'error': 'Item not found'}

            current_items = response_1['Item'].get('attributes', {})  # Retrieve the current attributes

            # Build the update expression for attributes
            update_expression = "set "  # This will hold the DynamoDB update expression
            expression_attribute_values = {}
            expression_attribute_names = {}
            updates = []

            # Compare new values with current values and build update expression
            for key, new_value in new_item.items():
                current_value = current_items.get(key)
                if new_value != current_value:
                    updates.append(f"attributes.#key{key} = :value{key}")
                    expression_attribute_values[f":value{key}"] = new_value
                    expression_attribute_names[f"#key{key}"] = key

            if not updates:
                return {'message': 'No changes detected'}

            # Add a timestamp for the "modified" field
            timestamp = datetime.now().isoformat()
            updates.append("modified = :modified")
            expression_attribute_values[":modified"] = timestamp

            # Combine the updates into a single update expression
            update_expression += ", ".join(updates)

            # Update the item in DynamoDB
            response_2 = self.data_table.update_item(
                Key={'portfolio_index': portfolio_index, 'doc_index': doc_index},  # New partition and sort keys
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
                ReturnValues="UPDATED_NEW"
            )

            return {'message': 'Item updated', 'response': str(response_2['Attributes'])}
        
        except ClientError as e:
            return {'error': str(e)}
                                                                                                                                        

   
    def delete_a_b_c(self,portfolio,org,ring,idx):

        #irn = 'irn:data:'+portfolio+':'+org+':'+ring+':*'
        portfolio_index = 'irn:data:'+portfolio
        doc_index = org+':'+ring+':'+idx # _id was generated in the controller      
        

        try: 
            response = self.data_table.delete_item(Key={'portfolio_index': portfolio_index, 'doc_index': doc_index})
            return {'message':'Item deleted', 'response':str(response)}
        except ClientError as e:
            return {'error': str(e)}
        
        
    
    #INDEX QUERIES
    
    def get_a_b_beginswith(self,query):
        
        '''
        Incoming object instance  
            {
            'portfolio':<portfolio_id>,
            'org':<org_id>,
            'ring':<ring_id>,
            'operator':'begins_with',
            'value':<value>,
            'limit':<page_limit>,
            'lastkey':<page_lastkey>,
            'sort': <asc|desc>
            }
        '''
        
        
        portfolio_index = f'irn:data:{query["portfolio"]}'  # This will be used as the partition key (PK)
        path_index_head = f'irn:h_index:{query["org"]}:{query["ring"]}'
        if 'value' not in query or not query['value']:
            path_index_tail = ''
        else:
            path_index_tail = f':{query["value"]}'
        path_index = path_index_head + path_index_tail
        
        
        try:
            # Build the query parameters with KeyConditionExpression
            query_params = {
                'TableName': self.table_name,  # Make sure this references the correct DynamoDB table
                'IndexName': 'path_index',  # Specify the name of your secondary index
                'KeyConditionExpression': Key('portfolio_index').eq(portfolio_index) & Key('path_index').begins_with(path_index),  # Include both keys
                'Limit': query['limit'],
                "ScanIndexForward": False if query['sort'] == 'asc' else True
            }
            
            print(f'portfolio_index >>> {portfolio_index}')
            print(f'path_index >>> {path_index}')
            print(f'QUERY PARAMS >>> {query_params}')

            # Add the ExclusiveStartKey to the query parameters if provided (for pagination)
            if query['lastkey']:
                query_params['ExclusiveStartKey'] = query['lastkey'] 

            # Query DynamoDB to get items with matching PK and SK prefix
            response = self.data_table.query(**query_params)
            
            # Extract items and pagination key
            items = response.get('Items', [])
            endkey = response.get('LastEvaluatedKey')  # Pagination key for next query

            # Build the result
            result = {
                'items': items,
                'lastkey': endkey  # This will be passed as 'lastkey' in the next call if needed
            }

            return result

        except (BotoCoreError, ClientError) as e:
            return {"error": str(e)}   
    
    
    def get_a_b_greaterthan(self,portfolio_index,path_index):
        
        return {'message': 'This feature is not implemented yet'}
    
    def get_a_b_lessthan(self,portfolio_index,path_index):
        
        return {'message': 'This feature is not implemented yet'}
    
    def get_a_b_equalto(self,portfolio_index,path_index):
        
        return {'message': 'This feature is not implemented yet'}

    def describe_table(self, logical_name="Ring data"):
        _describe_or_raise(
            self.dynamodb.meta.client,
            self.table_name,
            self.region,
            logical_name,
        )

    def scan_projected(self, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
        yield from _scan_projected(self.data_table, fields)

    def query_path_prefix_page(
        self,
        portfolio: str,
        org: str,
        ring: str,
        limit: int,
        last_evaluated_key: Optional[Dict] = None,
    ) -> Tuple[List[Dict], Optional[Dict]]:
        portfolio_index = f"irn:data:{portfolio}"
        path_index_prefix = f"irn:h_index:{org}:{ring}"
        kwargs = {
            "IndexName": "path_index",
            "KeyConditionExpression": Key("portfolio_index").eq(portfolio_index)
            & Key("path_index").begins_with(path_index_prefix),
            "Limit": limit,
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        response = self.data_table.query(**kwargs)
        return response.get("Items", []), response.get("LastEvaluatedKey")

    def delete_document(self, portfolio_index, doc_index):
        self.data_table.delete_item(
            Key={"portfolio_index": portfolio_index, "doc_index": doc_index}
        )

    def batch_get(self, keys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        client = self.data_table.meta.client
        table_name = self.data_table.name
        found: List[Dict[str, Any]] = []
        for start in range(0, len(keys), 100):
            pending = keys[start : start + 100]
            while pending:
                response = client.batch_get_item(RequestItems={table_name: {"Keys": pending}})
                found.extend(response.get("Responses", {}).get(table_name, []))
                pending = response.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        return found


def _describe_or_raise(dynamodb_client, table_name: str, region: str, logical_name: str) -> None:
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        if code == "ResourceNotFoundException":
            raise RuntimeError(
                f"{logical_name} table '{table_name}' was not found in region '{region}'. "
                f"Use --region, --env, --ring-table, or --graph-table."
            ) from exc
        raise RuntimeError(
            f"Failed to describe {logical_name} table '{table_name}' in region '{region}': {code} {msg}"
        ) from exc


def _scan_projected(table, fields: Iterable[str]) -> Iterator[Dict[str, Any]]:
    names = list(fields)
    expr_attr_names = {f"#f{i}": field for i, field in enumerate(names)}
    projection_expression = ", ".join(expr_attr_names.keys())
    last_evaluated_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "ProjectionExpression": projection_expression,
            "ExpressionAttributeNames": expr_attr_names,
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        response = table.scan(**kwargs)
        for item in response.get("Items", []):
            yield item
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    
    
    
