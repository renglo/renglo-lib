import logging
import secrets
import string
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid
from decimal import Decimal

logger = logging.getLogger(__name__)


def _cognito_temp_password(length: int = 20) -> str:
    """Generate a one-time password that satisfies default Cognito policy."""
    alphabet = string.ascii_letters + string.digits + '!@#$%^&*'
    while True:
        pwd = ''.join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(c.islower() for c in pwd)
            and any(c.isupper() for c in pwd)
            and any(c.isdigit() for c in pwd)
            and any(c in '!@#$%^&*' for c in pwd)
        ):
            return pwd


class AuthModel:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        
        #Dynamo
        self.dynamodb = boto3.resource('dynamodb')
        entity_table_name = self.config.get('DYNAMODB_ENTITY_TABLE', 'default_entity_table')
        rel_table_name = self.config.get('DYNAMODB_REL_TABLE', 'default_rel_table')
        self.entity_table = self.dynamodb.Table(entity_table_name)
        self.rel_table = self.dynamodb.Table(rel_table_name)

        #SES
        cognito_region = self.config.get('COGNITO_REGION', 'us-east-1')
        self.cognito_client = boto3.client('cognito-idp', region_name=cognito_region) 
        self.USER_POOL_ID = self.config.get('COGNITO_USERPOOL_ID', '')
        self.COGNITO_APP_CLIENT_ID = self.config.get('COGNITO_APP_CLIENT_ID', '')


 #-------------------------------------------------AWS COGNITO


    def check_user_by_email(self,email):
        try:
            # Get the email from the request
            #email = request.json.get('email')
            if not email:
                return {'success': False, 'error': 'Email is required', 'status': 400}

            # List users by email filter
            response = self.cognito_client.list_users(
                UserPoolId=self.USER_POOL_ID,
                Filter=f'email = "{email}"'  # Filter by email
            )

            # Check if a user was found
            if response['Users']:
                user = response['Users'][0]  # Get the first user from the response

                # Extract Cognito User ID (the 'sub' attribute)
                cognito_user_id = next(
                    (attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'sub'), 
                    None
                )

                if cognito_user_id:
                    return {
                        "success":True, 
                        "message": "User found", 
                        "document": {'email':email,'cognito_user_id':cognito_user_id},
                        "status" : 200
                    } 
                        
            return {
                "success":False, 
                "message": "User not found",
                "status" : 404
            }

        except self.cognito_client.exceptions.UserNotFoundException:
            return {
                "success":False, 
                "message": "User not found (UserNotFoundException)",
                "status" : 404
            }
        except Exception as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
            }
        
    def cognito_user_delete(self, username):
        """Remove a Cognito user. Used to roll back a failed invite create."""
        try:
            response = self.cognito_client.admin_delete_user(
                UserPoolId=self.USER_POOL_ID,
                Username=username,
            )
            return {
                'success': True,
                'message': 'User deleted',
                'document': response,
                'status': 200,
            }
        except ClientError as e:
            return {
                'success': False,
                'message': e.response.get('Error', {}).get('Message', str(e)),
                'status': e.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 400),
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'status': 400,
            }

    def cognito_user_create_with_permanent_password(self, email, password, first='FIRST', last='LAST'):
        """
        Create an invitee Cognito user already CONFIRMED (no NEW_PASSWORD_REQUIRED).

        AdminCreateUser always starts users in FORCE_CHANGE_PASSWORD; we immediately
        assign the user-chosen password as permanent. On failure after create, delete
        the Cognito user so they are not stuck unable to reset password.
        """
        created = False
        try:
            create_response = self.cognito_client.admin_create_user(
                UserPoolId=self.USER_POOL_ID,
                Username=email,
                UserAttributes=[
                    {'Name': 'email', 'Value': email},
                    {'Name': 'email_verified', 'Value': 'true'},
                    {'Name': 'given_name', 'Value': first},
                    {'Name': 'family_name', 'Value': last},
                ],
                TemporaryPassword=_cognito_temp_password(),
                MessageAction='SUPPRESS',
            )
            created = True

            self.cognito_client.admin_set_user_password(
                UserPoolId=self.USER_POOL_ID,
                Username=email,
                Password=password,
                Permanent=True,
            )

            return {
                'success': True,
                'message': 'User created with permanent password',
                'document': create_response,
                'status': 200,
            }
        except ClientError as e:
            if created:
                cleanup = self.cognito_user_delete(email)
                if not cleanup.get('success'):
                    logger.warning(
                        'Failed to roll back Cognito user %s after permanent password error: %s',
                        email,
                        cleanup.get('message'),
                    )
            return {
                'success': False,
                'message': e.response.get('Error', {}).get('Message', str(e)),
                'status': e.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 400),
            }
        except Exception as e:
            if created:
                cleanup = self.cognito_user_delete(email)
                if not cleanup.get('success'):
                    logger.warning(
                        'Failed to roll back Cognito user %s after permanent password error: %s',
                        email,
                        cleanup.get('message'),
                    )
            return {
                'success': False,
                'message': str(e),
                'status': 400,
            }

    def cognito_user_permanent_password_assign(self, email, password):
        try:
            response = self.cognito_client.admin_set_user_password(
                UserPoolId=self.USER_POOL_ID,
                Username=email,
                Password=password,
                Permanent=True,
            )
            return {
                'success': True,
                'message': 'Password assigned',
                'document': response,
                'status': 200,
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'status': 400,
            }

    def cognito_user_create(self, email, first='FIRST', last='LAST'):
        """Create a Cognito user with a temporary password (FORCE_CHANGE_PASSWORD)."""
        try:
            response = self.cognito_client.admin_create_user(
                UserPoolId=self.USER_POOL_ID,
                Username=email,
                UserAttributes=[
                    {'Name': 'email', 'Value': email},
                    {'Name': 'email_verified', 'Value': 'true'},
                    {'Name': 'given_name', 'Value': first},
                    {'Name': 'family_name', 'Value': last},
                ],
                TemporaryPassword=_cognito_temp_password(),
                MessageAction='SUPPRESS',
            )
            return {
                'success': True,
                'message': 'User created successfully',
                'document': response,
                'status': 200,
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'status': 400,
            }


    def cognito_update_user_attributes(self, username, *, given_name=None, family_name=None):
        """Update standard Cognito profile attributes for an existing pool user."""
        attributes = []
        if given_name is not None:
            attributes.append({'Name': 'given_name', 'Value': given_name})
        if family_name is not None:
            attributes.append({'Name': 'family_name', 'Value': family_name})
        if not attributes:
            return {
                'success': True,
                'message': 'Nothing to update',
                'status': 200,
            }
        try:
            response = self.cognito_client.admin_update_user_attributes(
                UserPoolId=self.USER_POOL_ID,
                Username=username,
                UserAttributes=attributes,
            )
            return {
                'success': True,
                'message': 'Cognito user updated',
                'document': response,
                'status': 200,
            }
        except ClientError as e:
            return {
                'success': False,
                'message': str(e),
                'status': 400,
            }


    # NOT USED — admin NEW_PASSWORD_REQUIRED is completed in the console (authInvite).
    def cognito_user_login_challenge(self, email, temporary_password, new_password):
        try:
            auth_response = self.cognito_client.admin_initiate_auth(
                UserPoolId=self.USER_POOL_ID,
                ClientId=self.COGNITO_APP_CLIENT_ID,
                AuthFlow='ADMIN_NO_SRP_AUTH',
                AuthParameters={
                    'USERNAME': email,
                    'PASSWORD': temporary_password,
                },
            )

            if auth_response.get('ChallengeName') == 'NEW_PASSWORD_REQUIRED':
                challenge_response = self.cognito_client.respond_to_auth_challenge(
                    ClientId=self.COGNITO_APP_CLIENT_ID,
                    ChallengeName='NEW_PASSWORD_REQUIRED',
                    ChallengeResponses={
                        'USERNAME': email,
                        'NEW_PASSWORD': new_password,
                    },
                    Session=auth_response['Session'],
                )
                return {
                    'success': True,
                    'message': 'Password changed successfully. User is now authenticated.',
                    'document': challenge_response['AuthenticationResult'],
                    'status': 200,
                }

            return {
                'success': False,
                'message': 'Unexpected challenge. Expected NEW_PASSWORD_REQUIRED.',
                'status': 400,
            }

        except self.cognito_client.exceptions.NotAuthorizedException:
            return {'success': False, 'message': 'Invalid temporary password', 'status': 401}
        except Exception as e:
            return {'success': False, 'message': str(e), 'status': 500}

            





#---------------------------------------------------- AWS SES



    def send_email(self, sender, recipient, subject, body_text, body_html):
        region = (
            self.config.get('AWS_REGION')
            or self.config.get('AWS_DEFAULT_REGION')
            or self.config.get('COGNITO_REGION')
            or 'us-east-1'
        )
        ses_client = boto3.client('ses', region_name=region)

        # Email details
        email_data = {
            'Source': sender,
            'Destination': {
                'ToAddresses': [
                    recipient,
                ],
            },
            'Message': {
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': body_text,
                        'Charset': 'UTF-8'
                    },
                    'Html': {
                        'Data': body_html,
                        'Charset': 'UTF-8'
                    }
                }
            }
        }

        try:
            # Send the email
            response = ses_client.send_email(**email_data)

            if response['MessageId']:
                return{
                    "success":True, 
                    "message": "Email sent", 
                    "document": {
                        'MessageId':response['MessageId']
                        },
                    "status" : response['ResponseMetadata']['HTTPStatusCode']
                }
 
        except ClientError as e:
            '''
            example e: 'Email address is not verified. The following identities failed the check in region US-EAST-1: user@email.com'
            '''
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "document": e.response,
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
            }
            


    





#-------------------------------------------------MODEL/ENTITIES


    def list_entity(self,index,limit=50,lastkey=None):

        try:
            # Build the query parameters
            query_params = {
                'KeyConditionExpression': boto3.dynamodb.conditions.Key('index').eq(index),
                'Limit': int(limit)
            }
            
            # Add the ExclusiveStartKey to the query parameters if provided
            if lastkey:
                query_params['ExclusiveStartKey'] = {'index': index, 'ref': lastkey}

            # Query DynamoDB to get items with the same partition key
            response = self.entity_table.query(**query_params)
            items = response.get('Items', [])
            endkey = response.get('LastEvaluatedKey') # This will become the first in the next page 

            documents = {
                "items": items,
                "lastkey": endkey
            }

            return {
                "success":True, 
                "message": "Documents found", 
                "document": documents,
                "status" : response['ResponseMetadata']['HTTPStatusCode']
            }
        
        except ClientError as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }



    def get_entity(self,index,id):
   
        try:
            logger.debug('INDEX:'+index)
            logger.debug('ID:'+id)
            response = self.entity_table.get_item(Key={'index':index,'_id':id})
            item = response.get('Item')
            logger.debug('MODEL: get_entity:')
            logger.debug(response)
            logger.debug('MODEL: item:')
            logger.debug(item)
            

            if item:
                #return item
                return {
                    "success":True, 
                    "message": "Entity found", 
                    "document": item,
                    "status" : response['ResponseMetadata']['HTTPStatusCode']
                    }
            else:
                return {
                    "success":False, 
                    "message": "Entity not found",
                    "status" : 404
                    }
        except ClientError as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
        
    
    def create_entity(self,data):

        data['modified'] = datetime.now().isoformat()
        
        try:
            response = self.entity_table.put_item(Item=data)
            logger.debug('MODEL: Created entity successfully:'+str(data))
            return {
                "success":True, 
                "message": "Entity created", 
                "document": data,
                "status" : response['ResponseMetadata']['HTTPStatusCode']
                }
        except ClientError as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "document": data,
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
        


    def update_entity(self,data):

        data['modified'] = datetime.now().isoformat()
        
        try:
            response = self.entity_table.put_item(Item=data)
            #logger.debug('MODEL: Updated entity successfully')
            return {
                "success":True, 
                "message": "Entity updated", 
                "document": data,
                "status" : response['ResponseMetadata']['HTTPStatusCode']
                }
        except ClientError as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "document": data,
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
        



    def delete_entity(self,**entity_document):

        keys = {
            'index': entity_document['index'],
            '_id': entity_document['_id']
        }

        try:
            response = self.entity_table.delete_item(Key=keys)
            logger.debug('MODEL: Deleted Entity:' + str(entity_document))
            return {
                "success":True,
                "message": "Entity deleted", 
                "document": entity_document,
                "status" : response['ResponseMetadata']['HTTPStatusCode'] 
                }
        
        except ClientError as e:
            return {
                "success":False,
                "message": e.response['Error']['Message'],
                "document": rel_document,
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }




    def get_rel(self,index,rel):
   
        try:
            response = self.rel_table.get_item(Key={'index':index,'rel':rel})
            item = response.get('Item')

            if item:
                #return item
                return {
                    "success":True, 
                    "message": "Entity found", 
                    "document": item,
                    "status" : 200
                    }
            else:
                return {
                    "success":False, 
                    "message": "Entity not found",
                    "status" : 404
                    }
        except ClientError as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
        

    
    def list_rel(self,index,limit=50,lastkey=None):

        try:
            # Build the query parameters
            query_params = {
                'KeyConditionExpression': boto3.dynamodb.conditions.Key('index').eq(index),
                'Limit': int(limit)
            }
            
            # Add the ExclusiveStartKey to the query parameters if provided
            if lastkey:
                query_params['ExclusiveStartKey'] = {'index': index, 'ref': lastkey}

            # Query DynamoDB to get items with the same partition key
            response = self.rel_table.query(**query_params)
            items = response.get('Items', [])
            endkey = response.get('LastEvaluatedKey') # This will become the first in the next page 

            documents = {
                "items": items,
                "lastkey": endkey
            }

            return {
                "success":True, 
                "message": "Documents found", 
                "document": documents,
                "status" : response['ResponseMetadata']['HTTPStatusCode']
            }
        
        except ClientError as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
        

    def list_rel_prefix(self,partition_key_value,prefix):
        

        if not partition_key_value or not prefix:
            return {
                    "success":False, 
                    "message": 'Partition key and prefix are required',
                    "status" : 400
                    }

        try:
            # Query the table with the begins_with function on the sort key
            response = self.rel_table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('index').eq(partition_key_value) &
                                    boto3.dynamodb.conditions.Key('rel').begins_with(prefix)
            )

        
            return {
                "success":True, 
                "message": "Documents found", 
                "document": response['Items'],
                "status" : response['ResponseMetadata']['HTTPStatusCode']
            }

        except Exception as e:
            return {
                "success":False, 
                "message": e.response['Error']['Message'],
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
   
        



    def create_rel(self, **rel_document):

        
        try:
            response = self.rel_table.put_item(Item=rel_document)
            logger.debug('MODEL: Created Relationship:' + str(rel_document))
            return {
                "success":True,
                "message": "Rel created", 
                "document": rel_document,
                "status" : response['ResponseMetadata']['HTTPStatusCode'] 
                }
        
        except ClientError as e:
            return {
                "success":False,
                "message": e.response['Error']['Message'],
                "document": rel_document,
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }
        


    def delete_rel(self, **rel_document):

        keys = {
            'index': rel_document['index'],
            'rel': rel_document['rel']
        }

        try:
            response = self.rel_table.delete_item(Key=keys)
            logger.debug('MODEL: Deleted Relationship:' + str(rel_document))
            return {
                "success":True,
                "message": "Rel deleted", 
                "document": rel_document,
                "status" : response['ResponseMetadata']['HTTPStatusCode'] 
                }
        
        except ClientError as e:
            return {
                "success":False,
                "message": e.response['Error']['Message'],
                "document": rel_document,
                "status" : e.response['ResponseMetadata']['HTTPStatusCode']
                }







    