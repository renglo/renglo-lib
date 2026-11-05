import logging
import secrets
import string

from renglo.config_names import setting
import boto3
from botocore.exceptions import ClientError

from renglo.wl import build_raw_email

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


class Identity:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        
        # Cognito and email.
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

            # The app user id is the hash of Cognito Username, the same
            # string the ID token puts in cognito:username. For a password
            # user that username equals sub. For Google it is Google_<subject>
            # and sub is a different id.
            matches = []
            for user in response.get('Users') or []:
                username = str(user.get('Username') or '').strip()
                if not username:
                    continue
                sub = next(
                    (attr['Value'] for attr in user.get('Attributes') or [] if attr.get('Name') == 'sub'),
                    None,
                )
                matches.append({
                    'username': username,
                    'sub': sub,
                    'status': user.get('UserStatus') or '',
                })

            if matches:
                return {
                    "success": True,
                    "message": "User found",
                    "document": {
                        'email': email,
                        'cognito_username': matches[0]['username'],
                        'cognito_user_id': matches[0].get('sub'),
                        'matches': matches,
                    },
                    "status": 200,
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



    def send_email(
        self,
        sender,
        recipient,
        subject,
        body_text,
        body_html,
        inline_images=None,
    ):
        region = (
            setting(self.config, "REGION")
            or self.config.get('AWS_DEFAULT_REGION')
            or self.config.get('COGNITO_REGION')
            or 'us-east-1'
        )
        ses_client = boto3.client('ses', region_name=region)

        try:
            if inline_images:
                raw = build_raw_email(
                    sender=sender,
                    recipient=recipient,
                    subject=subject,
                    body_text=body_text,
                    body_html=body_html,
                    inline_images=inline_images,
                )
                response = ses_client.send_raw_email(
                    Source=sender,
                    Destinations=[recipient],
                    RawMessage={'Data': raw},
                )
            else:
                response = ses_client.send_email(
                    Source=sender,
                    Destination={'ToAddresses': [recipient]},
                    Message={
                        'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                        'Body': {
                            'Text': {'Data': body_text, 'Charset': 'UTF-8'},
                            'Html': {'Data': body_html, 'Charset': 'UTF-8'},
                        },
                    },
                )

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
            


    





