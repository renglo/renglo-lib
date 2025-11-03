WL_NAME='enerclave'

TANK_BASE_URL = 'https://enerclave.renglo.com'
TANK_FE_BASE_URL = 'https://enerclave-1.renglo.com'
TANK_DOC_BASE_URL = 'https://enerclave.renglo.com'
TANK_AWS_REGION = 'us-east-1'

#Crontab/Cronjob
TANK_API_GATEWAY_ARN = 'arn:aws:execute-api:us-east-1:*:*'
TANK_ROLE_ARN = 'arn:aws:iam::*:role/enerclave_tt_role'
TANK_ENV = 'enerclave'

# DynamoDB
DYNAMODB_ENTITY_TABLE = 'enerclave_entities'
DYNAMODB_BLUEPRINT_TABLE = 'enerclave_blueprints'
DYNAMODB_RINGDATA_TABLE = 'enerclave_data'
DYNAMODB_REL_TABLE = 'enerclave_rel'
DYNAMODB_CHAT_TABLE = 'enerclave_chat'

CSRF_SESSION_KEY = '765e9566b1f9ac7a88cb7a145f4b6a2b2445dbb1861abff400fd654dc81cdcfc'
SECRET_KEY = '3a2e6546bc7b4ec8158262db2b97738c6110e5a391d342fbc62ddbe39dc6d46e'

# flask_cognito
COGNITO_REGION = 'us-east-1'
COGNITO_USERPOOL_ID = 'us-east-1_uUfQHPxzw'
COGNITO_APP_CLIENT_ID = '3f8ns7amer3cosn5h9o8f4lmiq'
COGNITO_CHECK_TOKEN_EXPIRATION = True

#UI
PREVIEW_LAYER = 2

#-----------
S3_BUCKET_NAME = 'enerclave-47436325'

#---------
#OPEN AI
OPENAI_API_KEY='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'

#---------
#WEB SOCKET
WEBSOCKET_CONNECTIONS=''

#--------
ALLOW_DEV_ORIGINS = False
