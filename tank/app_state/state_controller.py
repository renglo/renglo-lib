from flask import redirect,url_for, jsonify, current_app, session, request
import urllib.parse
import requests
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid
from decimal import Decimal
from tank.app_state.state_model import StateModel
from tank.app_auth.auth_controller import AuthController


class StateController:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.STM = StateModel(config=self.config)
        self.AUC = AuthController(config=self.config, tid=tid, ip=ip)    

    def get_state(self,name,v):
        
        return self.STM.get_state(name,v)
    

