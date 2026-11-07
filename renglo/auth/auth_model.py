"""Auth documents, with the directory selected by CLOUD.

Entity and relationship rows use the document store for the auth subsystem.
User lookup, invite create, password, and profile updates go through
identity_aws or identity_azure. Mail stays on the AWS module.
"""

import logging
from datetime import datetime

from renglo.auth.entity_status import HARD_DELETE_REFUSED, entity_forbids_hard_delete
from renglo.config_names import table_name
from renglo.document_stores.select import Conflict, StoreError, open_store
from renglo.submodel import load_submodel

logger = logging.getLogger(__name__)

class AuthModel:

    def __init__(self, config=None, tid=False, ip=False, *, entity_store=None, rel_store=None):
        self.config = config or {}
        self._identity_impl = None
        entity_table_name = table_name(self.config, "ENTITY_TABLE", "default_entity_table")
        rel_table_name = table_name(self.config, "REL_TABLE", "default_rel_table")
        self.entity_store = entity_store or open_store(
            self.config, "auth", entity_table_name, "index", "_id", use_profile=False
        )
        self.rel_store = rel_store or open_store(
            self.config, "auth", rel_table_name, "index", "rel", use_profile=False
        )

    def _identity(self):
        if self._identity_impl is None:
            cls = load_submodel("renglo.auth.providers", "identity", "Identity", self.config)
            self._identity_impl = cls(self.config)
        return self._identity_impl

    def lookup_by_email(self, email):
        return self._identity().lookup_by_email(email)

    def check_user_by_email(self, email):
        return self.lookup_by_email(email)

    def delete_user(self, username):
        return self._identity().delete_user(username)

    def create_user_with_password(self, email, password, first="FIRST", last="LAST"):
        return self._identity().create_user_with_password(email, password, first, last)

    def set_password(self, email, password):
        return self._identity().set_password(email, password)

    def create_user(self, email, first="FIRST", last="LAST"):
        return self._identity().create_user(email, first, last)

    def update_profile(self, username, *, given_name=None, family_name=None):
        return self._identity().update_profile(
            username, given_name=given_name, family_name=family_name
        )

    def send_email(self, sender, recipient, subject, body_text, body_html, inline_images=None):
        return self._identity().send_email(
            sender, recipient, subject, body_text, body_html, inline_images
        )

    def list_entity(self,index,limit=50,lastkey=None):

        try:
            continuation = {'index': index, 'ref': lastkey} if lastkey else None
            items, endkey = self.entity_store.query(
                index, limit=int(limit), continuation=continuation
            )
            documents = {
                "items": items,
                "lastkey": endkey
            }
            return {
                "success":True, 
                "message": "Documents found", 
                "document": documents,
                "status" : 200
            }
        
        except StoreError as e:
            return {
                "success":False, 
                "message": e.message,
                "status" : e.status
                }



    def get_entity(self,index,id):
   
        try:
            logger.debug('INDEX:'+index)
            logger.debug('ID:'+id)
            item = self.entity_store.get(index, id)
            logger.debug('MODEL: get_entity:')
            logger.debug(item)
            

            if item:
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
        except StoreError as e:
            return {
                "success":False, 
                "message": e.message,
                "status" : e.status
                }
        
    
    def put_entity_exclusive(self, data):
        """Insert an entity only when that index/_id pair does not exist yet."""
        data['modified'] = datetime.now().isoformat()
        try:
            status = self.entity_store.create(data)
            return {
                "success": True,
                "message": "Entity created",
                "document": data,
                "status": status,
            }
        except Conflict as e:
            return {
                "success": False,
                "message": e.message,
                "document": data,
                "status": 409,
            }
        except StoreError as e:
            return {
                "success": False,
                "message": e.message,
                "document": data,
                "status": e.status,
            }


    def create_entity(self,data):

        data['modified'] = datetime.now().isoformat()
        
        try:
            status = self.entity_store.put(data)
            logger.debug('MODEL: Created entity successfully:'+str(data))
            return {
                "success":True, 
                "message": "Entity created", 
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
        


    def update_entity(self,data):

        data['modified'] = datetime.now().isoformat()
        
        try:
            status = self.entity_store.put(data)
            return {
                "success":True, 
                "message": "Entity updated", 
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
        



    def delete_entity(self,**entity_document):

        # Team and extension deletes call this helper. Portfolio and org rows
        # stay in the table and are hidden by status instead.
        if entity_forbids_hard_delete(entity_document):
            logger.debug('MODEL: Refused hard delete of portfolio or org')
            return {
                "success": False,
                "message": HARD_DELETE_REFUSED,
                "document": entity_document,
                "status": 400,
            }

        keys = {
            'index': entity_document['index'],
            '_id': entity_document['_id']
        }

        try:
            status = self.entity_store.delete(keys['index'], keys['_id'])
            logger.debug('MODEL: Deleted Entity:' + str(entity_document))
            return {
                "success":True,
                "message": "Entity deleted", 
                "document": entity_document,
                "status" : status
                }
        
        except StoreError as e:
            return {
                "success":False,
                "message": e.message,
                "document": rel_document,
                "status" : e.status
                }




    def get_rel(self,index,rel):
   
        try:
            item = self.rel_store.get(index, rel)

            if item:
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
        except StoreError as e:
            return {
                "success":False, 
                "message": e.message,
                "status" : e.status
                }
        

    
    def list_rel(self,index,limit=50,lastkey=None):

        try:
            continuation = {'index': index, 'ref': lastkey} if lastkey else None
            items, endkey = self.rel_store.query(
                index, limit=int(limit), continuation=continuation
            )
            documents = {
                "items": items,
                "lastkey": endkey
            }
            return {
                "success":True, 
                "message": "Documents found", 
                "document": documents,
                "status" : 200
            }
        
        except StoreError as e:
            return {
                "success":False, 
                "message": e.message,
                "status" : e.status
                }
        

    def list_rel_prefix(self,partition_key_value,prefix):
        

        if not partition_key_value or not prefix:
            return {
                    "success":False, 
                    "message": 'Partition key and prefix are required',
                    "status" : 400
                    }

        try:
            items, _token = self.rel_store.query(partition_key_value, prefix=prefix)
            return {
                "success":True, 
                "message": "Documents found", 
                "document": items,
                "status" : 200
            }

        except StoreError as e:
            return {
                "success":False, 
                "message": e.message,
                "status" : e.status
                }
   
        



    def create_rel(self, **rel_document):

        
        try:
            status = self.rel_store.put(rel_document)
            logger.debug('MODEL: Created Relationship:' + str(rel_document))
            return {
                "success":True,
                "message": "Rel created", 
                "document": rel_document,
                "status" : status
                }
        
        except StoreError as e:
            return {
                "success":False,
                "message": e.message,
                "document": rel_document,
                "status" : e.status
                }
        


    def delete_rel(self, **rel_document):

        keys = {
            'index': rel_document['index'],
            'rel': rel_document['rel']
        }

        try:
            status = self.rel_store.delete(keys['index'], keys['rel'])
            logger.debug('MODEL: Deleted Relationship:' + str(rel_document))
            return {
                "success":True,
                "message": "Rel deleted", 
                "document": rel_document,
                "status" : status
                }
        
        except StoreError as e:
            return {
                "success":False,
                "message": e.message,
                "document": rel_document,
                "status" : e.status
                }







    
