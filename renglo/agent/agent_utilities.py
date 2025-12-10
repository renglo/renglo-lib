from renglo.data.data_controller import DataController
from renglo.docs.docs_controller import DocsController
from renglo.chat.chat_controller import ChatController
from renglo.schd.schd_controller import SchdController

from openai import OpenAI

import random
import json
import boto3
from datetime import datetime
from typing import List, Dict, Any, Callable
import re
from decimal import Decimal
import time
import uuid

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


class AgentUtilities:
    def __init__(self, 
                 config, 
                 portfolio, 
                 org, 
                 entity_type, 
                 entity_id, 
                 thread,
                 connection_id = None
                 ):
        """
        Initialize AgentUtilities with configuration and required parameters.
        
        Args:
            config (dict): Configuration dictionary containing API keys, URLs, etc.
        
        Args:
            portfolio (str): Portfolio identifier
            org (str): Organization identifier  
            entity_type (str): Type of entity
            entity_id (str): Entity identifier
            thread (str): Thread identifier
        """
        self.config = config or {}
        self.portfolio = portfolio
        self.org = org
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.thread = thread
        self.chat_id = None
        self.connection_id = connection_id
        
        # OpenAI Client
        try:    
            openai_key = self.config.get('OPENAI_API_KEY', '')
            self.AI_1 = OpenAI(api_key=openai_key)
            print(f"OpenAI client initialized")
        except Exception as e:
            print(f"Error initializing OpenAI client: {e}")
            self.AI_1 = None

        self.AI_1_MODEL = "gpt-3.5-turbo"  # Baseline model. Good for multi-step chats
        self.AI_2_MODEL = "gpt-4o-mini"    # This model is not very smart
        
        # Initialize controllers
        self.DAC = DataController(config=self.config)
        self.DCC = DocsController(config=self.config)
        self.CHC = ChatController(config=self.config)
        self.SHC = SchdController(config=self.config)
        
        # Initialize WebSocket client
        try:
            websocket_url = self.config.get('WEBSOCKET_CONNECTIONS', '')
            self.apigw_client = boto3.client("apigatewaymanagementapi", endpoint_url=websocket_url)
        except Exception as e:
            print(f"Error initializing WebSocket client: {e}")
            self.apigw_client = None

    def get_message_history(self):
        """
        Get the message history for the current thread.
        
        Returns:
            dict: Success status and message list
        """
        action = 'get_message_history'
        
        try:
            print(f'type: {self.entity_type}')
            print(f'entity_id: {self.entity_id}')
            print(f'thread: {self.thread}')
        
            # Thread was not included, create a new one?
            if not self.thread:
                return {'success': False, 'action': action, 'output': 'Error: No thread provided'}
                
            response = self.CHC.list_turns(
                self.portfolio,
                self.org,
                self.entity_type,
                self.entity_id,
                self.thread
            )
             
            if 'success' not in response:
                print(f'Something failed during message list: {response}')
                return {'success': False, 'action': action, 'input': self.thread, 'output': response}
            
            # Prepare messages to look like an OpenAI message array
            # Also remove messages that don't belong to an approved type
            message_list = []
            for turn in response['items']: 
                for m in turn['messages']:
                    out_message = m['_out']
                    if m['_type'] in ['user', 'system', 'text', 'tool_rq', 'tool_rs']:  # OK to show to LLM
                        message_list.append(out_message)      
            
            return {'success': True, 'action': action, 'input': self.thread, 'output': message_list}
        
        except Exception as e:
            print(f'Get message history failed: {str(e)}')
            return {'success': False, 'action': action, 'output': f'Error: {str(e)}'}

    def update_chat_message_document(self, update, call_id=False):
        """
        Update a chat message document.
        
        Args:
            update (dict): The update to apply
            call_id (bool): Whether to use call_id
            
        Returns:
            dict: Success status and response
        """
        action = 'update_chat_message_document'
        print(f'Running: {action}')
        
        try:
            response = self.CHC.update_turn(
                self.portfolio,
                self.org,
                self.entity_type,
                self.entity_id,
                self.thread,
                self.chat_id,
                update,
                call_id=call_id
            )
            
            if 'success' not in response:
                print(f'Something failed during update chat message {response}')
                return {'success': False, 'action': action, 'input': update, 'output': response}
            
            return {'success': True, 'action': action, 'input': update, 'output': response}
        
        except Exception as e:
            print(f'Update chat message failed: {str(e)}')
            return {'success': False, 'action': action, 'output': f'Error: {str(e)}'}

    def update_workspace_document(self, update, workspace_id):
        """
        Update a workspace document.
        
        Args:
            update (dict): The update to apply
            workspace_id (str): The workspace ID
            
        Returns:
            dict: Success status and response
        """
        action = 'update_workspace_document'
        print(f'Running: {action}')
        
        response = self.CHC.update_workspace(
            self.portfolio,
            self.org,
            self.entity_type,
            self.entity_id,
            self.thread,
            workspace_id,
            update
        )
        
        if 'success' not in response:
            return {'success': False, 'action': action, 'input': update, 'output': response}
        
        return {'success': True, 'action': action, 'input': update, 'output': response}



    def save_chat(self, output, interface=None, connection_id=None, c_id=None, msg_type=None):
        """
        Save chat message to storage and context.
        
        Args:
            output (dict): The output to save
            interface (bool): Whether to use interface
            
        Input:
            {
                'role':'',
                'tool_calls':'',
                'content':''     
            }
        """
        if msg_type == 'consent':
            print('Sending consent form')
            # This is a consent request from the agent to the user
            message_type = 'consent'
            if not interface:
                interface = 'binary_consent'
            doc = {'_out': self.sanitize(output), '_type': 'consent','_interface':interface,'_next': c_id}
            self.update_chat_message_document(doc)
            self.print_chat(doc,message_type,connection_id=connection_id)
            
            
        elif output.get('tool_calls') and output.get('role') == 'assistant':
            print('Saving the tool call')
            # This is a tool call
            message_type = 'tool_rq'
            doc = {'_out': self.sanitize(output), '_type': 'tool_rq'}
            # Memorize to permanent storage
            self.update_chat_message_document(doc)      
            
            # Creating empty placeholders corresponding to each one of the un-executed tool calls.
            # This was a work-around as OpenAI doesn't like to see a tools_calls without its corresponding response.
            # It happens because sometimes, the chat messages are passed to the LLM before the tool is executed 
            # (e.g: Asking the user for approval to use a tool, the agent needs to understand the response using an LLM)
            for tool_call in output['tool_calls']:
                rs_template = {
                    "role": "tool",
                    "tool_call_id": tool_call['id'],
                    "content": []
                }
                doc_rs_placeholder = {'_out': rs_template, '_type': 'tool_rs'}
                self.update_chat_message_document(doc_rs_placeholder)
                            
        elif output.get('content') and output.get('role') == 'assistant':
            print('Saving the assistant message to the user')
            # This is a human readable message from the agent to the user
            message_type = 'text'
            doc = {'_out': self.sanitize(output), '_type': message_type, '_next': c_id}
            # Memorize to permanent storage
            response_1 = self.update_chat_message_document(doc)
            print(f'Chat update response:',response_1)
            # Print to live chat
            self.print_chat(output, message_type, as_is=True, connection_id=connection_id)
            # Print to API
            self.print_api(output['content'], message_type)
            
        elif 'tool_call_id' in output and 'role' in output and output['role'] == 'tool':
            # This is a response from the tool
            print(f'Including Tool Response in the chat: {output}')
            # This is the tool response
            message_type = 'tool_rs'            
            doc = {'_out': self.sanitize(output), '_type': message_type, '_interface': interface, '_next': c_id}
            # Memorize to permanent storage
            self.update_chat_message_document(doc, output['tool_call_id'])
              
            if interface:  
                self.print_chat(doc, message_type, as_is=True, connection_id=connection_id)


    def print_api(self, message, type='text', public_user=None):
        """
        Print message to API.
        
        Args:
            message (str): The message to print
            type (str): The message type
            public_user (str): The public user identifier
            
        Returns:
            dict: Success status and response
        """
        action = 'print_api'
        

        callback_msg_handler = self.config.get('CALLBACK_MSG_HANDLER', False)
        
         
        try:
            if callback_msg_handler:
                if public_user:
                    target = public_user
                else:
                    return {'success': False, 'action': action, 'input': message, 'output': 'This is an internal call, no API output is needed'}
               
                params = {'message': message, 'type': type, 'target': target}  
                
                parts = callback_msg_handler.split('/')
                if len(parts) != 2:
                    error_msg = f"{callback_msg_handler} is not a valid tool."
                    print(error_msg)
                    self.print_chat(error_msg, 'text')
                    raise ValueError(error_msg)
                
                print(f'Calling {callback_msg_handler}') 
                response = self.SHC.handler_call(self.portfolio, self.org, parts[0], parts[1], params)
                
                return response
            else:
                return {'success': False, 'action': action, 'input': message, 'output': ''}
                
        except ValueError as ve:
            print(f"ValueError in {action}: {ve}")
            return {'success': False, 'action': action, 'input': message, 'output': str(ve)}
        except Exception as e:
            print(f"Error in {action}: {e}")
            return {'success': False, 'action': action, 'input': message, 'output': str(e)}

    def print_chat(self, output, type='text', as_is=False, connection_id=None, c_id = None):
        """
        Print message to chat via WebSocket.
        
        Args:
            output: The output to print
            type (str): The message type
            as_is (bool): Whether to use output as-is
            connection_id (str): The WebSocket connection ID
            
        Returns:
            bool: Success status
        """
        print(f'Running: Print Chat: {output}')
        
        if not connection_id:
            #Try the context
            connection_id = self.connection_id
        
        if as_is:
            doc = output  
        elif isinstance(output, dict) and 'role' in output and 'content' in output and output['role'] and output['content']: 
            # Content responses from LLM  
            doc = {'_out': {'role': output['role'], 'content': self.sanitize(output['content'])}, '_type': type, '_next:':c_id}      
        elif isinstance(output, str):
            # Any text response
            doc = {'_out': {'role': 'assistant', 'content': str(output)}, '_type': type, '_next:':c_id}     
        else:
            # Everything else
            doc = {'_out': {'role': 'assistant', 'content': self.sanitize(output)}, '_type': type, '_next:':c_id} 
            
        if not connection_id or not self.apigw_client:
            #print(f'WebSocket not configured or this is a RESTful post to the chat.')
            return False
             
        try:
            print(f'Sending Real Time Message to: {connection_id}')
            
            # WebSocket
            self.apigw_client.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps(doc, cls=DecimalEncoder)
            )
               
            print(f'Message has been updated')
            return True
        
        except self.apigw_client.exceptions.GoneException:
            print(f'Connection is no longer available')
            return False
        except Exception as e:
            print(f'Error sending message: {str(e)}')
            return False

    def mutate_workspace(self, changes, public_user=None, workspace_id=None):
        """
        Mutate workspace with changes.
        
        Args:
            changes (dict): The changes to apply
            public_user (str): The public user identifier
            workspace_id (str): The workspace ID
            
        Returns:
            bool: Success status
        """
        try:
        
            if not self.thread:
                return False
            
            if public_user:
                payload = {'context': {'public_user': public_user}}
            else:
                payload = {}

            # Sanitize changes early to prevent serialization errors in logging
            changes = self.sanitize(changes)
            first_key = next(iter(changes), None)
            print("MUTATE_WORKSPACE>>", first_key)
        
            # 1. Get the workspace in this thread
            print(f'Looking for workspaces @:{self.portfolio}:{self.org}:{self.entity_type}:{self.entity_id}:{self.thread} ')
            workspaces_list = self.CHC.list_workspaces(
                self.portfolio,
                self.org,
                self.entity_type,
                self.entity_id,
                self.thread
            ) 
            #print('WORKSPACES_LIST >>', workspaces_list) 
            
            if not workspaces_list['success']:
                return False
            
            if len(workspaces_list['items']) == 0:
                # Create a workspace as none exist
                response = self.CHC.create_workspace(
                    self.portfolio,
                    self.org,
                    self.entity_type,
                    self.entity_id,
                    self.thread, payload
                ) 
                if not response['success']:
                    return False
                # Regenerate workspaces_list
                workspaces_list = self.CHC.list_workspaces(
                    self.portfolio,
                    self.org,
                    self.entity_type,
                    self.entity_id,
                    self.thread
                ) 
                #print('UPDATED WORKSPACES_LIST >>>>', workspaces_list) 
                
            if not workspace_id:
                workspace = workspaces_list['items'][-1]
            else:
                for w in workspaces_list['items']:
                    if w['_id'] == workspace_id:
                        workspace = w
                        
            # CRITICAL: Sanitize workspace immediately after retrieval from database
            # This converts all Decimals before any merging or manipulation
            workspace = self.sanitize(workspace)
            #print('Selected workspace >>>>', workspace) 
            if 'state' not in workspace:
                workspace['state'] = {
                    "beliefs": {},
                    "desire": '',           
                    "intent": [],       
                    "history": [],          
                    "in_progress": None    
                }
                
            # 2. Store the output in the workspace
            for key, output in changes.items():
                if key == 'belief':
                    # output = {"date":"345"}
                    if isinstance(output, dict):
                        # Sanitize output before merging, then sanitize the merged result
                        sanitized_output = self.sanitize(output)
                        merged_beliefs = {**workspace['state']['beliefs'], **sanitized_output}
                        workspace['state']['beliefs'] = self.sanitize(merged_beliefs)
                        
                if key == 'desire':
                    if isinstance(output, str):
                        workspace['state']['desire'] = output
                        
                if key == 'intent':
                    if isinstance(output, dict):
                        # Sanitize nested intent data to ensure no Decimals slip through
                        workspace['state']['intent'] = self.sanitize(output) 
                        
                if key == 'belief_history':
                    if isinstance(output, dict):
                        # Now update the belief history
                        for k, v in output.items():
                            history_event = {
                                'type': 'belief',
                                'key': k,
                                'val': self.sanitize(v),
                                'time': datetime.now().isoformat()
                            }
                            workspace['state']['history'].append(history_event)
                                
                if key == 'cache':
                    print(f'Updating workspace cache: {output}')
                    if 'cache' not in workspace: 
                        workspace['cache'] = {}
                    if isinstance(output, dict):
                        for k, v in output.items():
                            # Sanitize nested values to ensure no Decimals slip through
                            workspace['cache'][k] = self.sanitize(v)
                
                if key == 'is_active':
                    if isinstance(output, bool):
                        workspace['data'] = output  # Output overrides existing data
                        
                if key == 'action':
                    if isinstance(output, str):
                        workspace['state']['action'] = output  # Output overrides existing data
                        
                if key == 'follow_up':
                    if isinstance(output, dict):
                        # Sanitize nested follow_up data to ensure no Decimals slip through
                        workspace['state']['follow_up'] = self.sanitize(output)
                        
                if key == 'slots':
                    if isinstance(output, dict):
                        # Sanitize nested slots data to ensure no Decimals slip through
                        workspace['state']['slots'] = self.sanitize(output)
                        
                if key == 'plan':
                    if isinstance(output, dict):
                        plan_id = output['id']
                        if 'plan' not in workspace:
                            workspace['plan'] = {}
                        # Sanitize nested plan data to ensure no Decimals slip through
                        workspace['plan'][plan_id] = self.sanitize(output)
                        
                if key == 'execution':
                    if isinstance(output, dict):
                        plan_id = output['plan_id']
                        if 'state_machine' not in workspace:
                            workspace['state_machine'] = {}
                        # Sanitize nested continuity data to ensure no Decimals slip through
                        workspace['state_machine'][plan_id] = self.sanitize(output)
                        print(f'Updating state machine:{workspace}')
                        
                if key == 'action_log':
                    if isinstance(output, dict):
                        '''
                        {
                            "plan_id":plan_id,
                            "plan_step":plan_step,
                            "tool":selected_tool,
                            "status":tool_step,
                            "details":"Calling tool"
                        }
                        '''
                        print(f'Storing action_log:{output}')
                        plan_id = output['plan_id']
                        plan_step = output['plan_step']
                        log = {"tool":output['tool'],"status":output['status'],"details":output["details"]}
                        if not 'action_log' in workspace['state_machine'][plan_id]['steps'][plan_step]:
                            workspace['state_machine'][plan_id]['steps'][plan_step]['action_log'] = []
                        
                        workspace['state_machine'][plan_id]['steps'][plan_step]['action_log'].append(log)
                        
                        
                            
             # 3. Update document in DB
       
            # Sanitize the entire workspace object to convert Decimals before updating
            sanitized_workspace = self.sanitize(workspace)
            print(f'WORSKPACE > Inserting updated workspace')
            self.update_workspace_document(
                sanitized_workspace,
                workspace['_id']
            )
            return True
        
        except Exception as e:
            print(f'Error updating workspace: {str(e)}')
            return False

    def llm(self, prompt):
        """
        Call the LLM with the given prompt.
        
        Args:
            prompt (dict): The prompt to send to the LLM
            
        Returns:
            The LLM response or False if error
        """
        
        
        try:
            # Create base parameters
            params = {
                'model': '',
                'messages': '',
                'temperature': 0.0
            }
        
            # Add optional parameters if they exist
            if 'model' in prompt:
                params['model'] = prompt['model']
            if 'messages' in prompt:
                params['messages'] = prompt['messages']
            if 'temperature' in prompt:
                params['temperature'] = prompt['temperature']
            if 'tools' in prompt:
                params['tools'] = prompt['tools']
            if 'tool_choice' in prompt:
                params['tool_choice'] = prompt['tool_choice']
            if 'response_format' in prompt:
                params['response_format'] = prompt['response_format']
                
            response = self.AI_1.chat.completions.create(**params) 
            
            return response.choices[0].message
 
        except Exception as e:
            print(f"Error running LLM call: {e}")
            return False
        
    
    def new_chat_thread_document(self,public_user=''):
        """
        Check if thread exists and if not create new one
        
        """
        action = 'new_chat_thread_document'
        print(f'Running: {action}')
        
        try:
        # List threads
            threads = self.CHC.list_threads(self.portfolio,self.org,self.entity_type,self.entity_id)
            print(f'List Threads: {threads}')
            
            if 'success' in threads:
                if len(threads['items']) < 1:
                    # No threads found
                    print('Creating new thread')
                    response_2 = self.CHC.create_thread(self.portfolio,self.org,self.entity_type, self.entity_id, public_user=public_user)
                    
                    if not response_2.get('success'):
                        print(f'Failed to create thread: {response_2}')
                        return {'success': False,'action': action,'input': message_payload,'output': response_2}
                
                    thread = response_2['document']
                    
                else:
                    thread = threads['items'][0]   
                return {
                    'success': True,'action': action,'output': thread
                }
                
            else: 
                return {
                    'success': False,'action': action,'output': thread
                }
                
                
        
        except Exception as e:
            
            print(f"Error getting/creating thread: {e}")
            return {'success': False,'action': action,'output': f"{e}"}
                 


    def new_chat_message_document(self, message, public_user=None):
        """
        Create a new chat message document.
        
        Args:
            message (str): The message content
            public_user (str): The public user identifier
            
        Returns:
            dict: Success status and response
        """
        action = 'new_chat_message_document'
        print(f'Running: {action}')  
        
        try:
        
            message_context = {}
            message_context['portfolio'] = self.portfolio
            message_context['org'] = self.org
            message_context['public_user'] = public_user
            message_context['entity_type'] = self.entity_type
            message_context['entity_id'] = self.entity_id
            message_context['thread'] = self.thread
            
            new_message = {"role": "user", "content": message}
            msg_wrap = {
                "_out": new_message,
                "_type": "text",
                "_id": str(uuid.uuid4())  # This is the Message ID
            }
            
            # Append new message to permanent storage
            message_object = {}
            message_object['context'] = message_context
            message_object['messages'] = [msg_wrap]
                    
            response = self.CHC.create_turn(
                self.portfolio,
                self.org,
                self.entity_type,
                self.entity_id,
                self.thread,
                message_object
            )
            
            '''
            response format
            
            {
                "success":BOOL, 
                "message": STRING, 
                "document": {
                    'author_id': STRING,
                    'time': STRING,
                    'is_active': BOOL,
                    'context': DICT,
                    'messages': STRING,
                    'index': STRING,
                    'entity_index': STRING,
                    '_id': STRING 
                },
                "status" : STRING
            }
            
            '''
            
            
            if 'document' in response and '_id' in response['document']:
                self.chat_id = response['document']['_id']
            
            print(f'Response: {response}')
        
            if 'success' not in response:
                return {'success': False, 'action': action, 'input': message, 'output': response}
            
            return {'success': True, 'action': action, 'input': message, 'output': response['document']}
        
        
        except Exception as e:
            
            print(f"Error getting/creating turn: {e}")
            return {'success': False,'action': action,'input': message_payload,'output': f"{e}"}
          
        
        

    def get_active_workspace(self, workspace_id=None):
        """
        Get the active workspace.
        
        Args:
            workspace_id (str): The workspace ID to get
            
        Returns:
            dict: The workspace or False if not found
        """
        workspaces_list = self.CHC.list_workspaces(
            self.portfolio,
            self.org,
            self.entity_type,
            self.entity_id,
            self.thread
        ) 
        
        if not workspaces_list['success']:
            return False
        
        if len(workspaces_list['items']) == 0:
            return False
        
        if not workspace_id:
            workspace = workspaces_list['items'][-1]
        else:
            for w in workspaces_list['items']:
                if w['_id'] == workspace_id:
                    workspace = w
                    break
            else:
                return False
        
        # Sanitize workspace before returning to ensure no Decimals are present
        return self.sanitize(workspace)

    def sanitize(self, obj):
        """
        Sanitize an object for JSON serialization.
        
        Args:
            obj: The object to sanitize
            
        Returns:
            The sanitized object
        """
        if isinstance(obj, list):
            return [self.sanitize(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self.sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            # Convert Decimal to int if it's a whole number, otherwise float
            return int(obj) if obj % 1 == 0 else float(obj)
        elif isinstance(obj, float):
            # Convert float to string
            return str(obj)
        elif isinstance(obj, int):
            # Keep integers as is
            return obj
        else:
            return obj

    def prune_history(self, history):
        """
        Prunes the history list to keep only the most recent value for each key while maintaining chronological order.
        Objects at the bottom of the list are newer.
        
        Args:
            history (list): List of belief objects with key, val, time, and type fields
            
        Returns:
            list: Pruned history list with only the most recent value for each key
        """
        # Create a dictionary to track the most recent value for each key
        latest_values = {}
        
        # First pass: identify the most recent value for each key
        for item in history:
            key = item['key']
            latest_values[key] = item
        
        # Second pass: create new list maintaining original order but only including latest values
        pruned_history = []
        seen_keys = set()
        
        # Iterate through history in reverse to maintain chronological order
        for item in reversed(history):
            key = item['key']
            if key not in seen_keys:
                pruned_history.append(item)
                seen_keys.add(key)
        
        # Reverse back to maintain original order (newest at bottom)
        return list(reversed(pruned_history))

    def string_from_object(self, object: dict) -> str:
        """
        Converts a dictionary into a formatted string.
        
        Args:
            object (dict): Dictionary containing key-value pairs
            
        Returns:
            str: Formatted string with key-value pairs separated by commas
            
        Example:
            Input: {"origin": "NYC", "destination": "SF", "departure_date": "2025-06-20", "guest_count": 4}
            Output: "origin = NYC, destination = SF, departure_date = 2025-06-20, guest_count = 4"
        """
        if not object:
            return ""
            
        formatted_pairs = []
        for key, value in object.items():
            # Convert value to string and handle different types appropriately
            if isinstance(value, (int, float)):
                formatted_value = str(value)
            elif isinstance(value, str):
                formatted_value = value
            else:
                formatted_value = str(value)
                
            formatted_pairs.append(f"{key} = {formatted_value}")
            
        return ", ".join(formatted_pairs)

    def format_object_to_slash_string(self, obj: dict) -> str:
        """
        Converts an object into a string with values separated by slashes.
        If a value is not a string, it will be replaced with an empty space.
        Keys are sorted alphabetically to ensure consistent output regardless of input order.
        
        Args:
            obj (dict): Dictionary containing key-value pairs
            
        Returns:
            str: Formatted string with values separated by slashes
            
        Example:
            Input: {"people": "4", "time": "16:00", "date": "2025-06-04"}
            Output: "2025-06-04/4/16:00"
        """
        if not obj:
            return ""
            
        values = []
        # Sort keys alphabetically
        for key in sorted(obj.keys()):
            value = obj[key]
            if isinstance(value, str):
                values.append(value)
            else:
                values.append("")
                
        return "/".join(values)

    def clean_json_response(self, response):
        """
        Cleans and validates a JSON response string from LLM.
        
        Args:
            response (str): The raw JSON response string from LLM
            
        Returns:
            dict: The parsed JSON object if successful
            None: If parsing fails
            
        Raises:
            json.JSONDecodeError: If the response cannot be parsed as JSON
        """
        try:
            # Clean the response by ensuring property names are properly quoted
            cleaned_response = response
            # Remove any comments (both single-line and multi-line)
            cleaned_response = re.sub(r'//.*?$', '', cleaned_response, flags=re.MULTILINE)  # Remove single-line comments
            cleaned_response = re.sub(r'/\*.*?\*/', '', cleaned_response, flags=re.DOTALL)  # Remove multi-line comments
            
            # First try to parse as is
            try:
                return json.loads(cleaned_response)
            except json.JSONDecodeError:
                pass
                
            # If that fails, try to fix common issues
            # Handle unquoted property names at the start of the object
            cleaned_response = re.sub(r'^\s*{\s*(\w+)(\s*:)', r'{"\1"\2', cleaned_response)
            
            # Handle unquoted property names after commas
            cleaned_response = re.sub(r',\s*(\w+)(\s*:)', r',"\1"\2', cleaned_response)
            
            # Handle unquoted property names after newlines
            cleaned_response = re.sub(r'\n\s*(\w+)(\s*:)', r'\n"\1"\2', cleaned_response)
            
            # Replace single quotes with double quotes for property names
            cleaned_response = re.sub(r'([{,]\s*)\'(\w+)\'(\s*:)', r'\1"\2"\3', cleaned_response)
            
            # Replace single quotes with double quotes for string values
            # This regex looks for : 'value' pattern and replaces it with : "value"
            cleaned_response = re.sub(r':\s*\'([^\']*)\'', r': "\1"', cleaned_response)
            
            # Remove spaces between colons and boolean values
            cleaned_response = re.sub(r':\s+(true|false|True|False)', r':\1', cleaned_response)
            
            # Remove trailing commas in objects and arrays
            # This regex will match a comma followed by whitespace and then a closing brace or bracket
            cleaned_response = re.sub(r',(\s*[}\]])', r'\1', cleaned_response)
            
            # Remove any timestamps in square brackets
            cleaned_response = re.sub(r'\[\d+\]\s*', '', cleaned_response)
            
            # Try to parse the cleaned response
            try:
                return json.loads(cleaned_response)
            except json.JSONDecodeError as e:
                print(f"First attempt failed. Error: {e}")
                
                # If first attempt fails, try to fix the raw field specifically
                # Find the raw field and ensure it's properly formatted
                raw_match = re.search(r'"raw":\s*({[^}]+})', cleaned_response)
                if raw_match:
                    raw_content = raw_match.group(1)
                    # Convert single quotes to double quotes in the raw content
                    raw_content = raw_content.replace("'", '"')
                    # Replace the raw field with the cleaned version
                    cleaned_response = cleaned_response[:raw_match.start(1)] + raw_content + cleaned_response[raw_match.end(1):]
                
                return json.loads(cleaned_response)
        
        except json.JSONDecodeError as e:
            print(f"Error parsing cleaned JSON response: {e}")
            raise

    def _convert_to_dict(self, obj):
        """
        Recursively converts an OpenAI response object to a dictionary.
        
        Args:
            obj: The object to convert (can be ChatCompletionMessage, ChatCompletionMessageToolCall, etc.)
            
        Returns:
            dict: The converted dictionary
        """
        if hasattr(obj, '__dict__'):
            return {key: self._convert_to_dict(value) for key, value in obj.__dict__.items()}
        elif isinstance(obj, list):
            return [self._convert_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._convert_to_dict(value) for key, value in obj.items()}
        else:
            return obj

    def remove_outer_escape(self, double_escaped_string):
        """
        Removes the outer escape from a double-escaped JSON string.
        
        Args:
            double_escaped_string (str): A string that has been escaped twice
            
        Returns:
            str: The cleaned JSON string, or None if parsing fails
            
        Example:
            Input: '"{\\"raw_document\\":\\"Wood Property\\\\n1/25 Wellington Street...\\"}"'
            Output: '{"raw_document": "Wood Property\n1/25 Wellington Street..."}'
        """
        try:
            # First, parse the outer JSON string to get the inner escaped string
            outer_parsed = json.loads(double_escaped_string)
            
            # Then parse the inner string to get the actual JSON object
            if isinstance(outer_parsed, str):
                inner_parsed = json.loads(outer_parsed)
                # Return the cleaned JSON as a string
                return json.dumps(inner_parsed)
            else:
                # If it's already a dict, convert back to string
                return json.dumps(outer_parsed)
                
        except json.JSONDecodeError as e:
            print(f"Error parsing double-escaped JSON: {e}")
            return None

    def validate_interpret_openai_llm_response(self, raw_response: dict) -> dict:
        """
        Validates that the LLM response follows the expected format.
        
        Args:
            raw_response (dict): The raw response from the LLM
            
        Returns:
            dict: Validation result with success status and output
        """
        action = 'validate_interpret_openai_llm_response'
        # Convert OpenAI response object to dictionary if needed
        response = self._convert_to_dict(raw_response)     
        
        # Check if response has required 'role' field
        if 'role' not in response:
            return {"success": False, "action": action, "input": response, "output": "Response missing required 'role' field"}
            
        # Check if role is 'assistant'
        if response['role'] != 'assistant':
            return {"success": False, "action": action, "input": response, "output": "Response role must be 'assistant'"}
            
        # Check if response has either 'content' or 'tool_calls'
        has_content = 'content' in response and response['content'] is not None
        has_tool_calls = 'tool_calls' in response and response['tool_calls'] is not None
        
        if not (has_content or has_tool_calls):
            return {"success": False, "action": action, "input": response, "output": "Response must have either non-null 'content' or non-null 'tool_calls'"}
            
        if has_content and has_tool_calls:
            # If this happens, remove content so the message is still compliant
            response['content'] = ''
            
        # If it's a tool call, validate the tool_calls structure
        if has_tool_calls:
            if not isinstance(response['tool_calls'], list):
                return {"success": False, "action": action, "input": response, "output": "'tool_calls' must be a list"}
                
            for tool_call in response['tool_calls']:
                if not isinstance(tool_call, dict):
                    return {"success": False, "action": action, "input": response, "output": "Each tool call must be a dictionary"}
                    
                required_fields = ['id', 'type', 'function']
                for field in required_fields:
                    if field not in tool_call or tool_call[field] is None:
                        return {"success": False, "action": action, "input": response, "output": f"Tool call missing required field '{field}' or field is null"}
                        
                if tool_call['type'] != 'function':
                    return {"success": False, "action": action, "input": response, "output": "Tool call type must be 'function'"}
                    
                if not isinstance(tool_call['function'], dict):
                    return {"success": False, "action": action, "input": response, "output": "Tool call 'function' must be a dictionary"}
                    
                function_required_fields = ['name', 'arguments']
                for field in function_required_fields:
                    if field not in tool_call['function'] or tool_call['function'][field] is None:
                        return {"success": False, "action": action, "input": response, "output": f"Tool call function missing required field '{field}' or field is null"}
                        
                # Validate that arguments is a valid JSON string
                try:
                    if isinstance(tool_call['function']['arguments'], str):
                        json.loads(tool_call['function']['arguments'])
                except json.JSONDecodeError:
                    # Try to fix double-escaped JSON
                    escape_result = self.remove_outer_escape(tool_call['function']['arguments'])
                    if escape_result:
                        # Validate the cleaned result
                        try:
                            json.loads(escape_result)
                            tool_call['function']['arguments'] = escape_result
                        except json.JSONDecodeError:
                            return {"success": False, "action": action, "input": response, "output": "Tool call arguments must be a valid JSON string after cleaning"}
                    else:
                        return {"success": False, "action": action, "input": response, "output": "Tool call arguments must be a valid JSON string"}
                    
        # If it's a content response, validate content is a string
        if has_content and not isinstance(response['content'], str):
            return {"success": False, "action": action, "input": response, "output": "Content must be a string"}
            
        return {"success": True, "action": action, "output": response}

    def clear_tool_message_content(self, message_list, recent_tool_messages=1):
        """
        Clear content from all tool messages except the last x ones.
        This prevents overwhelming the LLM with tool output history.
        
        Args:
            message_list: List of messages to process
            recent_tool_messages: Number of recent tool messages to keep with content (default: 1)
            
        Returns:
            list: The processed message list
        """
        print(f'Raw message_list: {message_list}')
        
        # Find the indices of the last x tool messages
        tool_indices = []
        for i in range(len(message_list) - 1, -1, -1):
            if message_list[i].get('role') == 'tool':
                tool_indices.append(i)
                if len(tool_indices) >= recent_tool_messages:
                    break
        
        # Clear content from all tool messages except the last x ones
        for i, message in enumerate(message_list):
            if message.get('role') == 'tool' and i not in tool_indices:
                print(f'Found a tool message: {message}')
                # Actually clear the content (set to empty string)
                message['content'] = ""
            else:
                # Convert complex content to string format for OpenAI API
                if isinstance(message.get('content'), list):
                    # If content is an array, sanitize and convert it to a JSON string
                    sanitized_content = self.sanitize(message['content'])
                    message['content'] = json.dumps(sanitized_content)
                elif isinstance(message.get('content'), dict):
                    # If content is an object, sanitize and convert it to a JSON string
                    sanitized_content = self.sanitize(message['content'])
                    message['content'] = json.dumps(sanitized_content)
                else:
                    # If content is already a string or other type, sanitize and convert to string
                    sanitized_content = self.sanitize(message.get('content', ''))
                    message['content'] = str(sanitized_content)
                
        print(f'Cleared tool message content: {message_list}')
        
        return message_list