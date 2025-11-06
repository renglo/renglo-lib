from datetime import datetime

class GupshupIntegration:
    """
    Class to handle Gupshup webhook integration and message processing.
    """
    
    def __init__(self, chc, agc, current_app):
        """
        Initialize the GupshupIntegration class.
        
        Args:
            chc: Chat handler client instance
            agc: AI gateway client instance  
            current_app: Flask current_app instance
        """
        
        

        self.CHC = chc
        self.AGC = agc
        self.current_app = current_app
    
    def extract_gupshup_payload(self, payload):
        """
        Extract required data from Gupshup webhook payload.
        
        Args:
            payload (dict): The raw Gupshup webhook payload
            
        Returns:
            dict: Extracted data with keys: message, timestamp, sender_name, sender_id, app_id
            or tuple: (False, error_message) if extraction fails
        """
        try:
            # Validate top-level structure
            if 'entry' not in payload or not payload['entry']:
                return False, "Missing 'entry' field in payload"
            
            if 'gs_app_id' not in payload:
                return False, "Missing 'gs_app_id' field in payload"
            
            # Get the first entry
            entry = payload['entry'][0]
            if 'changes' not in entry or not entry['changes']:
                return False, "Missing 'changes' field in entry"
            
            # Get the first change
            change = entry['changes'][0]
            if 'value' not in change:
                return False, "Missing 'value' field in change"
            
            value = change['value']
            
            # Extract contacts and messages
            if 'contacts' not in value or not value['contacts']:
                return False, "Missing 'contacts' field in value"
            
            if 'messages' not in value or not value['messages']:
                return False, "Missing 'messages' field in value"
            
            contact = value['contacts'][0]
            message = value['messages'][0]
            
            # Extract required fields
            try:
                # 1. Message
                message_text = message['text']['body']
                
                # 2. Timestamp
                timestamp = message['timestamp']
                
                # 3. Sender Name
                sender_name = contact['profile']['name']
                
                # 4. Sender ID
                sender_id = contact['wa_id']
                
                # 5. App ID
                app_id = payload['gs_app_id']
                
            except KeyError as e:
                return False, f"Missing required field: {e}"
            
            # Return extracted data
            extracted_data = {
                'message': message_text,
                'timestamp': timestamp,
                'sender_name': sender_name,
                'sender_id': sender_id,
                'app_id': app_id
            }
            
            return True, extracted_data
            
        except Exception as e:
            return False, f"Error extracting data: {str(e)}"

    def process_gupshup_message(self, portfolio, tool_id, payload):
        """
        Process a Gupshup message.

        """
        
        try:
        
            result = []
            
            valid, data = self.extract_gupshup_payload(payload)
            if not valid:
                self.current_app.logger.error(f'Invalid gupshup payload')
                return result
            
            msg_content = data['message']
            msg_timestamp = data['timestamp']
            msg_sender = data['sender_id'].strip()  # Remove whitespace and newlines

            
            entity_type = 'portfolio-tool-public'
            entity_id = f'{portfolio}-{tool_id}-{msg_sender}'
            org = ''
            # List threads will return success=true even if the list is empty (no threads)
            threads = self.CHC.list_threads(portfolio,org,entity_type, entity_id) 

            print(f'List Threads:{threads}')
            
            initialize_thread = False
            if 'success' in threads: 
                if len(threads['items'])<1:
                    # No threads found
                    # ACTION: Set flag to initialize a thread
                    print(f'Creating thread because: No threads have been found (List was empty)')
                    initialize_thread = True
                    
                else:       
                    # At least one thread exists. Pick the last thread
                    last_thread = threads['items'][0]
                    # For the thread to be valid. It needs to belong to the message sender number and be from today. (CONDITION DEPRECATED)
                    #if last_thread['author_id'] == msg_sender:
                    if datetime.fromtimestamp(float(last_thread['time'])).strftime('%Y-%m-%d') == datetime.fromtimestamp(float(msg_timestamp)).strftime('%Y-%m-%d'):
                        # This user has sent another message today already
                        # Complete the input object and send message to triage . END
                        # ACTION: Capture the message_thread and forward the message to the triage
                        print(f'Writing on existing thread:{last_thread}')
                        
                        input = {
                            'action':'gupshup_message',
                            'portfolio':portfolio,
                            'public_user': msg_sender,
                            'entity_type':entity_type,
                            'entity_id':entity_id,
                            'thread':last_thread['_id'],
                            'data': msg_content
                        }
                        
                        # config_location = <org_id> | '_all'
                        # get_location = [<org_id_1>,<org_id_2>,<org_id_3>]
                        # post_location = 
                            
                        response_1 = self.AGC.triage(input,core_name='portfolio_public')
                        result.append(response_1)
                        return result

                    else:
                        # This user has sent messages before but not today
                        # ACTION: Set flag to initialize a thread
                        print(f'Creating thread because:Last thread is not from today')
                        initialize_thread = True
                            
                    
            else:
                # This user has not sent a message before
                # ACTION: Set flag to initialize a thread
                print(f'Creating thread because: no threads found')
                initialize_thread = True
            
            
                        
            if initialize_thread:
                print(f'Calling : create_thread ')
                org = ''
                response_2 = self.CHC.create_thread(portfolio,org,entity_type,entity_id,public_user = msg_sender)
                result.append(response_2)
                if not response_2['success']:
                    self.current_app.logger.error(f'Failed to create thread: {response_2}')
                    return result
                    
                new_thread_id = response_2['document']['_id'] 
                
                # This object emulates the object received via WebSocket
                input = {
                    'action':'gupshup_message', # We don't need this since this is not a websocket
                    'portfolio':portfolio,
                    'public_user': msg_sender, # The web socket version doesn't have this attribute
                    'entity_type':entity_type,
                    'entity_id':entity_id,
                    'thread':new_thread_id,
                    'data': msg_content
                }
                    
                response_3 = self.AGC.triage(input,core_name='portfolio_public')
                result.append(response_3)
                
            return result
                
        except Exception as e:
            self.current_app.logger.error(f"Error processing gupshup message: {e}")
            return result

