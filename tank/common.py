
import jwt
import re
import hashlib

def decode_jwt(token):
    # Decode the JWT to get the user information
    decoded = jwt.decode(token, options={"verify_signature": False})
    return decoded



def get_username_from_email(email):
    # Extract the part before the @
    username = email.split('@')[0]
    # Remove any non-alphanumeric characters
    cleaned_username = re.sub(r'[^a-zA-Z0-9]', '', username)
    return cleaned_username




def create_md5_hash(input_string, num_digits):
    # Create an MD5 hash object
    md5_hash = hashlib.md5()
    # Update the hash object with the input string encoded as bytes
    md5_hash.update(input_string.encode('utf-8'))
    # Get the full hexadecimal MD5 hash
    full_hash = md5_hash.hexdigest()
    # Return the first N digits of the hash
    return full_hash[:num_digits]