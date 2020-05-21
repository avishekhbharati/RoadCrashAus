import hashlib
import binascii
import os
import json
from collections.abc import Mapping, Iterable
from decimal import Decimal

class PasswordHelper:
    def hash_password(self, password):
        """Hash a password for storing."""
        salt = hashlib.sha256(os.urandom(60)).hexdigest().encode('ascii')
        pwdhash = hashlib.pbkdf2_hmac('sha512', password.encode('utf-8'), 
                                    salt, 100000)
        pwdhash = binascii.hexlify(pwdhash)
        return (salt + pwdhash).decode('ascii')
    

    def verify_password(self, stored_password, provided_password):
        """Verify a stored password against one provided by user"""
        salt = stored_password[:64]
        stored_password = stored_password[64:]
        pwdhash = hashlib.pbkdf2_hmac('sha512', 
                                    provided_password.encode('utf-8'), 
                                    salt.encode('ascii'), 
                                    100000)
        pwdhash = binascii.hexlify(pwdhash).decode('ascii')
        return pwdhash == stored_password



class Response_Message:
    success = False
    message = ""
    items = []

    def __init__(self, success, message = None, objects = None):
        self.success = success
        self.message = message
        self.items = objects


class DecimalEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, Mapping):
            return '{' + ', '.join(f'{self.encode(k)}: {self.encode(v)}' for (k, v) in obj.items()) + '}'
        if isinstance(obj, Iterable) and (not isinstance(obj, str)):
            return '[' + ', '.join(map(self.encode, obj)) + ']'
        if isinstance(obj, Decimal):
            return f'{obj.normalize():f}'  # using normalize() gets rid of trailing 0s, using ':f' prevents scientific notation
        return super().encode(obj)
    