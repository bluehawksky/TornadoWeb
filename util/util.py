# -*- coding: utf-8 -*-

import re, uuid, time, hashlib, base64, string, binascii, socket
import jwt

from random import randint

from datetime import datetime

from tornado.log import app_log
from tornado.gen import sleep, Return
from tornado.escape import utf8, to_basestring, json_decode, json_encode

from .struct import Ignore
from .cache import MLock
from .database import safestr


class Utils():
    
    randint = staticmethod(randint)
    
    safestr = staticmethod(safestr)
    
    sleep = staticmethod(sleep)
    
    debug = staticmethod(app_log.debug)
    error = staticmethod(app_log.error)
    
    json_encode = staticmethod(json_encode)
    json_decode = staticmethod(json_decode)
    
    @staticmethod
    def Return(value=None):
        
        raise Return(value)
    
    @staticmethod
    def Break():
        
        raise Ignore()
    
    @staticmethod
    def allocate_lock(*args):
        
        return MLock(*args)
    
    @staticmethod
    def today():
        
        return datetime.today()
    
    @staticmethod
    def timestamp():
        
        return int(time.time())
    
    @staticmethod
    def split_int(val, sep=r',', maxsplit=-1):
        
        return [int(temp) for temp in val.split(sep, maxsplit) if temp.strip().isdigit()]
    
    @staticmethod
    def sub_str(value, length, suffix=r'...'):
        
        if(len(value) > length):
            return r'{0:s}{1:s}'.format(value[0:length], suffix)
        else:
            return value
    
    @staticmethod
    def re_match(pattern, value):
        
        result = re.match(pattern, value)
        
        return True if result else False
    
    @staticmethod
    def ip2int(val):
        
        try:
            return int(binascii.hexlify(socket.inet_aton(val)), 16)
        except socket.error:
            return int(binascii.hexlify(socket.inet_pton(socket.AF_INET6, val)), 16)
    
    @staticmethod
    def int2ip(val):
        
        try:
            return socket.inet_ntoa(binascii.unhexlify(r'%08x' % val))
        except socket.error:
            return socket.inet_ntop(socket.AF_INET6, binascii.unhexlify(r'%032x' % val))
    
    @staticmethod
    def radix36(val, align=0):
        
        base = string.digits + string.ascii_uppercase
        
        num = int(val)
        result = ''
        
        while(num > 0):
            num,rem = divmod(num, 36)
            result = r'{0}{1}'.format(base[rem], result)
        
        return r'{0:0>{1:d}s}'.format(result, align)
    
    @staticmethod
    def radix62(val, align=0):
        
        base = string.digits + string.ascii_letters
        
        num = int(val)
        result = ''
        
        while(num > 0):
            num,rem = divmod(num, 62)
            result = r'{0}{1}'.format(base[rem], result)
        
        return r'{0:0>{1:d}s}'.format(result, align)
    
    @staticmethod
    def b64_encode(val):
        
        val = utf8(val)
        
        result = base64.b64encode(val)
        
        return to_basestring(result)
    
    @staticmethod
    def b64_decode(val):
        
        val = utf8(val)
        
        result = base64.b64decode(val)
        
        return to_basestring(result)
    
    @staticmethod
    def jwt_encode(val, key):
        
        result = jwt.encode(val, key)
        
        return to_basestring(result)
    
    @staticmethod
    def jwt_decode(val, key):
        
        val = utf8(val)
        
        return jwt.decode(val, key)
    
    @staticmethod
    def uuid1(node=None, clock_seq=None):
        
        return uuid.uuid1(node, clock_seq).hex
    
    @staticmethod
    def md5(val):
        
        val = utf8(val)
        
        return hashlib.md5(val).hexdigest()
    
    @staticmethod
    def sha1(val):
        
        val = utf8(val)
        
        return hashlib.sha1(val).hexdigest()
    
    @staticmethod
    def sha256(val):
        
        val = utf8(val)
        
        return hashlib.sha256(val).hexdigest()
    
    @staticmethod
    def sha512(val):
        
        val = utf8(val)
        
        return hashlib.sha512(val).hexdigest()

