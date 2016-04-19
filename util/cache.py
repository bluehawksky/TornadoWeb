# -*- coding: utf-8 -*-

import time, hashlib, pickle, binascii, zlib, config

from tornado.gen import coroutine, sleep, Return, Task
from tornado_redis import Client, ConnectionPool
from tornado.escape import utf8

from .decorator import singleton, catch_error


@singleton
class MCache():
    
    def __init__(self):
        
        with catch_error():
            
            self.__conn_pool = ConnectionPool(host=config.Static.RedisHost[0], port=config.Static.RedisHost[1], max_connections=config.Static.RedisMaxConn)
        
    def _get_client(self):
        
        return Client(connection_pool=self.__conn_pool)
        
    def _to_store_value(self, val):
        
        if(val is None):
            return None
        
        stream = pickle.dumps(val)
        
        stream = zlib.compress(stream)
        
        result = binascii.b2a_base64(stream).decode(r'ascii')
        
        return result
        
    def _from_store_value(self, val):
        
        if(val is None):
            return None
        
        stream = binascii.a2b_base64(val.encode(r'ascii'))
        
        stream = zlib.decompress(stream)
        
        result = pickle.loads(stream)
        
        return result
    
    def key(self, key, *args):
        
        if(not args):
            return r'_'.join([config.Static.Secret, str(key)])
        
        keys = r'_'.join(str(arg) for arg in args)
        
        if(len(keys) > 32):
            keys = hashlib.md5(utf8(keys)).hexdigest()
        
        return r'_'.join([config.Static.Secret, str(key), keys])
    
    @coroutine
    def touch(self, key, expire=0):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.expire, key, expire)
            
            raise Return(result)
    
    @coroutine
    def has(self, key):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.exists, key)
            
            raise Return(result)
    
    @coroutine
    def get(self, key):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.get, key)
            
            result = self._from_store_value(result)
            
            raise Return(result)
    
    @coroutine
    def set(self, key, val, expire=0):
        
        with catch_error():
            
            client = self._get_client()
            
            val = self._to_store_value(val)
            
            result = yield Task(client.set, key, val, expire)
            
            raise Return(result)
    
    @coroutine
    def setnx(self, key, val, expire=0):
        
        with catch_error():
            
            client = self._get_client()
            
            val = self._to_store_value(val)
            
            result = yield Task(client.set, key, val, expire, only_if_not_exists=True)
            
            raise Return(result)
    
    @coroutine
    def setxx(self, key, val, expire=0):
        
        with catch_error():
            
            client = self._get_client()
            
            val = self._to_store_value(val)
            
            result = yield Task(client.set, key, val, expire, only_if_exists=True)
            
            raise Return(result)
    
    @coroutine
    def incr(self, key, amount=1):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.incrby, key, amount)
            
            raise Return(result)
    
    @coroutine
    def decr(self, key, amount=1):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.decrby, key, amount)
            
            raise Return(result)
    
    @coroutine
    def delete(self, *keys):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.delete, *keys)
            
            raise Return(result)
    
    @coroutine
    def keys(self, pattern=r'*'):
        
        with catch_error():
            
            client = self._get_client()
            
            result = yield Task(client.keys, pattern)
            
            raise Return(result)


class MLock():
    
    def __init__(self, *args):
        
        self._cache = MCache()
        
        self._lock_tag = self._cache.key(r'threading_lock', args)
        
        self._valid = False
    
    @coroutine
    def acquire(self, expire=60, duration=0.01):
        
        now_time = time.time()
        
        self._valid = yield self._cache.setnx(self._lock_tag, now_time, expire)
        
        while(not self._valid):
            
            sleep(duration)
            
            self._valid = yield self._cache.setnx(self._lock_tag, now_time, expire)
    
    @coroutine
    def release(self):
        
        if(self._valid):
        
            self._valid = False
            
            if(self._cache):
                yield self._cache.delete(self._lock_tag)
            
        self._cache = None
        
        self._lock_tag = None
    
    def __enter__(self):
        
        return self
    
    def __exit__(self, *args):
        
        self.release()
    
    def __del__(self):
        
        self.release()

