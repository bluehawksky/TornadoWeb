# -*- coding: utf-8 -*-

import config

from tornado.gen import coroutine, Return

from util.util import Utils
from util.struct import Ignore
from util.cache import MCache
from util.database import MySQLPool


class BaseModel(Utils):
    
    @staticmethod
    def Return(value=None):
        
        raise Return(value)
    
    @staticmethod
    def Break():
        
        raise Ignore()
    
    def __init__(self):
        
        # 数据缓存
        self._mc = MCache()
        
        # 数据连接池
        self._dbm = MySQLPool().master()
        self._dbs = MySQLPool().slave()
    
    def __del__(self):
        
        # 数据缓存
        self._mc = None
        
        # 数据连接池
        self._dbm = None
        self._dbs = None
    
    def cache_key(self, *keys):
        
        return self._mc.key(*keys)
    
    @coroutine
    def get_cache(self, key):
        
        result = yield self._mc.get(key)
        
        self.Return(result)
    
    @coroutine
    def set_cache(self, key, val, time=0):
        
        if(time == 0):
            time = config.Static.RedisExpires
        
        result = yield self._mc.set(key, val, time)
        
        self.Return(result)
    
    @coroutine
    def del_cache(self, pattern=r'*'):
        
        keys = yield self._mc.keys(pattern)
        
        if(keys):
            
            result = yield self._mc.delete(*keys)
            
            self.Return(result)
        
        else:
            
            self.Return(True)

