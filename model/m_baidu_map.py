# -*- coding: utf-8 -*-

import config

from tornado.gen import coroutine

from util.decorator import singleton, catch_error

from ._base import BaseModel


@singleton
class BaiduMapClient(BaseModel):
    
    def __init__(self):
        
        self._api_url = r'http://api.map.baidu.com'
        self._api_key = config.Static.BaiduMapAK
    
    @coroutine
    def __query(self, url, **params):
        
        params[r'ak'] = self._api_key
        params[r'output'] = r'json'
        
        result = None
        
        with catch_error():
            
            response = yield self.fetch_url(r''.join([self._api_url, url]), params)
            
            if(response):
                result = self.json_decode(response)
            
        self.Return(result)
    
    @coroutine
    def geocode(self, location):
        """
        地址解析
        """
        
        aip_url = r'/geocoder/v2/'
        
        params = {}
        
        style = type(location)
        
        if(style is str):
            params[r'address'] = location
        elif(style in (list,tuple,)):
            params[r'location'] = r','.join(str(val) for val in location)
        
        response = yield self.__query(aip_url, **params)
        
        self.Return(response)
    
    @coroutine
    def ip(self, location):
        """
        IP定位
        """
        
        aip_url = r'/location/ip'
        
        params = {r'ip':location, r'coor':r'bd09ll'}
        
        response = yield self.__query(aip_url, **params)
        
        self.Return(response)

