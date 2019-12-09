package com.utils;

import redis.clients.jedis.Jedis;

import java.util.LinkedHashMap;
import java.util.Map;

public class GeoRedisForTrading {
    public void addRecord(String GeoStr, String TradingArea){
        Jedis jedis = JedisPoolUtil.getJedisFromPool();
        Map<String, String> map = new LinkedHashMap<String, String>();
        map.put(GeoStr,TradingArea);
        jedis.hmset("GeoHashTrading", map);
        JedisPoolUtil.releaseReource(jedis);
    }
}
