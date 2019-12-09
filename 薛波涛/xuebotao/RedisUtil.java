package com.xuebotao.util;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

/**
 * 连接redis
 */
public class RedisUtil   {
    private static JedisPool pool;

    /**
     * 获得jedis实例
     * @return
     */
    public static Jedis getJedisPool(){
        if(pool==null) { //外层的if是为了提高效率 节省每个线程来了进去判断等待
            synchronized (RedisUtil.class) {
                if (pool == null) { //为了保证JedisPool的实例的唯一性
                    JedisPoolConfig poolconfig = new JedisPoolConfig();
                    poolconfig.setMaxIdle(30);
                    poolconfig.setMaxTotal(100);
                    poolconfig.setTestOnBorrow(true);
                    pool = new JedisPool(poolconfig,"xue01",6379);
                }
            }
        }
        return  pool.getResource();
    }
    /**
     * 将当前的jedis实例归还到jedis池中存储起来
     * @param jedis
     */
    public static  void releaseReource(Jedis jedis){
        if(jedis!=null){
            pool.returnResourceObject(jedis);
        }
    }

}
