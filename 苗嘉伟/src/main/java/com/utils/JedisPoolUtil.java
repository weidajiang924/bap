package com.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUtil {
    private static JedisPool pool;

    /**
     * 获得Jedis的实例
     *
     * @return
     */
    public static Jedis getJedisFromPool() {
        if (pool == null) {//外层的if是为了提高效率
            synchronized (JedisPoolUtil.class) {
                if (pool == null) { //该if是为了保证JedisPool实例的唯一性（单例）
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxIdle(DmpRedisUtil.getIntValueByKey(CommonData.JEDIS_MAX_IDLE));
                    config.setMaxTotal(DmpRedisUtil.getIntValueByKey(CommonData.JEDIS_MAX_TOTAL));
                    config.setTestOnBorrow(DmpRedisUtil.getBooleanValueByKey(CommonData.JEDIS_ON_BORROW));
                    pool = new JedisPool(config, DmpRedisUtil.getValueByKey(CommonData.JEDIS_HOST),
                            DmpRedisUtil.getIntValueByKey(CommonData.JEDIS_PORT));
                }
            }
        }

        return pool.getResource();
    }


    /**
     * 将当前的Jedis实例归还到Jedis池中存储起来
     *
     * @param jedis
     */
    public static void releaseReource(Jedis jedis) {
        if (jedis != null) {
            pool.returnResource(jedis);
        }
    }

}
