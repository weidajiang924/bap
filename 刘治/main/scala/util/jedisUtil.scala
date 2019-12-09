package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class jedisUtil

object jedisUtil {
   var pool: JedisPool = null

  def getJedisFromPool: Jedis = {
    if (pool == null) {
        val config = new JedisPoolConfig
        config.setMaxIdle(10)
        config.setMaxTotal(100)
        config.setTestOnBorrow(true)
        pool = new JedisPool(config, "MyDis", 6379)
    }
    pool.getResource
  }

//  def releaseReource(jedis: Jedis): Unit = {
//    if (jedis != null) pool.r
//  }
}
