package util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object jedisConnectionPool {
  val config = new JedisPoolConfig
  config.setMaxIdle(30)
  config.setMaxTotal(10)

  val pool = new JedisPool(config,"hadoop05",6379,10000)

  def getConnect()={
    pool.getResource
  }

}
