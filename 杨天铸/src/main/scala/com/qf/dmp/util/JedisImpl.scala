package com.qf.dmp.util

import java.util.Properties

import com.qf.dmp.contant.Contant
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisImpl {

  var pool:JedisPool = null
  private val properties = new Properties()

  properties.load(this.getClass.getClassLoader.getResourceAsStream(Contant.PROPERTIES_PATH))

  def getJedisPool(): Jedis ={
        if(pool == null){
          val config = new JedisPoolConfig()
          config.setMaxIdle(properties.getProperty(Contant.JEDIS_MAX_IDLE).toInt)
          config.setMaxTotal(properties.getProperty(Contant.JEDIS_MAX_TOTAL).toInt)
          config.setTestOnBorrow(properties.getProperty(Contant.JEDIS_ON_BORROW).toBoolean)
          pool = new JedisPool(config,properties.getProperty(Contant.JEDIS_HOST),
            properties.getProperty(Contant.JEDIS_PORT).toInt)
        }

    pool.getResource
  }



  def close(jedis:Jedis): Unit ={
    if(jedis!=null){
      pool.returnResourceObject(jedis)
    }

  }
}
