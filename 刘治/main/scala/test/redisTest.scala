package test

import redis.clients.jedis.Jedis

object redisTest {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("MyDis", 6379, 2000)
//    jedis.set("aaa","bbb")
    println(jedis.exists("aaa"))
  }

}
