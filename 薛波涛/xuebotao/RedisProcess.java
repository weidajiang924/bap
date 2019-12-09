package com.xuebotao.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

/**
 * 查询redis和存储redis
 */
public class RedisProcess  {

    private static  Jedis jedis;
    @Before
    public  static  void  setup(){
        jedis= RedisUtil.getJedisPool();
        //jedis =new Jedis("xue01",6379,2000);

    }

    /**
     * 查询
     */
    @Test
    public static  String selecthash(String hash){
        /*//存储员工的姓名
        String code =jedis.set("empName","薛波涛");
        System.out.println(code);  //OK*/

        String empname=jedis.get(hash);
        if (empname!=null){
            return empname;
        }
        else {
            return "null";
        }
    }

    public static  String savehash(String hash, String sahnqguan){
        //存储员工的姓名
        String code =jedis.set(hash,sahnqguan);
        System.out.println(code);
      return  code;
    }

    @After
    public void  cleanUp(){
        RedisUtil.releaseReource(jedis);
    }

}
