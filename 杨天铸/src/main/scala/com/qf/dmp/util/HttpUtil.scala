package com.qf.dmp.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils


object HttpUtil {

  def get(url:String,header:String = null):String={

    val client: CloseableHttpClient = HttpClients.createDefault()
    val get = new HttpGet(url)
    //设置header
    if(header!=null){
      val json: JSONObject = JSON.parseObject(header)
      json.keySet()
        .toArray
        .map(_.toString)
        .foreach(key=>get.setHeader(key,json.getString(key)))
    }
    //发送请求
    val response: CloseableHttpResponse = client.execute(get)
    //获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")
  }

}
