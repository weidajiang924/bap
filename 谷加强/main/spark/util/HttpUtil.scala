package util

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpUtil {
  def get(url:String,header:String = null)={
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    if(header!=null){
      val json = JSON.parseObject(header)
      json.keySet().toArray().map(_.toString)
        .foreach(key=>httpGet.setHeader(key,json.getString(key)))
    }
    val response: CloseableHttpResponse= httpClient.execute(httpGet)
    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
