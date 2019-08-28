package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import sun.net.www.http.HttpClient

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/24
  */
/**
  * http协议请求
  */
object HttpUtil {
  //GET请求
  def get(url:String):String={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val get = new HttpGet(url)

    //发送请求
    val response: CloseableHttpResponse = client.execute(get)
    //获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")

  }
}
