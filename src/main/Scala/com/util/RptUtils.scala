package com.util

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/21
  */
object RptUtils {
  //处理请求数
//ysrq|sxrq|mzadrq|cypt|successpt|clickrealshow|clickreallook|winPrice|adpayment|
  def request(requestmode:Int,processnode:Int):List[Double]={
    var ysrq =  0
    var sxrq = 0
    var mzadrq = 0
    if (requestmode==1&& processnode >=1){
      ysrq = 1
    }
  if (requestmode==1&& processnode >=2){
    sxrq = 1
  }
  if (requestmode==1&& processnode == 3){
     mzadrq = 1
  }
   List(ysrq,sxrq,mzadrq)
  }

  //处理展示点击数

  def click(requestmode:Int,iseffective:Int):List[Double]={
    var clickrealshow = 0
    var clickreallook = 0
    if (requestmode ==2&& iseffective==1){clickrealshow = 1}
    if (requestmode ==3&& iseffective==1){clickrealshow = 1}
List(clickrealshow,clickreallook)

  }
  //处理竞价操作

  def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Double,
         WinPrice:Double,adpayment:Double):List[Double]={

    var cypt = 0.0
    var successpt = 0.0
    var winPrice = 0.0
    var Adpayment = 0.0
  if (iseffective==1&&isbilling==1&&isbid==1){cypt=1}
    if(iseffective==1&&isbilling==1&& iswin ==1&&adorderid != 0){successpt=1}
    if(iseffective==1&&isbilling==1&& iswin ==1){
       winPrice = WinPrice / 1000
    }
    if(iseffective==1&&isbilling==1&& iswin ==1){
      Adpayment = adpayment / 1000
    }
    List(cypt,successpt,winPrice,Adpayment)
  }


}
