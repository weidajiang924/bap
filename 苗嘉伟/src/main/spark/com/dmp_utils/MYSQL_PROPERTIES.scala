package com.dmp_utils

import java.util.Properties

object MYSQL_PROPERTIES {
  def GetConnectionProperties:Properties={
    val connectionProperties = new Properties()
    connectionProperties.put("user", SparkParams.MYSQL_USERNAME)
    connectionProperties.put("password", SparkParams.MYSQL_PASSWORD)
    connectionProperties
  }
}