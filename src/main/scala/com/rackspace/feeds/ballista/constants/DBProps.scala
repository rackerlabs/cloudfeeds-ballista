package com.rackspace.feeds.ballista.constants

object DBProps extends Enumeration {
  type propertyName = Value
  val dbName, driverClass, jdbcUrl, user, password, outputFileLocation, fileNamePrefix, queryClass = Value
}
