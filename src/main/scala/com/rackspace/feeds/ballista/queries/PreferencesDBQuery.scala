package com.rackspace.feeds.ballista.queries

import javax.sql.DataSource

import org.joda.time.DateTime

class PreferencesDBQuery extends DBQuery {

  override def fetch(runDate: DateTime, datacenter: String, dataSource: DataSource): String = {
    s"""
       | COPY (SELECT *
       |         FROM preferences
       |        limit 10)
       |   TO STDOUT 
       | WITH DELIMITER ','
     """.stripMargin
    
  }
  

}
