package com.rackspace.feeds.ballista.queries

import javax.sql.DataSource

import org.joda.time.DateTime

class PreferencesDBQuery extends DBQuery {

  override def fetch(runDate: DateTime, datacenter: String, dataSource: DataSource, maxRowLimit: String): String = {
    s"""
       | COPY (SELECT *
       |         FROM preferences
       |        limit $maxRowLimit)
       |   TO STDOUT 
       | WITH DELIMITER ','
     """.stripMargin
    
  }
  

}
