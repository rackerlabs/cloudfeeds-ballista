package com.rackspace.feeds.ballista.queries

import javax.sql.DataSource

import org.joda.time.DateTime
import org.apache.commons.lang3.NotImplementedException

class PreferencesDBQuery extends DBQuery {

  override def fetch(runDate: DateTime, region: String, dataSource: DataSource, maxRowLimit: String): String = {
    s"""
       | COPY (SELECT id,
       |              alternate_id,
       |              json_extract_path_text(payload::json, 'enabled') as enabled,
       |              payload,
       |              created,
       |              updated
       |         FROM preferences
       |        limit $maxRowLimit)
       |   TO STDOUT
       | WITH DELIMITER $PG_DELIMITER
     """.stripMargin
    
  }
  
  override def fetch(runDate: DateTime, tenantIds: Set[String], region: String, dataSource: DataSource, maxRowLimit: String): String = {
    throw new NotImplementedException("not implemented for PreferencesDBQuery")
  }
}
