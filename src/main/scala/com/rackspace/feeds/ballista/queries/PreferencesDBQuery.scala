package com.rackspace.feeds.ballista.queries

import javax.sql.DataSource

import org.joda.time.DateTime
import org.apache.commons.lang3.NotImplementedException

class PreferencesDBQuery extends DBQuery {

  override def fetch(runDate: DateTime, region: String, dataSource: DataSource, maxRowLimit: String): String = {
    this.fetch(runDate, Set.empty, region, dataSource, maxRowLimit)
  }
  
  override def fetch(runDate: DateTime, tenantIds: Set[String], region: String, dataSource: DataSource, maxRowLimit: String): String = {
    var whereClause = ""
    if ((tenantIds != null) && (tenantIds.nonEmpty)) {
      // escape each tenantId to prevent SQL injection
      val escapedTenantIds = tenantIds.map(tid => tid.replace("'", "''"))
      whereClause = "WHERE id in ('" + escapedTenantIds.toArray.mkString("','") + "')"
    }

    s"""
       | COPY (SELECT id,
       |              alternate_id,
       |              json_extract_path_text(payload::json, 'enabled') as enabled,
       |              payload,
       |              created,
       |              updated
       |         FROM preferences $whereClause
       |        limit $maxRowLimit)
       |   TO STDOUT
       | WITH DELIMITER $PG_DELIMITER
     """.stripMargin
  }
}
