package com.rackspace.feeds.ballista.queries

import javax.sql.DataSource

import org.joda.time.DateTime

/**
 * Trait designed to be extended by classes to provide their own
 * custom queries
 */
trait DBQuery {
  
  val PG_DELIMITER = "E'\\x01'"  //ctrl-A character
  
  /**
   * Generates a database query to extract data from the database.
   *
   * @param runDate
   * @param datacenter
   * @param dataSource
   * @return a database query to extract data
   */
  def fetch(runDate: DateTime, datacenter: String, dataSource: DataSource, maxRowLimit: String): String
}
