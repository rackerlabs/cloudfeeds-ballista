package com.rackspace.feeds.ballista.service

import java.io.OutputStream
import javax.sql.DataSource

trait DataExport {

  /**
   * This method retrieves a database connection from the dataSource, executes 
   * the query to retrieve data from database and writes the output to a file stream.
   *
   * @param dataSource
   * @param query
   * @param outputStream
   * @return number of records copied
   */
  def export(dataSource: DataSource, query: String, outputStream: OutputStream): Long
}
