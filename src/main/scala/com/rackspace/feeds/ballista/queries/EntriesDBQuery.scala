package com.rackspace.feeds.ballista.queries

import java.sql.Connection
import javax.sql.DataSource

import com.rackspace.feeds.ballista.config.CommandOptionsParser
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory


class EntriesDBQuery extends DBQuery {

  val logger = LoggerFactory.getLogger(getClass)
  
  override def fetch(runDate: DateTime, datacenter: String, dataSource: DataSource): String = {
    val tableName = getTableName(runDate, dataSource)
    
    logger.debug(s"Preparing query to extract data from partitioned entries table[$tableName] for runDate[$runDate]")
    
    if (!isTableExist(tableName, dataSource)) {
      throw new RuntimeException("Partitioned table[$tableName] does not exist")
    }
    s"""
       | COPY (SELECT id, entryid, creationdate, datelastupdated, 
       |              regexp_replace(entrybody, E'[\\n\\r]+', ' ', 'g')
       |              feed, array_to_string( categories, '|' ) as categories, 
       |              eventtype, tenantid, '$datacenter' as dc
       |         FROM $tableName
       |        limit 10)
       |   TO STDOUT
       | WITH DELIMITER $DELIMITER
     """.stripMargin
  }

  /**
   * Based on the given runDate, computes the corresponding partitioned entries 
   * table name which contains the data for the given runDate
   *  
   * @param runDate
   * @param dataSource
   * @return partitioned entries table name
   */
  private def getTableName(runDate: DateTime, dataSource: DataSource): String = {

    val runDateStr = DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)

    var connection:Connection  = null
  
    try {
      connection  = dataSource.getConnection
      val statement = connection.createStatement

      if (!CommandOptionsParser.isValidRunDate(runDate, DateTime.now.withTimeAtStartOfDay())) {
        logger.error(s"!!!! runDate[$runDate] is invalid. !!!")
        throw new RuntimeException(s"runDate[$runDate] is invalid.")
      }

      /**
       * entries table is partitioned based on day of the week with table name convention
       * entries_1 thru entrires_7 . ISDOW returns the day of the week as Monday(1) to Sunday(7).
       */
      val resultSet = statement.executeQuery (
        s"""
          | select 'entries_' || ltrim(to_char(EXTRACT(ISODOW 
          |   from to_timestamp('$runDateStr', 'YYYY-MM-DD')), '9'))
          | as table_name
        """.stripMargin
      )

      resultSet.next()
      resultSet.getString("table_name")
      
    } catch {
      case e: Exception => throw new RuntimeException("Error retrieving the correct entries partition table", e)
    } finally {
      connection.close()
    }
  }

  /**
   * Returns true if the table is present in the underlying database
   *
   * @param tableName
   * @param dataSource
   * @return true/false indicating the presence of the table.
   */
  private def isTableExist(tableName: String, dataSource: DataSource) = {

    var connection:Connection  = null
    var tableCount:Int = 0
    
    try {
      connection  = dataSource.getConnection
      val statement = connection.createStatement

      val resultSet = statement.executeQuery (
        s"""
          | select count(0) as table_count 
          |   from pg_tables 
          |  where schemaname = 'public' 
          |    and tablename = '$tableName'
        """.stripMargin
      )

      resultSet.next()
      tableCount = resultSet.getInt("table_count")

    } catch {
      case e: Exception => 
        throw new RuntimeException(s"Error validating the existence of partitioned entries table[$tableName]", e)
    } finally {
      connection.close()
    }
    
    if (tableCount != 1) {
      false
    } else {
      true
    }
    
  }

}
