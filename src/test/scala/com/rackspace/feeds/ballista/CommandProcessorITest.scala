package com.rackspace.feeds.ballista

import java.io._
import java.nio.file.{Files, Paths}
import java.sql.{Connection, ResultSet}
import java.util.zip.GZIPInputStream
import javax.sql.DataSource

import com.rackspace.feeds.ballista.config.{AppConfig, CommandOptions}
import com.rackspace.feeds.ballista.constants.DBProps
import com.rackspace.feeds.ballista.queries.EntriesDBQuery
import com.rackspace.feeds.ballista.service.{DataExport, DefaultExportSvc}
import com.rackspace.feeds.ballista.util.SCPServer
import org.apache.commons.dbutils.DbUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.h2.tools.Csv
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * This class is created to use EntriesDBQuery and extract the
 * underneath select statement used.
 */
class TestEntriesDBQuery extends EntriesDBQuery {

  override def getTableName(runDate: DateTime, dataSource: DataSource): String = "entries"
  override def isTableExist(tableName: String, dataSource: DataSource) = true

  override def fetch(runDate: DateTime, datacenter: String, dataSource: DataSource, maxRowLimit: String): String = {
    val pgSQL = super.fetch(runDate, datacenter, dataSource, maxRowLimit)
    
    //hack: To use the same select query as the one used by the actual implementation. 
    //Stripping out the copy part of the sql and converting some functions to h2 specific
    val selectQuery = pgSQL.substring(pgSQL.indexOf("(") + 1, pgSQL.lastIndexOf(")"))
    selectQuery.replace(selectQuery.substring(selectQuery.indexOf("regexp_replace"), selectQuery.indexOf(" as entrybody,")),
                  "entrybody")
               .replace(selectQuery.substring(selectQuery.indexOf("array_to_string"), selectQuery.indexOf(" as categories,")),
                  "CONCAT_WS('|', ARRAY_GET(categories, 1), ARRAY_GET(categories, 2), ARRAY_GET(categories, 3), ARRAY_GET(categories, 4))")

  }
}

class H2DataExport extends DataExport {

  override def export(dataSource: DataSource, query: String, outputStream: OutputStream): Long = {
    var connection: Connection = null

    try {

      connection = dataSource.getConnection
      val resultSet: ResultSet = connection.createStatement().executeQuery(query)

      val csv: Csv = new Csv()
      csv.setWriteColumnHeader(false)
      csv.write(new OutputStreamWriter(outputStream), resultSet)

    } finally {
      DbUtils.closeQuietly(connection)
      IOUtils.closeQuietly(outputStream)
    }
  }
}

/**
 * Create TestCommandProcessor to test CommandProcessor with H2DataExport 
 * instead of PGDataExport
 */
class TestCommandProcessor extends CommandProcessor {
  
  override def getExportSvc(dbName: String): DefaultExportSvc = {
    
    new DefaultExportSvc(dbName) {
      override val dataExport = new H2DataExport
    }
  }
  
}

class CommandProcessorITest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll() = {
    SCPServer.configureAndStartServer()
  }

  override def afterAll() = {
    SCPServer.stopServer()
  }
  
  test("Verify that extracting testdb data and scp'ing is successful") {

    val runDate: DateTime = DateTime.now.minusDays(1).withTimeAtStartOfDay()
    val runDateStr: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(runDate)

    val testdbName: String = "testdb"
    val exitCode = new TestCommandProcessor().doProcess(CommandOptions(runDate, Set(testdbName)))

    assert(exitCode == 0, "Wrong exit code")

    val remoteOutputLocation = AppConfig.export.from.dbs.dbConfigMap(testdbName)(DBProps.outputFileLocation)
    val datacenter = AppConfig.export.datacenter
    val remoteFilePath = s"$remoteOutputLocation/$runDateStr/${datacenter}_${testdbName}_$runDateStr.gz"
    val remoteSuccessFilePath = s"$remoteOutputLocation/$runDateStr/_SUCCESS"

    assert(Files.exists(Paths.get(remoteFilePath)), s"Data for db[$testdbName] not present on remote server at path[$remoteFilePath]")
    assert(Files.exists(Paths.get(remoteSuccessFilePath)), s"Success file not created on remote server at path[$remoteFilePath]")
    assert(scala.io.Source.fromFile(remoteSuccessFilePath).mkString.replaceFirst("$\\n", "") == "testdb=5", "Success file content mismatch")

    var gZipReader:BufferedReader = null
    try {
      gZipReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(remoteFilePath)), "UTF-8"))
      
      var count: Integer = 0
      Iterator.continually(gZipReader.readLine())
        .takeWhile(line => StringUtils.isNotEmpty(line))
        .foreach( line => {
          count += 1
          assert(line.split(",").size == 11, "Number of columns in csv file does not match with hive")
      })

      assert(5 == count, "Total number of records present in gZip file does not match with number of records in database")
      
    } finally {
      IOUtils.closeQuietly(gZipReader)
    }


  }


}