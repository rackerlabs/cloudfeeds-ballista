package com.rackspace.feeds.ballista.service

import java.io.OutputStream
import javax.sql.DataSource

import com.rackspace.feeds.ballista.util.DataSourceUtil
import org.postgresql.PGConnection

class PGDataExport extends DataExport {

  override def export(dataSource: DataSource, query: String, outputStream: OutputStream): Long = {
    try {

      val pgConnection: PGConnection = DataSourceUtil.getPGConnection(dataSource)

      pgConnection.getCopyAPI
        .copyOut(query, outputStream)

    } finally {
      outputStream.close()
    }
  }
}
