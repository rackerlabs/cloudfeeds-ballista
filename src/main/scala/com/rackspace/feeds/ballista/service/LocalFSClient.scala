package com.rackspace.feeds.ballista.service

import java.io.{FileOutputStream, OutputStream}

class LocalFSClient extends FSClient {
  
  /**
   * Gets output stream for the provided filePath
   *
   * @param filePath
   * @return output stream
   */
  override def getOutputStream(filePath: String): OutputStream = {
    new FileOutputStream(filePath)
  }
}
