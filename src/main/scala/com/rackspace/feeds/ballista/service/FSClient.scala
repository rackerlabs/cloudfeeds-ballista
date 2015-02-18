package com.rackspace.feeds.ballista.service

import java.io.OutputStream

trait FSClient {

  /**
   * Gets output stream for the provided filePath
   *
   * @param filePath
   * @return output stream
   */
  def getOutputStream(filePath: String): OutputStream
}
