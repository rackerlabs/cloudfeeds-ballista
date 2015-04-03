package com.rackspace.feeds.ballista.util

import java.io._

import com.jcraft.jsch.{Channel, ChannelExec, Session}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.io.Source

class SCPUtil {

  val DefaultBufSize = 1024
  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Using sessionInfo, creates remote directory $remoteFileLocation/$remoteDir
   *
   * @param sessionInfo
   * @param remoteFileLocation
   * @param remoteDir
   */
  def scpEmptyDirectory(sessionInfo: SCPSessionInfo, remoteFileLocation: String, remoteDir: String) = {

    logger.info(s"SCP start: creating remote directory [$remoteFileLocation/$remoteDir]")
    
    val protocolMessage = new StringBuilder
    protocolMessage.append(s"D0755 0 $remoteDir\n")
    protocolMessage.append("E\n")
    
    scpInternal(sessionInfo, localFilePath = "", remoteFileLocation, isDirectory = true, protocolMessage.toString())    
    
    logger.info(s"SCP end: created remote directory [$remoteFileLocation/$remoteDir]")
  }

  /**
   * Using $sessionInfo, scp's the file $localFilePath to $remoteFileLocation/$remoteFileName 
   *  
   * @param sessionInfo
   * @param localFilePath
   * @param remoteFileLocation
   * @param remoteFileName                           
   */
  def scpFile(sessionInfo: SCPSessionInfo, localFilePath: String, remoteFileLocation: String, remoteFileName: String) = {

    logger.info(s"SCP start: local file [$localFilePath] to remote file [$remoteFileLocation/$remoteFileName]")

    val localFile = new File(localFilePath)
    val localFileSize = localFile.length()
    
    val streamCommand = s"C0644 $localFileSize $remoteFileName\n"
    
    scpInternal(sessionInfo, localFilePath, remoteFileLocation, isDirectory = false, streamCommand)
    
    logger.info(s"SCP end: local file [$localFilePath] to remote file [$remoteFileLocation/$remoteFileName]")
  }

  /**
   * Using $sessionInfo, scp's the file $localFilePath to $remoteFileLocation based on the $protocolMessage. Can be
   * used to either to scp files or empty directories based on the $isDirectory flag. 
   * 
   * - protocolMessage has to be set appropriate based on whether directory or file is being scp'ed.
   * - if localFilePath is empty, content is empty while writing file and directory is empty while creating directory. 
   *   Note that protocolMessage has to be changed appropriately
   *  
   * @param sessionInfo
   * @param localFilePath
   * @param remoteFileLocation
   * @param isDirectory
   * @param protocolMessage
   */
  private def scpInternal(sessionInfo: SCPSessionInfo, localFilePath: String, remoteFileLocation: String, isDirectory: Boolean, protocolMessage: String) = {

    var remoteOutputStream: OutputStream = null
    var remoteInputStream: InputStream = null

    try {

      val session: Session = sessionInfo.createSession()
      session.connect()

      val scpCommand = buildSCPCommand(remoteFileLocation, isDirectory)
      val channel: Channel = openChannelAndSetCommand(remoteFileLocation, session, scpCommand)

      remoteOutputStream = channel.getOutputStream
      remoteInputStream = channel.getInputStream

      channel.connect()

      if (isValidTransfer(remoteInputStream)) {
        logger.debug("channel connection successful")
      }

      sendProtocolMessage(remoteOutputStream, remoteInputStream, protocolMessage)

      if (StringUtils.isNotEmpty(localFilePath))
        sendContent(localFilePath, remoteOutputStream, remoteInputStream)

      IOUtils.closeQuietly(remoteOutputStream)

      channel.disconnect()
      session.disconnect()

    } finally {
      IOUtils.closeQuietly(remoteOutputStream)
      IOUtils.closeQuietly(remoteInputStream)
    }

  }
  
  private def buildSCPCommand(remoteFileLocation: String, isDirectory: Boolean) = {

    /**
     *  scp -t is meant for running scp in sink mode on the remote node. These options are for internal
     *  usage only and aren't documented. I found this link very helpful.
     *  (https://blogs.oracle.com/janp/entry/how_the_scp_protocol_works)
     *
     *  Jsch library internally parses this command into a String array. So be careful 
     *  to not include any extra spaces inbetween.
     *
     *  Adding r flag if directory creation is involved
     */
    
    val recursiveCopyFlag = if (isDirectory) "r" else ""
    s"scp -${recursiveCopyFlag}t $remoteFileLocation"
  }
  
  private def openChannelAndSetCommand(remoteFileLocation: String, session: Session, command: String): Channel = {


    val channel = session.openChannel("exec")

    logger.debug(s"scp command: [$command]")
    channel.asInstanceOf[ChannelExec].setCommand(command)
    channel
  }

  /**
   * This method sends the raw data to the consumer (scp in sink mode) by writing
   * to the $remoteOutputStream. The consumer reads exactly that much data as specified 
   * in the length field previously sent with raw protocol message.
   *
   * @param localFilePath
   * @param remoteOutputStream
   * @param remoteInputStream
   */
  private def sendContent(localFilePath: String, remoteOutputStream: OutputStream, remoteInputStream: InputStream): Unit = {
    val buf: Array[Byte] = new Array[Byte](DefaultBufSize)

    val inputStream = new FileInputStream(localFilePath)
    try {
      Iterator
        .continually (inputStream.read(buf, 0, buf.length))
        .takeWhile (numberOfBytesRead => numberOfBytesRead != -1)
        .foreach (numberOfBytesRead => remoteOutputStream.write(buf, 0, numberOfBytesRead))
    } finally {
      IOUtils.closeQuietly(inputStream)
    }

    buf(0) = 0.toByte
    remoteOutputStream.write(buf, 0, 1)
    remoteOutputStream.flush()

    if (isValidTransfer(remoteInputStream)) {
      logger.debug("file content sent successfully")
    }
  }

  /**
   * 
   * Some sample protocol messages 
   *
   * Cmmmm <length> <filename>
   *  a single file copy, mmmmm is mode(as the one used in chmod command). Example: C0644 299 remote_file_name
   *  C indicates file copy (D indicates directory)
   * 
   * after C message the data is expected (unless the file is empty)
   *  
   * Dmmmm <length> <dirname>
   *  start of recursive directory copy. Length is ignored but must be present. Example: D0755 0 docs
   *
   * after D message either C or E is expected. This means that it's correct to copy an empty directory providing that user used -r option.
   *
   * @param out remote output stream
   * @param in remoate intput stream
   * @param protocolMessage scp protocol message
   */
  private def sendProtocolMessage(out: OutputStream, in: InputStream, protocolMessage: String) {

    out.write(protocolMessage.getBytes)
    out.flush()

    if (isValidTransfer(in)) {
      logger.debug(s"[$protocolMessage] message sent successfully")
    }
  }

  /**
   * This method returns true for a valid transfer and throws RuntimeException if not.
   *
   * The consumer(scp in sink mode(-t)) can reply in 3 different messages; 
   * binary 0 (OK), 1 (warning) or 2 (fatal error; will end the connection). 
   *
   * Messages 1 and 2 can be followed by a the error message and then followed 
   * by a new line character.
   *
   * @param inputStream
   * @return true/false indicating a valid transfer
   */
  private def isValidTransfer(inputStream: InputStream): Boolean = {
    inputStream.read match {
      case 0 => true
      case anyOtherValue => {
        val message = Source.fromInputStream(inputStream).getLines().mkString
        val errorMessage = s"scp -t responded with $anyOtherValue message code followed by error message [$message]"
        logger.error(errorMessage)
        throw new RuntimeException(errorMessage)
      }
    }
  }

}

