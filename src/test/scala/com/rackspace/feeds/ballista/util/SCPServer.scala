package com.rackspace.feeds.ballista.util

import org.apache.sshd.SshServer
import org.apache.sshd.common.NamedFactory
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.{PasswordAuthenticator, UserAuth}
import org.apache.sshd.server.auth.{UserAuthPassword, UserAuthNone}
import org.apache.sshd.server.command.ScpCommandFactory
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

sealed trait SSHPasswordAuthenticator extends PasswordAuthenticator {
  val USERNAME = "test"
  val PASSWORD = "test"
  override def authenticate(username: String, password: String, session: ServerSession): Boolean = {
    USERNAME.equalsIgnoreCase(username) && PASSWORD.equalsIgnoreCase(password)
  }
}

/**
 * This class creates a SCP server. Once the server starts, you can scp files to it
 * using the sample command below.
 *
 * scp -P 9022 input_file.txt test@localhost:output_file.txt 
 *  
 * This class is not thread safe
 */
object SCPServer {
  
  val logger = LoggerFactory.getLogger(getClass)
  val sshd = SshServer.setUpDefaultServer()

  val DEFAULT_PORT = 9022
  def configureAndStartServer(): Unit = {
    try {
      sshd.setPort(DEFAULT_PORT)
      sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider("hostkey.ser"))
      sshd.setUserAuthFactories(List[NamedFactory[UserAuth]](new UserAuthPassword.Factory()).asJava)
      sshd.setPasswordAuthenticator(new SSHPasswordAuthenticator{})
      sshd.setCommandFactory(new ScpCommandFactory())
      sshd.start()
    }
    catch {
      case ex:Exception => logger.error("SCP Server failed to start", ex)
    }
  }
  
  def stopServer():Unit = {
    try {
      sshd.stop()
    }
    catch {
      case ex:Exception => logger.error("SCP Server failed to stop", ex)
    }
  }
}
