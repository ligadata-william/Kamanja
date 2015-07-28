
package com.ligadata.OutputAdapters

import java.util.Properties
import org.apache.log4j.Logger
import com.ligadata.FatafatBase.{ AdapterConfiguration, OutputAdapter, OutputAdapterObj, CountersAdapter }
import com.ligadata.AdaptersConfiguration.IbmMqAdapterConfiguration
import javax.jms.{ Connection, Destination, JMSException, Message, MessageProducer, Session, TextMessage, BytesMessage }
import com.ibm.msg.client.jms.JmsConnectionFactory
import com.ibm.msg.client.jms.JmsFactoryFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.common.CommonConstants
import com.ibm.msg.client.jms.JmsConstants
import com.ligadata.Exceptions.StackTrace

object IbmMqProducer extends OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter = new IbmMqProducer(inputConfig, cntrAdapter)
}

class IbmMqProducer(val inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter) extends OutputAdapter {
  private[this] val LOG = Logger.getLogger(getClass);

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = IbmMqAdapterConfiguration.GetAdapterConfig(inputConfig)

  private def printFailure(ex: Exception) {
    if (ex != null) {
      if (ex.isInstanceOf[JMSException]) {
        processJMSException(ex.asInstanceOf[JMSException])
      } else {
        LOG.error(ex)
      }
    }
  }

  private def processJMSException(jmsex: JMSException) {
    LOG.error(jmsex)
    var innerException: Throwable = jmsex.getLinkedException
    if (innerException != null) {
      LOG.error("Inner exception(s):")
    }
    while (innerException != null) {
      LOG.error(innerException)
      innerException = innerException.getCause
    }
  }

  var connection: Connection = null
  var session: Session = null
  var destination: Destination = null
  var producer: MessageProducer = null
  var retval = false

  try {
    val ff = JmsFactoryFactory.getInstance(JmsConstants.WMQ_PROVIDER)
    val cf = ff.createConnectionFactory()
    cf.setStringProperty(CommonConstants.WMQ_HOST_NAME, qc.host_name)
    cf.setIntProperty(CommonConstants.WMQ_PORT, qc.port)
    cf.setStringProperty(CommonConstants.WMQ_CHANNEL, qc.channel)
    cf.setIntProperty(CommonConstants.WMQ_CONNECTION_MODE, qc.connection_mode)
    cf.setStringProperty(CommonConstants.WMQ_QUEUE_MANAGER, qc.queue_manager)
    if (qc.ssl_cipher_suite.size > 0)
      cf.setStringProperty(CommonConstants.WMQ_SSL_CIPHER_SUITE, qc.ssl_cipher_suite)
    // cf.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME, qc.application_name)
    connection = cf.createConnection()
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    if (qc.queue_name != null && qc.queue_name.size > 0)
      destination = session.createQueue(qc.queue_name)
    else if (qc.topic_name != null && qc.topic_name.size > 0)
      destination = session.createTopic(qc.topic_name)
    else {
      // Throw error
    }
    producer = session.createProducer(destination)
    connection.start()
  } catch {
    case jmsex: Exception => {
      printFailure(jmsex)
      val stackTrace = StackTrace.ThrowableTraceString(jmsex)
      LOG.error("StackTrace:"+stackTrace)}
  }

  override def send(message: String, partKey: String): Unit = {
    try {
      // Do we need text Message or Bytes Message?
      if (qc.msgType == com.ligadata.AdaptersConfiguration.MessageType.fByteArray) {
        val outmessage = session.createBytesMessage()
        outmessage.writeBytes(message.getBytes)
        outmessage.setStringProperty("ContentType", qc.content_type)
        producer.send(outmessage)
      } else { // By default we are taking (qc.msgType == com.ligadata.AdaptersConfiguration.MessageType.fText)
        val outmessage = session.createTextMessage(new String(message))
        outmessage.setStringProperty("ContentType", qc.content_type)
        producer.send(outmessage)
      }
      val key = Category + "/" + qc.Name + "/evtCnt"
      cntrAdapter.addCntr(key, 1) // for now adding each row
    } catch {
      case jmsex: Exception => {
        printFailure(jmsex)
        val stackTrace = StackTrace.ThrowableTraceString(jmsex)
      LOG.error("StackTrace:"+stackTrace)
      }
    }
  }

  override def send(message: Array[Byte], partKey: Array[Byte]): Unit = {
    send(new String(message), new String(partKey))
    // If we are going to send ByteArray message, we need to switch sending to here.
  }

  override def Shutdown(): Unit = {
    if (producer != null) {
      try {
        producer.close()
      } catch {
        case jmsex: Exception => {
          val stackTrace = StackTrace.ThrowableTraceString(jmsex)
          LOG.error("Producer could not be closed."+"\nSatcktrace:"+stackTrace)
          printFailure(jmsex)
        }
      }
    }

    // Do we need to close destination ??

    if (session != null) {
      try {
        session.close()
      } catch {
        case jmsex: Exception => {
          val stackTrace = StackTrace.ThrowableTraceString(jmsex)
          LOG.error("Session could not be closed."+"\nStacktrace:"+stackTrace)
          printFailure(jmsex)
        }
      }
    }
    if (connection != null) {
      try {
        connection.close()
      } catch {
        case jmsex: Exception => {
          val stackTrace = StackTrace.ThrowableTraceString(jmsex)
          LOG.error("Connection could not be closed."+"\nStackTrace:"+stackTrace)
          printFailure(jmsex)
        }
      }
    }
  }
}

