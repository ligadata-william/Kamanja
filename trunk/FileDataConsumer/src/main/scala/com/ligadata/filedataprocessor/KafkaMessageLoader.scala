package com.ligadata.filedataprocessor

import java.io.{File, PrintWriter}
import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by danielkozin on 9/24/15.
 */
class KafkaMessageLoader(broker: String, topic: String) {
  val pw = new PrintWriter(new File("/tmp/output.txt" ))
  var partIdx: Int = 0
  // Set up some properties for the Kafka Producer
  val props = new Properties()
  props.put("metadata.broker.list", broker);
  props.put("request.required.acks", "1")

  // create the producer object
  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def pushData(messages: Array[Array[Byte]]): Unit = {
    //
  }

  def pushData(messages: Array[KafkaMessage]): Unit = {
   // messages.foreach(msg => {
   //   pw.write(msg.msg)
  //    pw.write('\n')
//      println("\nKafkaMessage:\n  File: " + msg.relatedFileName+", offset:  "+ msg.offsetInFile + "\n " + new String(msg.msg))
   // })
  //  pw.close
  }

  // Push the data into kafka
  private def insertData(msg: String): Unit = {

    if (send(producer, topic, msg.getBytes("UTF8"), partIdx.toString.getBytes("UTF8"))) {
      partIdx = partIdx + 1
      //return "SUCCESS: Added message to Topic:"+topic
    } else {
      println("FAILURE: Failed to add message to Topic:"+topic)
    }
  }

  /**
   * send message
   */
  private def send(producer: Producer[AnyRef, AnyRef], topic: String, message: Array[Byte], partIdx: Array[Byte]): Boolean = {
    try {
      producer.send(new KeyedMessage(topic, partIdx, message))
      return true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return false
    }
  }
}
