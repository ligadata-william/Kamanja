package com.ligadata.keyvaluestore.cassandra
import com.ligadata.keyvaluestore._
import com.datastax.driver.core.BatchStatement
import java.nio.ByteBuffer

object KeyValueBatchTransaction{
  def putCassandraBatch(tableList: Array[String],store: DataStore,objArray: Array[IStorage]){
    val batch = new BatchStatement
    var i = 0
    var session = store.asInstanceOf[KeyValueCassandra].session
    objArray.foreach ( obj => { 
      var key = ByteBuffer.wrap(obj.Key.toArray[Byte]);
      var value = ByteBuffer.wrap(obj.Value.toArray[Byte]);
      var updateStmt = session.prepare("UPDATE " + tableList(i)  + " SET value = ? WHERE Key=?;")
      batch.add(updateStmt.bind(value,key));
      i = i + 1
    })
    session.execute(batch);
  }
}
