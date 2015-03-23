/*
 * Store a Audit Object
 *
 */
package com.ligadata.keyvaluestore

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.ligadata.keyvaluestore.cassandra.KeyValueCassandra
import java.lang.Comparable
import java.util.Date

class AuditRecord
{
  var auditTime: String = _
  var action: String = _
  var notes : String = _
  var objectAccessed : String = _
  var success: String = _
  var transactionId: String = _
  var userOrRole : String = _
  var userPrivilege: String = _

  override def toString : String = 
    "(" + auditTime + "," + action + "," + "," + objectAccessed + "," + success + "," + transactionId + "," + userOrRole + "," + userPrivilege + ")"
}

trait AuditStoreOperations
{
  def add(rec: AuditRecord)
  def get(startTime: Date, endTime: Date, userOrRole: String, action: String, objectAccessed: String): Array[AuditRecord]
}

trait AuditStore extends AuditStoreOperations
{
  def Shutdown() = {}
}
