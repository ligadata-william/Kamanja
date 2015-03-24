/*
 * Store a Audit Object
 *
 */
package com.ligadata.keyvaluestore

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.ligadata.olep.metadata._
import java.lang.Comparable

import java.util.Date


trait AuditStoreOperations
{
  def add(rec: AuditRecord)
  def get(startTime: Date, endTime: Date, userOrRole: String, action: String, objectAccessed: String): Array[AuditRecord]
}

trait AuditStore extends AuditStoreOperations
{
  def Shutdown() = {}
}
