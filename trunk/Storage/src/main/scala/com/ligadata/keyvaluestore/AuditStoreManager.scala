package com.ligadata.keyvaluestore

import com.ligadata._

object AuditStoreManager
{
  // We will add more implementations here 
  // so we can test  the system characteristics
  //
  def Get(parameter: keyvaluestore.PropertyMap) : keyvaluestore.AuditStore = {
    parameter("connectiontype") match {
      // Other KV stored
      case "hbase" =>  return new keyvaluestore.hbase.AuditStoreHBase(parameter)
      case "cassandra" =>  return new keyvaluestore.cassandra.AuditStoreCassandra(parameter)
      // Default, thrown an exception
      case _ => throw new Exception("Auditing is not defined for the datastoretype : " + parameter("connectiontype"))
    }
  }
}
