package com.ligadata.keyvaluestore

import com.ligadata._

object KeyValueManager
{
	// We will add more implementations here 
	// so we can test  the system characteristics
	//
	def Get(parameter: keyvaluestore.PropertyMap) : keyvaluestore.DataStore = 
	{
		parameter("connectiontype") match 
		{
			// Other KV stored
			case "voldemort" =>  return new keyvaluestore.voldemort.KeyValueVoldemort(parameter)
			case "hbase" =>  return new keyvaluestore.hbase.KeyValueHBase(parameter)
			// Simple file base implementations
			case "treemap" =>  return new keyvaluestore.mapdb.KeyValueTreeMap(parameter)
			case "hashmap" =>  return new keyvaluestore.mapdb.KeyValueHashMap(parameter)
			case "redis" =>  return new keyvaluestore.redis.KeyValueRedis(parameter)
			
			// Default, Cassandra is nothing matched
			case _ => return new keyvaluestore.cassandra.KeyValueCassandra(parameter)
		}
	}
}
