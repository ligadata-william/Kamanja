
package com.ligadata.KamanjaBase

class AdapterConfiguration {
  var Name: String = _ // Name of the Adapter, KafkaQueue Name/MQ Name/File Adapter Logical Name/etc
  var formatOrInputAdapterName: String = _ // CSV/JSON/XML for input adapter. For output it is just corresponding input adapter name. For Status it is default
  var delimiterString: String = _ // Delimiter String for CSV
  var associatedMsg: String = _ // Queue Associated Message
  var className: String = _ // Class where the Adapter can be loaded (Object derived from InputAdapterObj)
  var jarName: String = _ // Jar where the className can be found
  var dependencyJars: Set[String] = _ // All dependency Jars for jarName 
  var adapterSpecificCfg: String = _ // adapter specific (mostly json) string 
}

trait CountersAdapter {
  def addCntr(key: String, cnt: Long): Long
  def addCntr(cntrs: scala.collection.immutable.Map[String, Long]): Unit
  def getCntr(key: String): Long
  def getDispString(delim: String): String
  def copyMap: scala.collection.immutable.Map[String, Long]
}

// Input Adapter Object to create Adapter
trait InputAdapterObj {
  def CreateInputAdapter(inputConfig: AdapterConfiguration, output: Array[OutputAdapter], envCtxt: EnvContext, mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter): InputAdapter
}

// Input Adapter
trait InputAdapter {
  val inputConfig: AdapterConfiguration // Configuration
  val output: Array[OutputAdapter] // Outputs adapters associated to this Input adapter
  val envCtxt: EnvContext // Environment Context

  def UniqueName: String = { // Making String from key
    return "{\"Name\" : \"%s\"}".format(inputConfig.Name)
  }

  def Category = "Input"
  def Shutdown: Unit
  def StopProcessing: Unit
  def StartProcessing(maxParts: Int, partitionInfo: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, (PartitionUniqueRecordValue, Int, Int))], ignoreFirstMsg: Boolean): Unit // each value in partitionInfo is (PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue). // key, processed value, Start transactionid, Ignore Output Till given Value (Which is written into Output Adapter) & processing Transformed messages (processing & total)
  def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey]
  def DeserializeKey(k: String): PartitionUniqueRecordKey
  def DeserializeValue(v: String): PartitionUniqueRecordValue
  def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
  def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
}

// Output Adapter Object to create Adapter
trait OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter
}

// Output Adapter
trait OutputAdapter {
  val inputConfig: AdapterConfiguration // Configuration

  def send(message: String, partKey: String): Unit
  def send(message: Array[Byte], partKey: Array[Byte]): Unit
  def Shutdown: Unit
  def Category = "Output"
}

trait ExecContext {
  val input: InputAdapter
  val curPartitionId: Int
  val output: Array[OutputAdapter]
  val envCtxt: EnvContext

  def execute(transId: Long, data: String, format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, processingXformMsg: Int, totalXformMsg: Int, associatedMsg: String, delimiterString: String): Unit
}

trait MakeExecContext {
  def CreateExecContext(input: InputAdapter, curPartitionId: Int, output: Array[OutputAdapter], envCtxt: EnvContext): ExecContext
}

trait PartitionUniqueRecordKey {
  val Type: String // Type of the Key -- For now putting File/Kafka like that. This is mostly for readable purpose (for which adapter etc)
  def Serialize: String // Making String from key
  def Deserialize(key: String): Unit // Making Key from Serialized String
}

trait PartitionUniqueRecordValue {
  def Serialize: String // Making String from Value
  def Deserialize(key: String): Unit // Making Value from Serialized String
}


