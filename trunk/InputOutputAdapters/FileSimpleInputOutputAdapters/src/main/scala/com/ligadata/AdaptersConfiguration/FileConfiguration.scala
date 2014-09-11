
package com.ligadata.AdaptersConfiguration

import com.ligadata.OnLEPBase.AdapterConfiguration

class FileAdapterConfiguration extends AdapterConfiguration {
  var Files: Array[String] = _ // Array of files and each execute one by one
  var CompressionString: String = _ // If it is null or empty we treat it as TEXT file
  var MessagePrefix: String = _ // This is the first String in the message
  var IgnoreLines: Int = _ // number of lines to ignore in each file
  var AddTS2MsgFlag: Boolean = false // Add TS after the Prefix Msg
}
