package com.ligadata.MetadataAPITest

import org.scalatest._

class TestAllSuites extends FunSuite {
  var ts1 = new APIInitSpec
  ts1.execute()
  var ts2 = new ContainerSpec
  ts2.execute()
  var ts3 = new MessageSpec
  ts3.execute()
  var ts4 = new ModelSpec
  ts4.execute()
  var ts5 = new TypeSpec
  ts5.execute()
  var ts6 = new FunctionSpec
  ts6.execute()
}
