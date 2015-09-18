package com.ligadata.jpmml.deployment

/**
 *
 */
object JpmmlModelDeployPerfTestApp extends App {
  assert(args.length ==1 || args.length == 2, s"Can only provide one or two args. Provided - ${args.length} args")
  val pmmlPath =  args(0)
  val count = if (args.length == 2) args(1).toInt else 1000

  val testHelper = new JpmmlModelDeployPerfTestHelper
  testHelper.runTest(count, pmmlPath)
}
