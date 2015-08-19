package com.ligadata.pmml.support

import scala.collection.mutable.TreeSet
import scala.reflect.runtime.{ universe => ru }
import java.io.{ File }

object PMMLConfiguration {
  var jarPaths: collection.immutable.Set[String] = _
}
