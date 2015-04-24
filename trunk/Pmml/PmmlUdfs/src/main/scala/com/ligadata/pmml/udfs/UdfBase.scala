package com.ligadata.pmml.udfs

import scala.reflect.runtime.universe._

/**
 * UdfBase trait is inherited by all UDF libraries to be used in the Pmml models executed by Fatafat.
 * It provides key access to the function members of the 'object' methods defined so they may 
 * be cataloged in the Fatafat metadata. 
 * 
 * @see UdfExtract for how function information is extracted from an object. 
 */

trait UdfBase {
	val typeMirror = runtimeMirror(this.getClass.getClassLoader)
	val instanceMirror = typeMirror.reflect(this)
	val members = instanceMirror.symbol.typeSignature.members
} 
