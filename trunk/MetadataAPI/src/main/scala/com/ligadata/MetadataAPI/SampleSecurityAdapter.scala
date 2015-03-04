package com.ligadata.MetadataAPI

import com.ligadata.olep.metadata.SecurityAdapter
import java.util.Properties

class SampleSecurityAdapter extends SecurityAdapter{
  
  override def performAuth (secParms: java.util.Properties): Boolean = {
    println("Nothing implemented... so proceed")
    return true  
  }
  
}
