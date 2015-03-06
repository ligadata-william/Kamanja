package com.ligadata.metadataapiservice

import org.apache.camel.util.jsse.{KeyStoreParameters, KeyManagersParameters, SSLContextParameters}
import javax.net.ssl.SSLContext
import com.ligadata.MetadataAPI.MetadataAPIImpl

// Must be enabled in the applicatin.conf
trait LigadataSSLConfiguration {

  // if there is no SSLContext in scope implicitly the HttpServer uses the default SSLContext,
  // since we want non-default settings in this example we make a custom SSLContext available here
  implicit def sslContext: SSLContext = {
    val keyStoreResource = MetadataAPIImpl.getSSLCertificatePath  
    
    val ksp = new KeyStoreParameters()
    ksp.setResource(keyStoreResource);
    ksp.setPassword("password")

    val kmp = new KeyManagersParameters()
    kmp.setKeyStore(ksp)
    kmp.setKeyPassword("password")

    val scp = new SSLContextParameters()
    scp.setKeyManagers(kmp)
    
    val context = scp.createSSLContext() 
    context
  }
}