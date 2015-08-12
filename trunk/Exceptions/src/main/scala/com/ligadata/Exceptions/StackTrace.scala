package com.ligadata.Exceptions

import java.io.{ InputStream, FileInputStream, File, StringWriter, PrintWriter }

object StackTrace {
  
  def ThrowableTraceString(t: Throwable): String = {
    val sw = new StringWriter();
    val pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    return sw.toString();
  }
  
}