/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.messagedef

class RDDHandler {

  def HandleRDD(msgName: String) = {
    """
 
  type T = """ + msgName + """     
  override def build = new T
  override def build(from: T) = new T(from.transactionId, from)
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
 
    """
  }

  def javaMessageFactory(message: Message): String = {
    var createFunc:String = ""
    if(message.msgtype.toLowerCase().equals("message")){
      createFunc = """
	public static BaseMsgObj baseObj = (BaseMsgObj) """ + message.Name + """$.MODULE$;
           """
    }else  if(message.msgtype.toLowerCase().equals("container")){
      createFunc = """
	public static BaseContainerObj baseObj = (BaseContainerObj) """ + message.Name + """$.MODULE$;
        """
    }

    """
import com.ligadata.KamanjaBase.JavaRDDObject;
import com.ligadata.KamanjaBase.BaseMsgObj;
import com.ligadata.KamanjaBase.BaseContainerObj;

public final class """ + message.Name + """Factory {
	public static JavaRDDObject<""" + message.Name + """> rddObject = """ + message.Name + """$.MODULE$.toJavaRDDObject();
""" + createFunc + """
}

"""

  }

}