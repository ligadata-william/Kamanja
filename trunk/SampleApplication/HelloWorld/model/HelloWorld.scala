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

package com.ligadata.samples.models

import com.ligadata.KamanjaBase._
import com.ligadata.KamanjaBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResultBase, TransactionContext, ModelContext }
import com.ligadata.KamanjaBase.{ BaseMsg, BaseContainer, RddUtils, RddDate, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import System._


object HelloWorldModel extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[msg1]
  override def CreateNewModel(mdlCtxt: ModelContext): ModelBase = return new HelloWorldModel(mdlCtxt)
  override def ModelName: String = "HelloWorldModel" 
  override def Version: String = "0.0.1"
  override def CreateResultObject(): ModelResultBase = new MappedModelResults()
}

class HelloWorldModel(mdlCtxt : ModelContext) extends ModelBase(mdlCtxt, HelloWorldModel) {
  
   override def execute(emitAllResults:Boolean):ModelResultBase = {
     
     var helloWorld : msg1 =  mdlCtxt.msg.asInstanceOf[msg1]
     
        if(helloWorld.score!=1)
          return null;
     
     var actualResults: Array[Result] = Array[Result](new Result("Id",helloWorld.id),
                                                        new Result("Name",helloWorld.Name),
                                                        new Result("Score",helloWorld.score))
     return HelloWorldModel.CreateResultObject().asInstanceOf[MappedModelResults].withResults(actualResults)
   }
   
  
}