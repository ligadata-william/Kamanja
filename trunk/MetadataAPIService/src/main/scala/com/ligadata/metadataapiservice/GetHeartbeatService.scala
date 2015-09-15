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

package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.kamanja.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object GetHeartbeatService {
  case class Process(nodeIds:String)
}

/**
 * @author danielkozin
 */
class GetHeartbeatService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor  {
  import GetHeartbeatService._ 
  import system.dispatcher
  
  implicit val system = context.system
  val log = Logging(system, getClass)
  val APIName = "GetHeartbeatService"
  
  def receive = {
    case Process(nodeId) =>
      process(nodeId)
      context.stop(self)
  }
  
  def process(nodeIds:String): Unit = {
    // NodeIds is a JSON array of nodeIds.
    if (nodeIds == null || (nodeIds != null && nodeIds.length == 0))
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Invalid BODY in a POST request.  Expecting either an array of nodeIds or an empty array for all").toString)  
  
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("get","heartbeat"))) {
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:Checking Heartbeat is not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.getHealthCheck(nodeIds)  
      requestContext.complete(apiResult)      
    }
  }  
  
}