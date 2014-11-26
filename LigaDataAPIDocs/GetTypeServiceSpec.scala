package com.ligadata.metadataapiservice

import scala.concurrent.future
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest

class GetTypeServiceSpec extends FreeSpec with MetadataAPIService with ScalatestRouteTest with Matchers {
  def actorRefFactory = system

  "The GetType Service" - {
    "when calling GET api/GetType/system/" - {
      "should return a string value" in {
        Get("/api/GetType/arrayoflong/Long") ~> metadataAPIRoute ~> check {
          status should equal(OK)
          entity.toString should include("system")
        }
      }
    }
  }
}
