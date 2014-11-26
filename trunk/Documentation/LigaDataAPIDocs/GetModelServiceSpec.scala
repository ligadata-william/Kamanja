package com.ligadata.metadataapiservice

import scala.concurrent.future
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest

class GetModelServiceSpec extends FreeSpec with MetadataAPIService with ScalatestRouteTest with Matchers {
  def actorRefFactory = system

  "The GetModel Service" - {
    "when calling GET api/GetModel/system/cignaalerts_000100/1" - {
      "should return a string value" in {
        Get("/api/GetModel/system/cignaalerts_000100/1") ~> metadataAPIRoute ~> check {
          status should equal(OK)
          entity.toString should include("system")
        }
      }
    }
  }
}
