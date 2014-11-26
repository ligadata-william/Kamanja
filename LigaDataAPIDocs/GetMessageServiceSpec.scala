package com.ligadata.metadataapiservice

import scala.concurrent.future
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest

class GetMessageServiceSpec extends FreeSpec with MetadataAPIService with ScalatestRouteTest with Matchers {
  def actorRefFactory = system

  "The GetMessage Service" - {
    "when calling GET api/GetMessage/system/cignaalerts_000100/1" - {
      "should return a string value" in {
        Get("/api/GetMessage/system/cignaalerts_000100/1") ~> metadataAPIRoute ~> check {
          status should equal(OK)
          entity.toString should include("system")
        }
      }
    }
  }
}
