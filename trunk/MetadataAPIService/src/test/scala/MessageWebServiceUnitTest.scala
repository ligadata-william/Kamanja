import scala.EmbeddedServices.EmbeddedMetadataService.EmbeddedMetadataService


/**
 * Created by dhaval on 7/13/15.
 */

class MessageWebServiceUnitTest extends MetadataAPIServiceUnitSpec {

  override def beforeEach: Unit ={
    println("Yo did it before!")
    val ems=EmbeddedMetadataService.instance
    ems.run()
   //Thread.sleep(5000)
    //assume(ems.isRunning,"Metadata Web service did not start!")
    //println("Started the web service!!!!"+ems.apiServiceThread.getState.toString)
  }

  describe("A metadata API service for CRUD operations on messages") {

    it("should upload a new message object to the metadata server (C)") {
      println("Yo did run the test!")
    }
  }
}
