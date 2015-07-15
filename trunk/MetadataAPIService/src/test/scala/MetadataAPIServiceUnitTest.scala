/*
import java.io.File

import dispatch._, Defaults._

import scala.Boolean

/*
*
 * Unit spec
 *
 * Created by dhaval kolapkar.
 *
 * CRUD operations on message(done), container(done), model(done), function, type, concept, or configuration.
 */
class MetadataAPIServiceUnitTest extends MetadataAPIServiceUnitSpec {

  def insertModel: String={
    var request = host / "api" / "model"
    val uri = getClass getResource "/model/HelloWorld_PMML_DELETE.xml" toURI
    val body: java.io.File = new File(uri)
    whenReady(http(request.POST <<< body OK dsl.asJson)) {
      json => {
       assume((json \ "APIResults" \ "statusCode").as[Int] === (0).asInstanceOf[Int], "Inserting model failed!")
        return (json \ "APIResults" \ "status/error Description").as[String].split(":")(1).toString
      }
    }
  }

  def insertMessage: String ={
    val request = host / "api" / "message"
    val uri = getClass getResource "/message/HelloWorld_Msg_Def_Delete.json" toURI
    val body: java.io.File = new File(uri)
    whenReady(http(request.POST <<< body OK dsl.asJson)) {
      json =>{
        assume((json \ "APIResults" \ "statusCode").as[Int] === (0).asInstanceOf[Int], "Inserting message failed!")
        return (json \ "APIResults" \ "status/error Description").as[String].split(":")(1).toString
      }
    }
  }

  def insertContainer: String={
    val request=host /"api"/"container"
    val uri=getClass getResource "/container/EnvCodesDelete.json" toURI
    val body: java.io.File = new File(uri)
    whenReady(http(request.POST <<< body OK dsl.asJson)) {
      json =>{
        assume((json \ "APIResults" \ "statusCode").as[Int] === (0).asInstanceOf[Int], "Inserting container failed!")
        return (json \ "APIResults" \ "status/error Description").as[String].split(":")(1).toString
      }
    }
  }

  def clearAllModels: Unit ={
    val request = host / "api" / "keys" / "model"
    whenReady(http(request.GET OK dsl.asJson)) {
      json =>{
        val resultData: Array[String]=(json \ "APIResults" \ "resultData").as[String].split(":")
        if (resultData.size==2){
          val modelIds=resultData(1).toString.split(",")
          for(modelId <- modelIds){
            val request = host / "api" / "model" / modelId
            whenReady(http(request.DELETE OK dsl.asJson)){
              json=> println("Deleted model with id: "+modelId)
            }
          }
        }
      }
    }
  }

  def clearAllMessages: Unit ={
    val request = host / "api" / "keys" / "message"
    whenReady(http(request.GET OK dsl.asJson)) {
      json =>{
        val resultData: Array[String]=(json \ "APIResults" \ "resultData").as[String].split(":")
        if (resultData.size==2){
          val messageIds=resultData(1).toString.split(",")
          for(messageId <- messageIds){
            val request = host / "api" / "message" / messageId
            whenReady(http(request.DELETE OK dsl.asJson)){
              json=> println("Deleted message with id: "+messageId)
            }
          }
        }
      }
    }
  }

  def clearAllContainers: Unit={
    val request = host / "api" / "keys" / "container"
    whenReady(http(request.GET OK dsl.asJson)) {
      json =>{
        val resultData: Array[String]=(json \ "APIResults" \ "resultData").as[String].split(":")
        if (resultData.size==2){
          val containerIds=resultData(1).toString.split(",")
          for(containerId <- containerIds){
            val request = host / "api" / "container" / containerId
            whenReady(http(request.DELETE OK dsl.asJson)){
              json=> println("Deleted message with id: "+containerId)
            }
          }
        }
      }
    }
  }

  override def beforeEach: Unit ={
    //delete all
    clearAllMessages
    clearAllModels
    clearAllContainers
  }

  override def afterEach: Unit ={
    //delete all
   clearAllModels
    clearAllMessages
    clearAllContainers
  }

  describe("A metadata API service for CRUD operations on messages") {

    it("should upload a new message object to the metadata server (C)") {
      val request = host / "api" / "message"
      val uri = getClass getResource "/message/HelloWorld_Msg_Def.json" toURI
      val body: java.io.File = new File(uri)
      whenReady(http(request.POST <<< body OK dsl.asJson)) {
        json => (json \ "APIResults" \ "status/error Description").as[String] should include regex ("Message Added Successfully*")
      }
    }

    it("should get all the messages in a metadata server (R)") {
      val request = host / "api" / "keys" / "message"
      whenReady(http(request.GET OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "status/error Description").as[String] should be("Successfully fetched all object keys")
      }
    }

    it("should retrieve a message from the metadata server (R)") {
      val msgId=insertMessage
      val request = host / "api" / "message" / msgId
      whenReady(http(request.GET OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
    }

    it("should delete a message from the metadata server (D)") {
      val msgId=insertMessage
      val request = host / "api" / "message" / msgId
      whenReady(http(request.DELETE OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }

    }
  }

  describe("A metadata API service for CRUD operations on container") {

    it("should upload a new container object to the metadata server(C)"){
      val request=host /"api"/"container"
      val uri=getClass getResource "/container/EnvCodes.json" toURI
      val body: java.io.File = new File(uri)
      whenReady(http(request.POST <<< body OK dsl.asJson)) {
        json => (json \ "APIResults" \ "status/error Description").as[String] should include regex ("Container Added Successfully*")
      }
    }

    it("should get all the containers in a metadata server(R)") {
      val request = host / "api" / "keys" / "container"
      whenReady(http(request.GET OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "status/error Description").as[String] should be("Successfully fetched all object keys")
      }
    }

    it("should retrieve a container from the metadata server(R)"){
      val containerId=insertContainer
      val request = host / "api" / "keys" / "container"/containerId
      whenReady(http(request.GET OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
    }

    it("should update an existing container object to the metadata server(U)"){
      //TODO
      val containerId=insertContainer
      val request = host / "api" / "container" / containerId
      val uri=getClass getResource "/container/EnvCodesPut.json" toURI
      val body: java.io.File = new File(uri)
      whenReady(http(request.PUT <<< body OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
    }

    it("should delete a container from the metadata server"){
      val containerId=insertContainer
      val request = host / "api" / "container"/containerId
      whenReady(http(request.DELETE OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
    }
  }

  describe("A metadata API service for CRUD operations on model"){

    it("should upload a new model object to the metadata server (C)"){
      val request = host / "api"/"model"
      val uri=getClass getResource "/model/HelloWorld_PMML.xml" toURI
      val body: java.io.File = new File(uri)
      whenReady(http(request.POST <<< body OK dsl.asJson)) {
        json => (json \ "APIResults" \ "status/error Description").as[String] should include regex ("Model Added Successfully*")
      }
    }

    it("should see all the models in a metadata server (R)"){
      val request = host / "api" / "keys" / "model"
      whenReady(http(request.GET OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "status/error Description").as[String] should be("Successfully fetched all object keys")
      }
    }

    it("should retrieve a model from the metadata server (R)"){
      val modelId=insertModel
      val request = host / "api" / "keys" / "model"/modelId
      whenReady(http(request.GET OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
    }

    it("should deactivate an existing model object in the metadata server (U)"){
      val modelId=insertModel
      var request=host /"api"/"deactivate"/"model"/modelId
      whenReady(http(request.PUT OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
      //need to activate this model before delete
      request=host /"api"/"activate"/"model"/modelId
      whenReady(http(request.PUT OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "Status Code").as[Int] should equal(0)
      }
    }

    it("should activate an existing model object in the metadata server(U)"){
      val modelId=insertModel
      //deactivate the model first
      var request=host /"api"/"deactivate"/"model"/modelId
      whenReady(http(request.PUT OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
      request=host /"api"/"activate"/"model"/modelId
      whenReady(http(request.PUT OK dsl.asJson)) {
        json =>
          (json \ "APIResults" \ "statusCode").as[Int] should equal(0)
      }
    }

    it("should delete a model from the metadata server (D)"){
      val modelId=insertModel
       val request = host / "api" / "model" / modelId
        whenReady(http(request.DELETE OK dsl.asJson)) {
          json =>
            (json \ "APIResults" \ "status/error Description").as[String] should include regex ("Deleted Model Successfully*")
        }
    }
  }
}*/
