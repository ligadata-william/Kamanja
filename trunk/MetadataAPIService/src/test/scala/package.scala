import dispatch._
import play.api.libs.json.Json
import scala.xml.XML

/**
 * Dsl
 *
 * Created by dhaval kolapkar
 */
package object dsl {
  import collection.JavaConverters._

  val asString = as.Response {
    response => response.getResponseBody
  }

  val asJson = as.Response {
    response => Json.parse(response.getResponseBody)
  }

  val asXml = as.Response {
    response =>
      val factory = javax.xml.parsers.SAXParserFactory.newInstance()
      factory.setNamespaceAware(false)
      XML.withSAXParser(factory.newSAXParser).loadString(response.getResponseBody)
  }

  val asHeaders = as.Response {
    response => new (String => Seq[String]) {
      def apply(name: String) = {
        response.getHeaders(name).asScala
      }
    }
  }

  val asHeader = as.Response {
    response => new (String => String) {
      def apply(name: String) = {
        response.getHeader(name)
      }
    }
  }
}
