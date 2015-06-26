import dispatch._
import org.scalatest._
import org.scalatest.time._

/**
 * Unit test specification
 *
 * Created by dhaval kolapkar
 */
trait MetadataAPIServiceUnitSpec extends FunSpec with BeforeAndAfterEach with Matchers with concurrent.ScalaFutures {
  implicit val config = PatienceConfig(scaled(Span(90000, Millis)), scaled(Span(8000, Millis)))
  lazy val http = new Http
  val host = :/("localhost",8081).secure
  //val host = :/("demo1319324.mockable.io")
}
