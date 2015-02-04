import org.junit._
import org.junit.Assert._
import com.ligadata.keyvaluestore._

class BasicVoldemortUnitTest
{
	@Test
	def testVoldemortLocal() 
	{
		return
		
		val connectinfo = new PropertyMap

		connectinfo+= ("connectiontype" -> "voldemort")
		connectinfo+= ("hostname" -> "tcp://localhost:6666") 
		connectinfo+= ("schema" -> "default")
		connectinfo+= ("table" -> "default")
		
		val store = KeyValueManager.Get(connectinfo)
		
		object i extends IStorage
		{
			val key = new Key
			val value = new Value
			key +=(1,2,3,4)
			value +=(10, 11, 12, 13, 14, 15, 16,17, 18, 19, 20)

			def Key = key
			def Value = value
			def Construct(Key: Key, Value: Value) = {}
		}
		
		object o extends IStorage
		{
			var key = new com.ligadata.keyvaluestore.Key;
			var value = new com.ligadata.keyvaluestore.Value

			def Key = key
			def Value = value
			def Construct(k: com.ligadata.keyvaluestore.Key, v: com.ligadata.keyvaluestore.Value) = 
			{ 
				key = k; 
				value = v; 
			}
		}
		
		store.put(i)
		store.get(i.Key, o)
		store.del(i)
		
		assertEquals(i.Key, o.Key)
		assertEquals(i.Value, o.Value)

		store.Shutdown()
		  
		println("testVoldemortLocal - Done")
	}
}
