package com.ligadata.messagedef

class RDDHandler {

  def HandleRDD(msgName: String) = {
    """
 
  type T = """ + msgName + """     
  override def build = new T
  override def build(from: T) = new T(from)
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
 
    """
  }

  def javaMessageFactory(message: Message): String = {
   
    var baseMsgFunc:String = ""
    if(message.msgtype.toLowerCase().equals("message")){
      baseMsgFunc = """
        public static BaseMsg CreateNewMessage() {
    		  return """ + message.Name + """$.MODULE$.CreateNewMessage();
		}
           """
    }else  if(message.msgtype.toLowerCase().equals("container")){
      baseMsgFunc = """
        public static BaseContainer CreateNewContainer() {
    		  return """ + message.Name + """$.MODULE$.CreateNewContainer();
    	}
        """
    }
      
     
    """
import com.google.common.base.Optional;
import com.ligadata.FatafatBase.BaseMsg;
import com.ligadata.FatafatBase.BaseContainer;
import com.ligadata.FatafatBase.InputData;
import com.ligadata.FatafatBase.JavaRDD;
import com.ligadata.FatafatBase.JavaRDDObject;
import com.ligadata.FatafatBase.RDDObject;
import com.google.common.base.Optional;
import com.ligadata.Utils.Utils;
import com.ligadata.FatafatBase.TimeRange;
import com.ligadata.FatafatBase.MessageContainerBase;
import com.ligadata.FatafatBase.api.java.function.Function1;
import com.ligadata.FatafatBase.FatafatUtils;
    
public final class """ + message.Name + """Factory {
	public static String FullName() {
		return """ + message.Name + """$.MODULE$.FullName();
	}

	public static String NameSpace() {
		return """ + message.Name + """$.MODULE$.NameSpace();
	}

	public static String Name() {
		return """ + message.Name + """$.MODULE$.Name();
	}
	
	public static String Version() {
		return """ + message.Name + """$.MODULE$.Version();
	}

	"""+  baseMsgFunc    + """
	
	public static boolean IsFixed() {
		return """ + message.Name + """$.MODULE$.IsFixed();
	}
	
	public static boolean IsKv() {
		return """ + message.Name + """$.MODULE$.IsKv();
	}
	
	public static boolean CanPersist() {
		return """ + message.Name + """$.MODULE$.CanPersist();
	}
	
	public static """ + message.Name + """ build() {
		return """ + message.Name + """$.MODULE$.build();
	}

	public static """ + message.Name + """ build(""" + message.Name + """ from) {
		return """ + message.Name + """$.MODULE$.build(from);
	}
	
	public static String[] PartitionKeyData(InputData inputdata) {
		return """ + message.Name + """$.MODULE$.PartitionKeyData(inputdata);
	}
	
	public static String[] PrimaryKeyData(InputData inputdata) {
		return """ + message.Name + """$.MODULE$.PrimaryKeyData(inputdata);
	}

	public static String getFullName() {
		return """ + message.Name + """$.MODULE$.getFullName();
	}
	
	public static JavaRDDObject<""" + message.Name + """> toJavaRDDObject() {
		return """ + message.Name + """$.MODULE$.toJavaRDDObject();
	}

	public static RDDObject<""" + message.Name + """> toRDDObject() {
		return """ + message.Name + """$.MODULE$.toRDDObject();
	}

	public static Optional<""" + message.Name + """> getRecent() {
		return  Utils.optionToOptional(""" + message.Name + """$.MODULE$.getRecent());
	}

	public static """ + message.Name + """ getRecentOrNew() {
		return """ + message.Name + """$.MODULE$.getRecentOrNew();
	}

	public static Optional<""" + message.Name + """> getRecent(String[] key) {
		return Utils.optionToOptional(""" + message.Name + """$.MODULE$.getRecent(key));
	}

	public static """ + message.Name + """ getRecentOrNew(String[] key) {
		return """ + message.Name + """$.MODULE$.getRecentOrNew(key);
	}
	
/*
	public static Optional<""" + message.Name + """> getOne(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return Utils.optionToOptional(""" + message.Name + """$.MODULE$.getOne(tmRange, FatafatUtils.toScalaFunction1(f)));
	}

	public static """ + message.Name + """ getOneOrNew(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getOneOrNew(tmRange, FatafatUtils.toScalaFunction1(f));
	}

	public static Optional<""" + message.Name + """> getOne(String[] key, TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return Utils.optionToOptional(""" + message.Name + """$.MODULE$.getOne(key, tmRange, FatafatUtils.toScalaFunction1(f)));
	}

	public static """ + message.Name + """ getOneOrNew(String[] key, TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getOneOrNew(key, tmRange, FatafatUtils.toScalaFunction1(f));
	}

	public static JavaRDD<""" + message.Name + """> getRDDForCurrKey(Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getRDDForCurrKey(FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}

	public static JavaRDD<""" + message.Name + """> getRDDForCurrKey(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getRDDForCurrKey(tmRange, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}
	
	public static JavaRDD<""" + message.Name + """> getRDD(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getRDD(tmRange, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}
	
*/
	public static JavaRDD<""" + message.Name + """> getRDD(TimeRange tmRange) {
		return """ + message.Name + """$.MODULE$.getRDD(tmRange).toJavaRDD();
	}
	
/*
	public static JavaRDD<""" + message.Name + """> getRDD(Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getRDD(FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}

	public static JavaRDD<""" + message.Name + """> getRDD(String[] key, TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getRDD(key, tmRange, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}
	
	public static JavaRDD<""" + message.Name + """> getRDD(String[] key, Function1<MessageContainerBase, Boolean> f) {
		return """ + message.Name + """$.MODULE$.getRDD(key, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}

*/

	public static JavaRDD<""" + message.Name + """> getRDD(String[] key, TimeRange tmRange) {
		return """ + message.Name + """$.MODULE$.getRDD(key, tmRange).toJavaRDD();
	}
	
	public static void saveOne(""" + message.Name + """ inst) {
		""" + message.Name + """$.MODULE$.saveOne(inst);
	}
	
	public static void saveOne(String[] key, """ + message.Name + """ inst) {
		""" + message.Name + """$.MODULE$.saveOne(key, inst);
	}
	
	public static void saveRDD(JavaRDD<""" + message.Name + """> data) {
		""" + message.Name + """$.MODULE$.saveRDD(data.rdd());
	}
}

"""

  }

}