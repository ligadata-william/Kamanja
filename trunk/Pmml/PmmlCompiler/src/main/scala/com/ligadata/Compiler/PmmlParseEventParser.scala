package com.ligadata.Compiler

import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler
import scala.collection._
import scala.collection.mutable.Stack
import org.apache.log4j.Logger

class PmmlParseEventParser(ctx : PmmlContext)  extends DefaultHandler with LogTrait {
	var start = false
	var node: PmmlNode = _
	val buffer:StringBuilder = new StringBuilder()
	
	override def startElement(namespaceURI: String, localName: String , qName:String , atts: Attributes ) = {
		buffer.setLength(0);

		if (ctx.pmmlElementVistitorMap.contains(qName)) {
			logger.debug(s"...encountered $qName")
			/** dispatch the element visitor that will build the appropriate kind of PmmlNode for it.and push to stack 
			 	The dispatcher pushes it on the stack to be decorated by its children. */
			ctx.dispatchElementVisitor(ctx, namespaceURI, localName , qName, atts)
			
			//if (qName == "SimpleRule") {
				//logger.debug("reached SimpleRule")
			//}
		} else {
			logger.debug(s"...element $qName is of no interest at all")
			//ctx.dispatchElementVisitor(ctx, namespaceURI, localName , qName, atts)
		}
	}

	override def endElement( uri: String, localName: String, qName: String) = {


  		var pmmlnode = if (ctx.pmmlNodeStack.isEmpty) null else ctx.pmmlNodeStack.top
  		
  		if (pmmlnode == null) {
  			logger.debug(s"endElement... pmmlNodeStack is empty ... qName = $qName")
  		} else {

			val buf : String = buffer.toString()
			val len = buf.length()
			if (buf.length() >= 0) {
				logger.debug(s"the following string was collected for $qName (length = $len) : $buf")
				
				pmmlnode.qName match {
				  case "Constant" => pmmlnode.asInstanceOf[PmmlConstant].Value(buf)
				  case "Array" => pmmlnode.asInstanceOf[PmmlArray].SetArrayValue(buf)
				  case _ => None
				}
				
			}
			
			if (ctx.topLevelContainers.contains(qName)) {
				ctx.pmmlNodeQueue.enqueue(pmmlnode)
			}
			
			ctx.pmmlNodeStack.pop
	  	}
		
		buffer.setLength(0);
	}

	override def characters(ch: Array[Char],start: Int , length: Int) = {
		buffer.append(new String(ch, start, length))
	}

}  