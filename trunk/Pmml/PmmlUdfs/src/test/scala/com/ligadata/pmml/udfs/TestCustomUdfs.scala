package com.ligadata.pmml.udfs
import org.scalatest.FlatSpec

class TestCustomUdfs extends FlatSpec {
   
  "Concat" should "return one String with no space" in {
    //input
    val input1:String ="textOne"
    val input2:String ="textTwo"
    //expected
    val expected:String="textOnetextTwo"
    //actual
    val actual:String=CustomUdfs.Concat(input1,input2)
    assert(expected === actual)
  }
   
    "It" should "concat Ints" in {
    //input
    val input1:Int =1
    val input2:Int=5
    //expected
    val expected:String="15"
    //actual
    val actual:String=CustomUdfs.Concat(input1,input2)
    assert(expected === actual)
  }
    
     "It" should "replace null values with empty string" in {
    //input
    val input1:String ="textOne"
    val input2:String =null
    //expected
    val expected:String="textOne"
    //actual
    val actual:String=CustomUdfs.Concat(input1,input2)
    assert(expected === actual)
  }
     
     "Matches" should "return true if chiled lies within the parent" in {
       
       //input
       val parent ="textOne"
       val chiled ="One"
       //actual
       val actual = CustomUdfs.matches(parent, chiled)
       assert(actual)
       
     }
     
      "it" should "return false if chiled doesn't lie within the parent" in {
       
       //input
       val parent ="textOne"
       val chiled ="textTwo"
       //actual
       val actual = CustomUdfs.matches(parent, chiled)
       assertResult(false)(actual)
       
     }
     
     "it" should "return false if one parameter is null" in {
       
       //input
       val input1 ="textOne"
       val input2 =null
       //expected
       val expected =false
       //actual
       val actual = CustomUdfs.matches(input1, input2)
       assertResult(false)(actual)
       
     }
     
     "replace" should " replace all instances of inWord  inside replacewithin with replacewith " in{
       //input
       val replacewithin ="textOne"
       val inWord ="One"
       val replacewith="Two"
       //expected
       val expected ="textTwo"
       //actual
       val actual = CustomUdfs.replace(replacewithin, inWord, replacewith)
       assert(expected===actual)
     }
     
      "it" should " return IllegalArgumentException if inWord parameter is null " in{
       //input
       val replacewithin ="textOne"
       val inWord = null
       val replacewith="Two"
       intercept[IllegalArgumentException]{
       val actual = CustomUdfs.replace(replacewithin, inWord, replacewith)
       }
     }
      
      "it" should " return IllegalArgumentException if replacewithin parameter is null " in{
       //input
       val replacewithin =null
       val inWord = "One"
       val replacewith="Two"
       intercept[IllegalArgumentException]{
       val actual = CustomUdfs.replace(replacewithin, inWord, replacewith)
       }
     }
      
      "formatNumber" should "format numbers as expected" in{
       //input
       val num =1.33333
       val formating="%.2f"
       //expected
       val expected ="1.33"
       //actual
       val actual = CustomUdfs.formatNumber(num, formating)
       assert(expected===actual)
     }
      
      
       "it" should "return IllegalArgumentException if num is not number" in{
       //input
       val num ="not number"
       val formatting ="%.2f"
       intercept[IllegalArgumentException]{
       val actual = CustomUdfs.formatNumber(num, formatting)
       }
     }
       
        "matchTermsetBoolean" should "ignore cases" in{
       //input
       val inputString ="ligadata company"
       val context= Array("Apple","Google","LIGADATA")
       val degree=1
       //actual
       val actual = CustomUdfs.matchTermsetBoolean(inputString, context, degree)
       assert(actual)
     }
        
        "matchTermsetBoolean" should "return false if inputString is null" in{
       //input
       val inputString =null
       val context= Array("Apple","Google","LIGADATA")
       val degree=1
       val expected= false
       //actual
       val actual = CustomUdfs.matchTermsetBoolean(inputString, context, degree)
       assertResult(expected)(actual)
     }
        "matchTermsetBoolean" should "return false if context is null" in{
       //input
       val inputString ="ligadata"
       val context= null
       val degree=1
       //expected
       val expected= false
       //actual
       val actual = CustomUdfs.matchTermsetBoolean(inputString, context, degree)
       assertResult(expected)(actual)
     }
        "getMatchingTokens" should "matched string delimited by ." in{
       //input
       val inputString ="ligadata company"
       val context= Array("Apple","Google","LIGADATA")
       //expected
       val expected= ".LIGADATA"
       //actual
       val actual = CustomUdfs.getMatchingTokens(inputString, context)
       assert(expected===actual)
     }
        "it" should "return false if inputString is null" in{
       //input
       val inputString ="ligadata"
       val context= null
       //expected
       val expected= ""
       //actual
       val actual = CustomUdfs.getMatchingTokens(inputString, context)
       assert(expected===actual)
     }
        "it" should "return false if context is null" in{
       //input
       val inputString =null
       val context= Array("Apple","Google","LIGADATA")
       //expected
       val expected= ""
       //actual
       val actual = CustomUdfs.getMatchingTokens(inputString, context)
       assert(expected===actual)
     }

}