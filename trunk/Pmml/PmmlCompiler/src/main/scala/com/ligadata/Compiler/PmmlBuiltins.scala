package com.ligadata.Compiler

import scala.math._

object PmmlBuiltins {
	 
	def plus[T](a : T, b : T) (implicit n:Integral[T]) = n.plus(a,b)
	def minus[T](a : T, b : T) (implicit n:Integral[T]) = n.minus(a,b)
	def multiply[T](a : T, b : T)(implicit n:Integral[T]) = n.times(a, b) 
	def divide[T](a : T, b : T)(implicit n:Integral[T]) = n.quot(a, b) 
	def sqrt[T](a : Double) : Double = { sqrt(a) }
	def log10(a : Double) : Double = { log10(a) } 
	def ln(a : Double) : Double = { log1p(a) } 
	def abs(a : Double) : Double = { abs(a) } 
	def exp(a : Double) : Double = { exp(a)}
	def floor(a : Double) : Double = { floor(a)}
	def ceil(a : Double) : Double = { floor(a)}
	def round(a : Double) : Long = { round(a)}
	def pow(a : Double, b : Double) { pow(a,b) }
	def threshold(a : Double, b : Double) : Integer = { if (a > b) 1 else 0 }

	def avg(a : Array[Double]) : Double = { 
	val total : Double = a.sum
	total / a.length
	}
	def sum(a : Array[Double]) : Double = {
		a.sum
	}
	def max(a : Array[Double])	: Double = { a.max } 
	def min(a : Array[Double])	: Double = { a.min } 
	def product(a : Array[Double])	: Double = { a.reduceLeft(_ * _) } 
	def median(a : Array[Double]) : Double = { 
		val sorted = a.sortWith(_ < _)
		val mid:Integer = sorted.length / 2
		if (sorted.length % 2 == 0) {
			((a(mid) + a(mid+1)) / 2)
		} else {
			a(mid)
		}
	}
 
	/**  
	def isMissing(_))	 
	def isNotMissing(_))	 
	def equal(_,_))	 
	def notEqual(_,_))	 
	def lessThan(_,_))	 
	def lessOrEqual(_,_))	 
	def greaterThan(_,_))	 
	def greaterOrEqual(_,_))	 
	def isIn(List[Any]))	 
	def isNotIn(List[Any]))
	def and(List[Any]))
	def or(List[Any]))
	def not(_))	 
	def ifPred(List[Any]))
	def uppercase(_))	 
	def lowercase(_))	 
	def substring(_,_))	 
	def trimBlanks(_))
	def formatNumber(_,_))	 
	def formatDatetime(_,_))	 
	def dateDaysSinceYear(_,_))	 
	def dateSecondsSinceYear(_,_))
	def dateSecondsSinceMidnight(_))
	*/

}
