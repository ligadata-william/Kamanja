package com.ligadata.loadtestcommon

import com.ligadata.loadtestcommon._

sealed trait StorageWorklet

// Basic operations
//
case class Add(Index : Int, nLength : Int) extends StorageWorklet
case class Put(Index : Int, nLength : Int) extends StorageWorklet	
case class Get(Index : Int) extends StorageWorklet
case class Del(Index : Int) extends StorageWorklet
case class Get_Put(Index : Int, nLength : Int) extends StorageWorklet // Update

// Higher order operations
//
case class S1_put(Index : Int, nLength : Int) extends StorageWorklet

// Mgmt 
case class ExecuteMaster(config: RemoteConfiguration) extends StorageWorklet
case class Execute(config: Configuration) extends StorageWorklet
case class Done(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long) extends StorageWorklet
case class SoFar(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long) extends StorageWorklet
case class PreparationDone(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long) extends StorageWorklet
case object Simulate extends StorageWorklet

case class Result() extends StorageWorklet // Something is done (really)
case class Result_W(nLength: Int, nStart: Long,  nEnd: Long ) extends StorageWorklet // A write is done
case class Result_R(nLength: Int, nStart: Long,  nEnd: Long ) extends StorageWorklet // A read is done
case class Result_D(nLength: Int, nStart: Long,  nEnd: Long ) extends StorageWorklet // A read is done
