/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
