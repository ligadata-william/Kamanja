
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

package com.ligadata.KvBase

import java.util.Date

case class Key(timePartition: Date, bucketKey: Array[String], transactionId: Long, rowId: Int)
case class Value(serializerType: String, serializedInfo: Array[Byte])
case class TimeRange(beginTime: Date, endTime: Date)

object KvBaseDefalts {
  val defaultTime = new Date(0)
}

