// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.serialization

import io.circe.Json

object JsonWriter extends Writer[Json] {
  override protected def writeImpl(value: Json): Array[Byte] = value.toString().getBytes("UTF-8")
}
