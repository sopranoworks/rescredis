// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.serialization

import io.circe.Json

object JsonImplicits {
  import Implicits._

  implicit val jsonWriter:Writer[Json] = JsonWriter
}
