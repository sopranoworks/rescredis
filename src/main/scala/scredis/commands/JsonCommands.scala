// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.commands

import io.circe._
import io.circe.syntax._
import scredis.exceptions.RedisProtocolException
import scredis.io.NonBlockingConnection
import scredis.protocol.requests.JsonRequests._
import scredis.serialization.{JsonImplicits, Writer}

import scala.concurrent.{ExecutionContext, Future}

trait JsonCommands { self: NonBlockingConnection =>

  /** The integers of a JSON.NUMINCRBY reply: an array for a JSONPath path, one number for a legacy path. */
  private def integers(reply: _root_.io.circe.Json): List[Option[Long]] = {
    def one(value: _root_.io.circe.Json): Option[Long] =
      if (value.isNull) None
      else value.asNumber.flatMap(_.toLong) match {
        case some @ Some(_) => some
        case None => throw RedisProtocolException(s"The new value is not an integer: $value")
      }
    reply.asArray match {
      case Some(values) => values.toList.map(one)
      case None => List(one(reply))
    }
  }

  object Json {
    /**
     * Set the JSON value at path in key
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. For new Redis keys the path must be the root. For existing keys, when the entire path exists, the value that it contains is replaced with the json value. For existing keys, when the path exists, except for the last element, a new child is added with the json value.
     *             Adds a key (with its respective value) to a JSON Object (in a RedisJSON data type key) only if it is the last child in the path, or it is the parent of a new child being added in the path. Optional arguments NX and XX modify this behavior for both new RedisJSON data type keys as well as the JSON Object keys in them.
     * @param value is value to set at the specified path
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return A simple string reply: true if executed correctly
     */
    def setRaw[W](key:String, path:String = "$", value:W)(implicit writer:Writer[W]):Future[Boolean] =
      send(JsonSet(key, path, value, None))

    /**
     * Set the JSON value at path in key
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. For new Redis keys the path must be the root. For existing keys, when the entire path exists, the value that it contains is replaced with the json value. For existing keys, when the path exists, except for the last element, a new child is added with the json value.
     *             Adds a key (with its respective value) to a JSON Object (in a RedisJSON data type key) only if it is the last child in the path, or it is the parent of a new child being added in the path. Optional arguments NX and XX modify this behavior for both new RedisJSON data type keys as well as the JSON Object keys in them.
     * @param value is value to set at the specified path
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return A simple string reply: true if executed correctly
     */
    def set[W](key:String, path:String = "$", value:W)(implicit encoder: Encoder[W]):Future[Boolean] =
      send(JsonSet(key, path, value.asJson, None)(JsonImplicits.jsonWriter))

    /**
     * Set the JSON value at path in key
     * 
     * sets the key only if it does not already exist.
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. For new Redis keys the path must be the root. For existing keys, when the entire path exists, the value that it contains is replaced with the json value. For existing keys, when the path exists, except for the last element, a new child is added with the json value.
     *             Adds a key (with its respective value) to a JSON Object (in a RedisJSON data type key) only if it is the last child in the path, or it is the parent of a new child being added in the path. Optional arguments NX and XX modify this behavior for both new RedisJSON data type keys as well as the JSON Object keys in them.
     * @param value is value to set at the specified path
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return A simple string reply: true if executed correctly
     */
    def setRawNX[W](key:String, path:String = "$", value:W)(implicit writer:Writer[W]):Future[Boolean] =
      send(JsonSet(key, path, value, Some("NX")))

    /**
     * Set the JSON value at path in key
     *
     * sets the key only if it does not already exist.
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. For new Redis keys the path must be the root. For existing keys, when the entire path exists, the value that it contains is replaced with the json value. For existing keys, when the path exists, except for the last element, a new child is added with the json value.
     *             Adds a key (with its respective value) to a JSON Object (in a RedisJSON data type key) only if it is the last child in the path, or it is the parent of a new child being added in the path. Optional arguments NX and XX modify this behavior for both new RedisJSON data type keys as well as the JSON Object keys in them.
     * @param value is value to set at the specified path
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return A simple string reply: true if executed correctly
     */
    def setNX[W](key:String, path:String = "$", value:W)(implicit encoder: Encoder[W]):Future[Boolean] =
      send(JsonSet(key, path, value.asJson, Some("NX"))(JsonImplicits.jsonWriter))

    /**
     * Set the JSON value at path in key
     *
     * sets the key only if it already exists.
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. For new Redis keys the path must be the root. For existing keys, when the entire path exists, the value that it contains is replaced with the json value. For existing keys, when the path exists, except for the last element, a new child is added with the json value.
     *             Adds a key (with its respective value) to a JSON Object (in a RedisJSON data type key) only if it is the last child in the path, or it is the parent of a new child being added in the path. Optional arguments NX and XX modify this behavior for both new RedisJSON data type keys as well as the JSON Object keys in them.
     * @param value is value to set at the specified path
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return A simple string reply: true if executed correctly
     */
    def setRawXX[W](key:String, path:String = "$", value:W)(implicit writer:Writer[W]):Future[Boolean] =
      send(JsonSet(key, path, value, Some("XX")))

    /**
     * Set the JSON value at path in key
     *
     * sets the key only if it already exists.
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. For new Redis keys the path must be the root. For existing keys, when the entire path exists, the value that it contains is replaced with the json value. For existing keys, when the path exists, except for the last element, a new child is added with the json value.
     *             Adds a key (with its respective value) to a JSON Object (in a RedisJSON data type key) only if it is the last child in the path, or it is the parent of a new child being added in the path. Optional arguments NX and XX modify this behavior for both new RedisJSON data type keys as well as the JSON Object keys in them.
     * @param value is value to set at the specified path
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return A simple string reply: true if executed correctly
     */
    def setXX[W](key:String, path:String = "$", value:W)(implicit encoder: Encoder[W]):Future[Boolean] =
      send(JsonSet(key, path, value.asJson, Some("XX"))(JsonImplicits.jsonWriter))

    /**
     * Return the value at path in JSON serialized form
     *
     * @param key is key to parse.
     * @param paths is JSONPath to specify. Default is root $. JSON.GET accepts multiple path arguments.
     * @return a JSON array. Each value is each JSON value that matches a path. Using multiple paths, JSON.GET returns a JSON object. Each value is an array of each JSON value that matches a path
     */
    def getRaw(key:String, paths:String*):Future[Option[Json]] =
      send(JsonGet(key, paths:_*))

    /**
     * Return the value at path in JSON serialized form
     * 
     * @param key is key to parse.
     * @param paths is JSONPath to specify. Default is root $. JSON.GET accepts multiple path arguments.
     * @param decoder is json decoder for R
     * @param ec is execution context
     * @tparam R is result value type
     * @return a JSON array. Each value is each JSON value that matches a path. Using multiple paths, JSON.GET returns a JSON object. Each value is an array of each JSON value that matches a path
     */
    def get[R](key:String, paths:String*)(implicit decoder: Decoder[R], ec:ExecutionContext):Future[Option[R]] =
      send(JsonGet(key, paths:_*)).map (_.flatMap (_.as[R].toOption))

    /**
     * Merge a given JSON value into matching paths. Consequently, JSON values at matching paths are updated, deleted, or expanded with new children.
     *
     * @param key is the key to merge into.
     * @param path specifies the JSONPath. For non-existing keys the path must be $. For existing keys, for each matched path, the value that matches the path is merged with the JSON value. For existing keys, when the path exists, except for the last element, a new child is added with the JSON value
     * @param value is the JSON value to merge with at the specified path. Merging is done according to the following rules per JSON value in the value argument while considering the corresponding original value if it exists:
     *
     *              merging an existing object key with a null value deletes the key
     *              merging an existing object key with non-null value updates the value
     *              merging a non-existing object key adds the key and value
     *              merging an existing array with any merged value, replaces the entire array with the value
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return a simple string reply: true if executed correctly or error if fails to set the new values
     */
    def mergeRaw[W](key:String, path:String, value:W)(implicit writer: Writer[W]):Future[Boolean] =
      send(JsonMerge(key, path, value))

    /**
     * Merge a given JSON value into matching paths. Consequently, JSON values at matching paths are updated, deleted, or expanded with new children.
     * 
     * @param key is the key to merge into.
     * @param path specifies the JSONPath. For non-existing keys the path must be $. For existing keys, for each matched path, the value that matches the path is merged with the JSON value. For existing keys, when the path exists, except for the last element, a new child is added with the JSON value
     * @param value is the JSON value to merge with at the specified path. Merging is done according to the following rules per JSON value in the value argument while considering the corresponding original value if it exists:
     *
     *              merging an existing object key with a null value deletes the key
     *              merging an existing object key with non-null value updates the value
     *              merging a non-existing object key adds the key and value
     *              merging an existing array with any merged value, replaces the entire array with the value
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return a simple string reply: true if executed correctly or error if fails to set the new values
     */
    def merge[W](key:String, path:String, value:W)(implicit encoder: Encoder[W]):Future[Boolean] =
      send(JsonMerge(key, path, value.asJson)(JsonImplicits.jsonWriter))

    /**
     * Delete a value
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $. Nonexisting paths are ignored.
     * @return an integer reply specified as the number of paths deleted (0 or more). 
     */
    def del(key:String, path:String = "$"):Future[Long] =
      send(JsonDel(key, path))

    /**
     * Toggle a Boolean value stored at path
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @return an array of integer replies for each path, the new value (0 if false or 1 if true), or nil for JSON values matching the path that are not Boolean.
     */
    def toggle(key:String, path:String = "$"):Future[List[Option[Long]]] =
      send(JsonToggle(key, path))

    /**
     * Clear container values (arrays/objects) and set numeric values to 0
     * 
     * @param key is key to parse.
     * @param path is JSONPath to specify. Default is root $. Nonexisting paths are ignored.
     * @return an integer reply specifying the number of matching JSON arrays and objects cleared + number of matching JSON numerical values zeroed. 
     */
    def clear(key:String, path:String = "$"):Future[Long] =
      send(JsonClear(key, path))

    /**
     * Report the type of JSON value at path
     * 
     * @param key is key to parse.
     * @param path is JSONPath to specify. Default is root $. Returns null if the key or path do not exist.
     * @return an array of string replies for each path, specified as the value's type. For more information about replies,
     */
    def types(key:String, path:String = "$"):Future[List[Option[String]]] =
      send(JsonType(key, path))

    /**
     * Report the length of the JSON String at path in key
     * 
     * @param key is key to parse.
     * @param path is JSONPath to specify. Default is root $, if not provided. Returns None if the key or path do not exist.
     * @return by recursive descent an array of integer replies for each path, the string's length, or None, if the matching JSON value is not a string.
     */
    def strLen(key:String, path:String = "$"):Future[List[Option[Long]]] =
      send(JsonStrLen(key, path))

    /**
     * Append the json-string values to the string at path
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param value is value to append to one or more strings. It is appended as it is: quotes, backslashes and control characters in it are escaped.
     * @return an array of integer replies for each path, the string's new length, or NOne, if the matching JSON value is not a string.
     */
    def strAppend(key:String, path:String = "$", value:Any):Future[List[Option[Long]]] =
      send(JsonStrAppend(key, path, _root_.io.circe.Json.fromString(value.toString).noSpaces))

    /**
     * Increment the number value stored at path by number
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify.
     * @param value is number value to increment.
     * @param writer is value type parameter writer
     * @param ec is execution context
     * @tparam W is source value type
     * @return new value for each path, or None, if the matching JSON value is not a number. The result is a list for a JSONPath path (`$...`) and a list of one value for a legacy path (`.field`). A new value that is not an integer fails the future with a [[scredis.exceptions.RedisProtocolException]] (the increment has been applied by the server).
     */
    def numIncBy[W](key:String, path:String = "$", value:W)(implicit writer: Writer[W], ec:ExecutionContext):Future[List[Option[Long]]] =
      send(JsonNumIncBy(key, path, value)).map(_.map(integers).getOrElse(Nil))

    // As of JSON version 2.0, this command is regarded as deprecated.
//    def numMulBy[W](key:String, path:String = "$", value:W)(implicit writer: Writer[W], ec:ExecutionContext):Future[List[Option[Long]]] =
//      send(JsonNumMulBy(key, path, value)).map(_.flatMap(_.as[List[Option[Long]]].toOption).getOrElse(Nil))


    /**
     * Set or update one or more JSON values according to the specified key-path-value triplets
     *
     * JSON.MSET is atomic, hence, all given additions or updates are either applied or not. It is not possible for clients to see that some of the keys were updated while others are unchanged.
     *
     * A JSON value is a hierarchical structure. If you change a value in a specific path - nested values are affected.
     *
     * @param keyPathValue is the specified key-path-value triplets
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return a simple string reply: true if executed correctly or error if fails to set the new values
     */
    def mSetRaw[W](keyPathValue:(String, String, W)*)(implicit writer: Writer[W]):Future[Boolean] =
      send(JsonMSet(keyPathValue:_*))

    /**
     * Set or update one or more JSON values according to the specified key-path-value triplets
     *
     * JSON.MSET is atomic, hence, all given additions or updates are either applied or not. It is not possible for clients to see that some of the keys were updated while others are unchanged.
     *
     * A JSON value is a hierarchical structure. If you change a value in a specific path - nested values are affected.
     * 
     * @param keyPathValue is the specified key-path-value triplets
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return a simple string reply: true if executed correctly or error if fails to set the new values
     */
    def mSet[W](keyPathValue:(String, String, W)*)(implicit encoder: Encoder[W]):Future[Boolean] =
      send(JsonMSet(keyPathValue.map(kpv => (kpv._1, kpv._2, kpv._3.asJson)):_*)(JsonImplicits.jsonWriter))

    /**
     * Return the values at path from multiple key arguments
     *
     * @param keys is key to parse. Returns null for nonexistent keys.
     * @param path is JSONPath to specify. Returns null for nonexistent paths.
     * @return an array of the value at each key's path.
     */
    def mGetRaw(keys:Seq[String], path:String ="$"):Future[List[Option[Json]]] =
      send(JsonMGet(keys, path))

    /**
     * Return the values at path from multiple key arguments
     * 
     * @param keys is key to parse. Returns null for nonexistent keys.
     * @param path is JSONPath to specify. Returns null for nonexistent paths.
     * @param decoder is json decoder for R
     * @param ec is execution context
     * @tparam R is result value type
     * @return an array of the value at each key's path. 
     */
    def mGet[R](keys:Seq[String], path:String ="$")(implicit decoder: Decoder[R], ec:ExecutionContext):Future[List[Option[List[Option[R]]]]] =
      mGetRaw(keys, path).map(_.map(_.flatMap(_.as[List[Option[R]]].toOption)))


    /**
     * Report the number of keys in the JSON object at path in key
     * 
     * @param key is key to parse. Returns None for nonexistent keys.
     * @param path is JSONPath to specify. Default is root $. Returns None for nonexistant path.
     * @return an array of integer replies for each path specified as the number of keys in the object or None, if the matching JSON value is not an object.
     */
    def objLen(key:String, path:String = "$"):Future[List[Option[Long]]] =
      send(JsonObjLen(key, path))

    /**
     * Return the keys in the object that's referenced by path
     * 
     * @param key is key to parse. Returns null for nonexistent keys.
     * @param path is JSONPath to specify. Default is root $. Returns None for nonexistant path.
     * @return an array of array replies for each path, an array of the key names in the object as a bulk string reply, or None if the matching JSON value is not an object.
     */
    def objKeys(key:String, path:String = "$"):Future[List[Option[List[String]]]] =
      send(JsonObjKeys(key, path))


    /**
     * Append the json values into the array at path after the last element in it
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param values is one or more values to append to one or more arrays.
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return an array of integer replies for each path, the array's new size, or None, if the matching JSON value is not an array.
     */
    def arrAppendRaw[W](key:String, path:String, values:W*)(implicit writer: Writer[W]):Future[List[Option[Long]]] =
      send(JsonArrAppend(key, path, values:_*))

    /**
     * Append the json values into the array at path after the last element in it
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param values is one or more values to append to one or more arrays.
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return an array of integer replies for each path, the array's new size, or None, if the matching JSON value is not an array.
     */
    def arrAppend[W](key:String, path:String, values:W*)(implicit encoder: Encoder[W]):Future[List[Option[Long]]] =
      send(JsonArrAppend(key, path, values.map(_.asJson):_*)(JsonImplicits.jsonWriter))

    /**
     *
     * Search for the first occurrence of a JSON value in an array
     *
     * @param key is key to parse.
     * @param path is JSONPath to specify.
     * @param value is value to find its index in one or more arrays.
     * @param start is inclusive start value to specify in a slice of the array to search. Default is 0.
     * @param stop is exclusive stop value to specify in a slice of the array to search, including the last element. Default is 0. Negative values are interpreted as starting from the end.
     * @param writer is value type parameter writer
     * @tparam W is value type
     * @return an array of integer replies for each path, the first position in the array of each JSON value that matches the path, -1 if unfound in the array, or nil, if the matching JSON value is not an array.
     */
    def arrIndexRaw[W](key:String, path:String, value:W, start:Int = 0, stop:Int = 0)(implicit writer: Writer[W]):Future[List[Option[Long]]] =
      send(JsonArrIndex(key, path, value, start, stop))

    /**
     *
     * Search for the first occurrence of a JSON value in an array
     *
     * @param key is key to parse.
     * @param path is JSONPath to specify.
     * @param value is value to find its index in one or more arrays.
     * @param start is inclusive start value to specify in a slice of the array to search. Default is 0.
     * @param stop is exclusive stop value to specify in a slice of the array to search, including the last element. Default is 0. Negative values are interpreted as starting from the end.
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return an array of integer replies for each path, the first position in the array of each JSON value that matches the path, -1 if unfound in the array, or nil, if the matching JSON value is not an array.
     */
    def arrIndex[W](key:String, path:String, value:W, start:Int = 0, stop:Int = 0)(implicit encoder: Encoder[W]):Future[List[Option[Long]]] =
      arrIndexRaw(key, path, value.asJson, start, stop)(JsonImplicits.jsonWriter)

    /**
     * Insert the json values into the array at path before the index (shifts to the right)
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param index is position in the array where you want to insert a value. The index must be in the array's range. Inserting at index 0 prepends to the array. Negative index values start from the end of the array.
     * @param values is one or more values to insert in one or more arrays.
     * @param writer is value type parameter writer
     * @tparam W is source value type
     * @return an array of integer replies for each path, the array's new size, or None, if the matching JSON value is not an array.
     */
    def arrInsertRaw[W](key:String, path:String, index:Int, values:W*)(implicit writer: Writer[W]):Future[List[Option[Long]]] =
      send(JsonArrInsert(key, path, index, values:_*))

    /**
     * Insert the json values into the array at path before the index (shifts to the right)
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param index is position in the array where you want to insert a value. The index must be in the array's range. Inserting at index 0 prepends to the array. Negative index values start from the end of the array.
     * @param values is one or more values to insert in one or more arrays.
     * @param encoder is json encoder for W
     * @tparam W is source value type
     * @return an array of integer replies for each path, the array's new size, or None, if the matching JSON value is not an array.
     */
    def arrInsert[W](key:String, path:String, index:Int, values:W*)(implicit encoder: Encoder[W]):Future[List[Option[Long]]] =
      send(JsonArrInsert(key, path, index, values.map(_.asJson):_*)(JsonImplicits.jsonWriter))

    /**
     * Report the length of the JSON array at path in key
     * 
     * @param key is key to parse.
     * @param path is JSONPath to specify. Default is root $, if not provided. Returns null if the key or path do not exist.
     * @return an array of integer replies, an integer for each matching value, each is the array's length, or None, if the matching value is not an array.
     */
    def arrLen(key:String, path:String = "$"):Future[List[Option[Long]]] =
      send(JsonArrLen(key, path))

    /**
     * Remove and return an element from the index in the array
     *
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param index is position in the array to start popping from. Default is -1, meaning the last element. Out-of-range indexes round to their respective array ends. Popping an empty array returns null.
     * @return an array of JSON value, or None, if the matching JSON value is not an array.
     */
    def arrPopRaw(key:String, path:String = "$", index:Int = -1):Future[List[Option[Json]]] =
      send(JsonArrPop(key, path, index))

    /**
     * Remove and return an element from the index in the array
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param index is position in the array to start popping from. Default is -1, meaning the last element. Out-of-range indexes round to their respective array ends. Popping an empty array returns null.
     * @param decoder is json decoder for R
     * @param ec is execution context
     * @tparam R is result value type
     * @return an array of JSON value, or None, if the matching JSON value is not an array.
     */
    def arrPop[R](key:String, path:String = "$", index:Int = -1)(implicit decoder: Decoder[R], ec:ExecutionContext):Future[List[Option[R]]] =
      arrPopRaw(key, path, index).map(_.map(_.flatMap(_.as[R].toOption)))

    /**
     * Trim an array so that it contains only the specified inclusive range of elements
     * 
     * @param key is key to modify.
     * @param path is JSONPath to specify. Default is root $.
     * @param start is index of the first element to keep (previous elements are trimmed). Default is 0.
     * @param stop is the index of the last element to keep (following elements are trimmed), including the last element. Default is 0. Negative values are interpreted as starting from the end.
     * @return an array of integer replies for each path, the array's new size, or None, if the matching JSON value is not an array.
     */
    def arrTrim(key:String, path:String = "$", start:Int = 0, stop:Int = 0):Future[List[Option[Long]]] =
      send(JsonArrTrim(key, path, start, stop))

    /**
     * Report a value's memory usage in bytes
     * 
     * @param key is key to parse.
     * @param path is JSONPath to specify. Default is root $.
     * @return an integer reply specified as the value size in bytes.
     */
    def debugMemory(key:String, path:String = "$"):Future[List[Long]] =
      send(JsonDebugMemory(key, path))
  }
}
