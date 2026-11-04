// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

import java.io.{BufferedInputStream, ByteArrayOutputStream, IOException}
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets.UTF_8

/**
 * A minimal RESP2 client on a plain socket, independent of the code under test: it administers the test
 * servers (CLUSTER NODES, DEBUG SEGFAULT, CLIENT LIST / KILL, INFO). One command at a time; a reply is a
 * String, a Long, a List of replies, null (nil), or an [[RawRedis.Error]].
 */
final class RawRedis(host: String, port: Int, timeoutMillis: Int = 5000) extends AutoCloseable {
  private val socket = new Socket()
  socket.connect(new InetSocketAddress(host, port), timeoutMillis)
  socket.setSoTimeout(timeoutMillis)
  private val in = new BufferedInputStream(socket.getInputStream)
  private val out = socket.getOutputStream

  def command(args: String*): Any = {
    val b = new ByteArrayOutputStream()
    b.write(s"*${args.size}\r\n".getBytes(UTF_8))
    args.foreach { a =>
      val bytes = a.getBytes(UTF_8)
      b.write(s"$$${bytes.length}\r\n".getBytes(UTF_8))
      b.write(bytes)
      b.write("\r\n".getBytes(UTF_8))
    }
    out.write(b.toByteArray)
    out.flush()
    read()
  }

  /** Sends a command whose reply never comes because the server dies (DEBUG SEGFAULT) */
  def commandWithoutReply(args: String*): Unit = {
    try command(args: _*) catch { case _: IOException => }
    ()
  }

  def string(args: String*): String = command(args: _*) match {
    case s: String => s
    case other => throw new IllegalStateException(s"${args.mkString(" ")}: $other")
  }

  private def line(): String = {
    val b = new ByteArrayOutputStream()
    var c = in.read()
    while (c != '\r') {
      if (c < 0) throw new IOException("connection closed")
      b.write(c)
      c = in.read()
    }
    in.read() // \n
    new String(b.toByteArray, UTF_8)
  }

  private def read(): Any = {
    val l = line()
    val rest = l.substring(1)
    l.charAt(0) match {
      case '+' => rest
      case '-' => RawRedis.Error(rest)
      case ':' => rest.toLong
      case '$' =>
        val n = rest.toInt
        if (n < 0) null
        else {
          val bytes = new Array[Byte](n)
          var read = 0
          while (read < n) {
            val r = in.read(bytes, read, n - read)
            if (r < 0) throw new IOException("connection closed")
            read += r
          }
          in.read(); in.read()
          new String(bytes, UTF_8)
        }
      case '*' =>
        val n = rest.toInt
        if (n < 0) null else List.fill(n)(read())
      case other => throw new IOException(s"unexpected reply type $other")
    }
  }

  def close(): Unit = socket.close()
}

object RawRedis {
  final case class Error(message: String)

  def withConnection[A](host: String, port: Int)(f: RawRedis => A): A = {
    val r = new RawRedis(host, port)
    try f(r) finally r.close()
  }

  /** One line of CLUSTER NODES */
  final case class Node(id: String, port: Int, flags: Set[String], masterId: String, linkState: String, slots: Seq[(Int, Int)]) {
    def isMaster: Boolean = flags.contains("master")
    def isFailed: Boolean = flags.contains("fail") || flags.contains("fail?")
    def owns(slot: Int): Boolean = slots.exists { case (a, b) => slot >= a && slot <= b }
  }

  def clusterNodes(port: Int): List[Node] = withConnection("127.0.0.1", port) { r =>
    r.string("CLUSTER", "NODES").split("\n").toList.filter(_.trim.nonEmpty).map { l =>
      val f = l.trim.split(" ")
      val slots = f.drop(8).filterNot(_.startsWith("[")).map { s =>
        s.split("-") match {
          case Array(a, b) => (a.toInt, b.toInt)
          case Array(a) => (a.toInt, a.toInt)
          case _ => (0, -1)
        }
      }.toSeq
      Node(f(0), f(1).split("@")(0).split(":").last.toInt, f(2).split(",").toSet, f(3), f(7), slots)
    }
  }
}
