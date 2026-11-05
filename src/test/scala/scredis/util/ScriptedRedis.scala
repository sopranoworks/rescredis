// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

import java.io.{BufferedInputStream, ByteArrayOutputStream, IOException, InputStream, OutputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

/**
 * A server that speaks enough RESP for one client and ends its connection right after a reply, so that the replies
 * the client receives last on a connection come just before that connection's end:
 *
 *   - `PING` → `+PONG`, `CLIENT …` → `+OK`, written at once
 *   - `GET batch:…` → the key as its value, held back; `GET end-fin:…` / `GET end-rst:…` → the same, then everything
 *     held back is written in one write and the connection is closed (FIN), or reset (RST) `resetDelayMs` later
 *   - `QUIT` → everything held back and `+OK` in one write, then FIN; or, by `quit`, FIN without `+OK` ("close"),
 *     RST without it ("reset"), or no answer and the connection left open ("silent")
 *   - `CLIENT SETNAME` is not answered when `answerSetName` is false (the client's connection stays initialising)
 *   - `SUBSCRIBE` / `UNSUBSCRIBE` / `PUNSUBSCRIBE` as Redis answers them; a channel `end-fin:…` is answered and the
 *     connection then closed (FIN), the first time only (the client subscribes to it again after reconnecting)
 */
final class ScriptedRedis(resetDelayMs: Int = 200, quit: String = "ok", answerSetName: Boolean = true) {

  private val server = new ServerSocket()
  server.setReuseAddress(true)
  server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
  val port: Int = server.getLocalPort

  private val acceptedCount = new AtomicInteger(0)
  private val ended = ConcurrentHashMap.newKeySet[String]()
  private val sockets = ConcurrentHashMap.newKeySet[Socket]()
  @volatile private var stopped = false

  /** The connections accepted so far */
  def accepted: Int = acceptedCount.get()

  /** Closes (FIN) every connection that is open */
  def closeAll(): Unit = sockets.forEach { socket =>
    try socket.close() catch { case _: IOException => }
  }

  def stop(): Unit = {
    stopped = true
    try server.close() catch { case _: IOException => }
    closeAll()
  }

  daemon("scripted-redis-accept") {
    try {
      while (!stopped) {
        val socket = server.accept()
        acceptedCount.incrementAndGet()
        socket.setTcpNoDelay(true)
        sockets.add(socket)
        daemon("scripted-redis-connection")(serve(socket))
      }
    } catch {
      case _: IOException =>
    }
  }

  private def daemon(name: String)(body: => Unit): Unit = {
    val t = new Thread(new Runnable { def run(): Unit = body }, name)
    t.setDaemon(true)
    t.start()
  }

  private def bulk(s: String): String = s"$$${s.getBytes(UTF_8).length}\r\n$s\r\n"

  private def serve(socket: Socket): Unit = {
    val in = new BufferedInputStream(socket.getInputStream)
    val out = socket.getOutputStream
    val held = new ByteArrayOutputStream()
    val channels = mutable.LinkedHashSet[String]()
    def hold(s: String): Unit = held.write(s.getBytes(UTF_8))
    def flush(o: OutputStream): Unit = {
      o.write(held.toByteArray)
      o.flush()
      held.reset()
    }
    try {
      var open = true
      while (open) {
        readCommand(in) match {
          case None => open = false
          case Some(command) =>
            command.head.toUpperCase match {
              case "PING" => hold("+PONG\r\n"); flush(out)
              case "CLIENT" => if (answerSetName) { hold("+OK\r\n"); flush(out) }
              case "GET" =>
                val key = command(1)
                hold(bulk(key))
                if (key.startsWith("end-fin:")) {
                  flush(out)
                  socket.close()
                  open = false
                } else if (key.startsWith("end-rst:")) {
                  flush(out)
                  Thread.sleep(resetDelayMs)
                  socket.setSoLinger(true, 0)
                  socket.close()
                  open = false
                } else if (!key.startsWith("batch:")) {
                  flush(out)
                }
              case "QUIT" => quit match {
                case "close" =>
                  socket.close()
                  open = false
                case "reset" =>
                  socket.setSoLinger(true, 0)
                  socket.close()
                  open = false
                case "silent" =>
                case _ =>
                  hold("+OK\r\n")
                  flush(out)
                  socket.close()
                  open = false
              }
              case "SUBSCRIBE" =>
                command.tail.foreach { channel =>
                  channels += channel
                  hold(s"*3\r\n${bulk("subscribe")}${bulk(channel)}:${channels.size}\r\n")
                }
                flush(out)
                if (command.tail.exists(c => c.startsWith("end-fin:") && ended.add(c))) {
                  socket.close()
                  open = false
                }
              case "UNSUBSCRIBE" =>
                val names = if (command.tail.isEmpty) channels.toList else command.tail.toList
                if (names.isEmpty) hold(s"*3\r\n${bulk("unsubscribe")}$$-1\r\n:0\r\n")
                names.foreach { channel =>
                  channels -= channel
                  hold(s"*3\r\n${bulk("unsubscribe")}${bulk(channel)}:${channels.size}\r\n")
                }
                flush(out)
              case "PUNSUBSCRIBE" =>
                hold(s"*3\r\n${bulk("punsubscribe")}$$-1\r\n:${channels.size}\r\n")
                flush(out)
              case other =>
                hold(s"-ERR unknown command '$other'\r\n")
                flush(out)
            }
        }
      }
    } catch {
      case _: IOException =>
    } finally {
      try socket.close() catch { case _: IOException => }
      sockets.remove(socket)
    }
  }

  private def readLine(in: InputStream): Option[String] = {
    val line = new StringBuilder
    var c = in.read()
    while (c != -1 && c != '\n') {
      if (c != '\r') line.append(c.toChar)
      c = in.read()
    }
    if (c == -1) None else Some(line.toString)
  }

  /** One command: an array of bulk strings */
  private def readCommand(in: InputStream): Option[Seq[String]] = readLine(in).map { header =>
    require(header.startsWith("*"), s"not an array: $header")
    (1 to header.drop(1).toInt).map { _ =>
      val length = readLine(in).get.drop(1).toInt
      val bytes = new Array[Byte](length)
      var read = 0
      while (read < length) {
        val n = in.read(bytes, read, length - read)
        if (n < 0) throw new IOException("closed")
        read += n
      }
      readLine(in)
      new String(bytes, UTF_8)
    }
  }
}
