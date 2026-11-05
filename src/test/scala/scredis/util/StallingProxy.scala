// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A TCP forwarder that can stall: while stalled, the connections stay open and everything sent on them in either
 * direction is dropped, as on a connection whose peer died without the connection being closed (a host that
 * vanished, a network partition), or only the replies are dropped. It can also cut every connection (RST).
 */
final class StallingProxy(targetHost: String, targetPort: Int) {

  private val server = {
    val s = new ServerSocket()
    s.setReuseAddress(true)
    s.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    s
  }
  val port: Int = server.getLocalPort

  private val stalled = new AtomicBoolean(false)
  private val repliesStalled = new AtomicBoolean(false)
  private val pairs = new ConcurrentLinkedQueue[(Socket, Socket)]()

  daemon(s"stalling-proxy-$port-accept") {
    try {
      while (!server.isClosed) {
        val client = server.accept()
        val upstream = try new Socket(targetHost, targetPort) catch { case _: IOException => null }
        if (upstream == null) client.close()
        else {
          client.setTcpNoDelay(true)
          upstream.setTcpNoDelay(true)
          pairs.add((client, upstream))
          pump(client, upstream, client, upstream, toClient = false)
          pump(upstream, client, client, upstream, toClient = true)
        }
      }
    } catch { case _: IOException => }
  }

  /** Drops everything from now on (true), or forwards again (false) */
  def stall(on: Boolean): Unit = stalled.set(on)

  /** Forwards the requests to the server and drops its replies from now on (true), or forwards them again (false) */
  def stallReplies(on: Boolean): Unit = repliesStalled.set(on)

  /** Cuts every connection: RST to the client */
  def cutAll(): Unit = {
    var pair = pairs.poll()
    while (pair != null) {
      try pair._1.setSoLinger(true, 0) catch { case _: IOException => }
      close(pair._1, pair._2)
      pair = pairs.poll()
    }
  }

  def stop(): Unit = {
    server.close()
    cutAll()
  }

  private def close(a: Socket, b: Socket): Unit = {
    try a.close() catch { case _: IOException => }
    try b.close() catch { case _: IOException => }
  }

  private def daemon(name: String)(body: => Unit): Unit = {
    val t = new Thread(new Runnable { def run(): Unit = body }, name)
    t.setDaemon(true)
    t.start()
  }

  private def pump(from: Socket, to: Socket, client: Socket, upstream: Socket, toClient: Boolean): Unit = daemon(s"stalling-proxy-$port-pump") {
    val buffer = new Array[Byte](65536)
    try {
      val in = from.getInputStream
      val out = to.getOutputStream
      var n = in.read(buffer)
      while (n >= 0) {
        if (!stalled.get() && !(toClient && repliesStalled.get())) {
          out.write(buffer, 0, n)
          out.flush()
        }
        n = in.read(buffer)
      }
    } catch { case _: IOException => }
    close(client, upstream)
  }
}
