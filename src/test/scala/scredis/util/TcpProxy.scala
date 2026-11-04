// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

import java.io.{IOException, InputStream, OutputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * A TCP forwarder between the client under test and a Redis server, for fault injection: it cuts every
 * connection (RST), accepts connections and closes them at once (a server that is down), refuses
 * connections (nothing listening), or cuts a connection part of the way through the next reply. It counts
 * the connections it accepted.
 */
final class TcpProxy(targetHost: String, targetPort: Int) {

  @volatile private var server: ServerSocket = bind(0)
  val port: Int = server.getLocalPort

  private val acceptedCount = new AtomicInteger(0)
  private val pairs = new ConcurrentLinkedQueue[(Socket, Socket)]()
  private val closeOnAccept = new AtomicBoolean(false)
  // bytes of the next reply (server to client) to forward before the connection is cut; -1 = none
  private val cutAfter = new AtomicInteger(-1)
  @volatile private var stopped = false

  acceptLoop(server)

  /** The number of connections accepted so far (including those closed at once) */
  def accepted: Int = acceptedCount.get()

  /** Cuts every connection: RST to the client, close to the server */
  def cutAll(): Unit = {
    var pair = pairs.poll()
    while (pair != null) {
      reset(pair._1, pair._2)
      pair = pairs.poll()
    }
  }

  /**
   * From now on connections are accepted and closed at once (true), as a forwarder does whose server is down
   * (Docker's published ports, a load balancer), or forwarded again (false)
   */
  def closeConnectionsOnAccept(close: Boolean): Unit = closeOnAccept.set(close)

  /** Stops listening: connection attempts are refused until [[listen]] */
  def refuse(): Unit = server.close()

  /** Listens again on the same port after [[refuse]] */
  def listen(): Unit = {
    server = bind(port)
    acceptLoop(server)
  }

  /** The next reply data is forwarded only up to `bytes` bytes; then that connection is cut */
  def cutInNextReply(bytes: Int): Unit = cutAfter.set(bytes)

  def stop(): Unit = {
    stopped = true
    server.close()
    cutAll()
  }

  private def bind(p: Int): ServerSocket = {
    val s = new ServerSocket()
    s.setReuseAddress(true)
    s.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, p))
    s
  }

  private def daemon(name: String)(body: => Unit): Unit = {
    val t = new Thread(new Runnable { def run(): Unit = body }, name)
    t.setDaemon(true)
    t.start()
  }

  private def reset(client: Socket, upstream: Socket): Unit = {
    try client.setSoLinger(true, 0) catch { case _: IOException => }
    close(client, upstream)
  }

  private def close(client: Socket, upstream: Socket): Unit = {
    try client.close() catch { case _: IOException => }
    try upstream.close() catch { case _: IOException => }
  }

  private def acceptLoop(s: ServerSocket): Unit = daemon(s"tcp-proxy-$port-accept") {
    try {
      while (!stopped && !s.isClosed) {
        val client = s.accept()
        acceptedCount.incrementAndGet()
        if (closeOnAccept.get()) client.close()
        else {
          val upstream = try new Socket(targetHost, targetPort) catch { case _: IOException => null }
          if (upstream == null) client.close()
          else {
            client.setTcpNoDelay(true)
            upstream.setTcpNoDelay(true)
            pairs.add((client, upstream))
            pump(client, upstream, toClient = true)
            pump(client, upstream, toClient = false)
          }
        }
      }
    } catch {
      case _: IOException => // the listening socket was closed
    }
  }

  // toClient: forwards server -> client (the replies), else client -> server (the requests)
  private def pump(client: Socket, upstream: Socket, toClient: Boolean): Unit = {
    val in: InputStream = (if (toClient) upstream else client).getInputStream
    val out: OutputStream = (if (toClient) client else upstream).getOutputStream
    daemon(s"tcp-proxy-$port-${if (toClient) "down" else "up"}") {
      val buffer = new Array[Byte](65536)
      try {
        var n = in.read(buffer)
        while (n >= 0) {
          val limit = if (toClient) cutAfter.getAndSet(-1) else -1
          if (limit >= 0) {
            out.write(buffer, 0, math.min(limit, n))
            out.flush()
            reset(client, upstream)
            n = -1
          } else {
            out.write(buffer, 0, n)
            out.flush()
            n = in.read(buffer)
          }
        }
      } catch {
        case _: IOException =>
      }
      close(client, upstream)
    }
  }
}
