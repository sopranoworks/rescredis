// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, ServerSocket}

import scredis.util.RawRedis.Node

/**
 * The failover cluster of the test setup (7100 - 7105, cluster-node-timeout 2000, see testing.md): kills a
 * master with DEBUG SEGFAULT and waits for its replica to take over, and for the killed node to be back as a
 * replica. Every node runs in a loop that starts it again 6 s after it died.
 */
object FailoverCluster {
  val Ports: Seq[Int] = 7100 to 7105

  private def now(): Long = System.nanoTime()
  private def pause(ms: Long): Unit = Thread.sleep(ms)

  private def tryNodes(port: Int): Option[List[Node]] =
    try Some(RawRedis.clusterNodes(port)) catch { case _: IOException | _: IllegalStateException => None }

  private def clusterStateOk(port: Int): Boolean =
    try RawRedis.withConnection("127.0.0.1", port)(_.string("CLUSTER", "INFO")).contains("cluster_state:ok")
    catch { case _: IOException | _: IllegalStateException => false } // down, or LOADING after a restart

  /** CLUSTER NODES of the first node that answers */
  def nodes(): List[Node] =
    Ports.iterator.map(tryNodes).collectFirst { case Some(n) => n }.getOrElse(throw new IOException("no node answers"))

  def masterOf(slot: Int): Node = nodes().find(n => n.isMaster && !n.isFailed && n.owns(slot))
    .getOrElse(throw new IllegalStateException(s"no master for slot $slot"))

  def masters(): List[Node] = nodes().filter(n => n.isMaster && !n.isFailed && n.slots.nonEmpty)

  /** Crashes the node (DEBUG SEGFAULT): its connections are closed by the kernel, nothing more is answered */
  def kill(port: Int): Unit = RawRedis.withConnection("127.0.0.1", port)(_.commandWithoutReply("DEBUG", "SEGFAULT"))

  /**
   * Crashes the node and, while it is down, listens on its port and closes every connection it accepts, as a
   * forwarder in front of a dead server does (Docker's published ports, a load balancer), for `holdMillis`
   * (less than the 6 s after which the node is started again). Returns the number of connections accepted.
   */
  def killBehindForwarder(port: Int, holdMillis: Long): Int = {
    kill(port)
    val deadline = System.nanoTime() + holdMillis * 1000000L
    var socket: ServerSocket = null
    while (socket == null && System.nanoTime() < deadline) {
      try {
        val s = new ServerSocket()
        s.setReuseAddress(true)
        s.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, port))
        socket = s
      } catch { case _: IOException => pause(20) }
    }
    var accepted = 0
    if (socket != null) {
      try {
        while (System.nanoTime() < deadline) {
          socket.setSoTimeout(math.max(1, ((deadline - System.nanoTime()) / 1000000L).toInt))
          try { socket.accept().close(); accepted += 1 } catch { case _: java.net.SocketTimeoutException => }
        }
      } finally socket.close()
    }
    accepted
  }

  /** Waits until a node other than `killedPort` is the master of `slot` and every live node says cluster_state:ok */
  def awaitFailover(killedPort: Int, slot: Int, timeoutMillis: Long = 30000): Node = {
    val deadline = now() + timeoutMillis * 1000000L
    var result: Option[Node] = None
    while (result.isEmpty) {
      if (now() > deadline) throw new IllegalStateException(s"no failover of $killedPort within $timeoutMillis ms")
      val live = Ports.filter(_ != killedPort)
      val view = live.iterator.map(tryNodes).collectFirst { case Some(n) => n }
      result = view.flatMap(_.find(n => n.isMaster && !n.isFailed && n.port != killedPort && n.owns(slot)))
        .filter(_ => live.forall(clusterStateOk))
      if (result.isEmpty) pause(100)
    }
    result.get
  }

  private def replicaInSync(port: Int): Boolean =
    try {
      val info = RawRedis.withConnection("127.0.0.1", port)(_.string("INFO", "replication"))
      !info.contains("role:slave") || (info.contains("master_link_status:up") && info.contains("master_sync_in_progress:0"))
    } catch { case _: IOException | _: IllegalStateException => false }

  /**
   * Waits until all six nodes answer, every node says cluster_state:ok, there are 3 masters with 1 replica each,
   * and every replica has completed its sync with its master (a replica that has not cannot take over)
   */
  def awaitWhole(timeoutMillis: Long = 60000): Unit = {
    val deadline = now() + timeoutMillis * 1000000L
    def whole: Boolean = Ports.forall(clusterStateOk) && {
      val n = nodes()
      val masters = n.filter(m => m.isMaster && m.slots.nonEmpty)
      n.size == 6 && !n.exists(_.isFailed) && n.forall(_.linkState == "connected") && masters.size == 3 &&
        masters.forall(m => n.count(_.masterId == m.id) == 1) &&
        Ports.forall(p => tryNodes(p).exists(view => !view.exists(_.isFailed))) && Ports.forall(replicaInSync)
    }
    while (!whole) {
      if (now() > deadline) throw new IllegalStateException(s"failover cluster not whole within $timeoutMillis ms: ${nodes()}")
      pause(200)
    }
  }
}
