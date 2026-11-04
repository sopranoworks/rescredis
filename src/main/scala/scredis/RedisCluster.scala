// Modified by Sopranoworks: JsonCommands mixed in; tcp buffer hints and decoder dispatcher path passed on; StreamCommands mixed in;
// WAIT routed by a key, SCRIPT LOAD / EXISTS / FLUSH sent to every master, a blocking client.
package scredis

import akka.actor.ActorSystem
import com.typesafe.config.Config
import scredis.commands._
import scredis.io.{ClusterConnection, Connection}
import scredis.protocol.AuthConfig
import scredis.protocol.requests.KeyRequests.WaitRoutedBy
import scredis.protocol.requests.ScriptingRequests.{ScriptExists, ScriptFlush, ScriptLoad}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

/**
  * Defines a `RedisCluster` [[scredis.Client]] supporting all non-blocking commands that can be addressed to either
  * any cluster node or be automatically routed to the correct node.
  *
  * @define e [[scredis.exceptions.RedisErrorResponseException]]
  * @define redisCluster [[scredis.RedisCluster]]
  * @define typesafeConfig com.typesafe.Config
  */
class RedisCluster private[scredis](
    nodes: Seq[Server] = RedisConfigDefaults.Redis.ClusterNodes,
    maxRetries: Int = 4,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
    tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
    clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait,
    systemOpt:Option[ActorSystem] = None,
    failCommandOnConnecting: Boolean = RedisConfigDefaults.Global.FailCommandOnConnecting,
    authOpt: Option[AuthConfig] = RedisConfigDefaults.Config.Redis.AuthOpt
  )
  extends ClusterConnection(
    nodes = nodes,
    maxRetries = maxRetries,
    receiveTimeoutOpt = receiveTimeoutOpt,
    connectTimeout = connectTimeout,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpSendBufferSizeHint,
    tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
    tryAgainWait = tryAgainWait,
    clusterDownWait = clusterDownWait,
    systemOpt = systemOpt,
    failCommandOnConnecting = failCommandOnConnecting,
    authOpt = authOpt
  ) with Connection
  with ClusterCommands
  with HashCommands
  with HyperLogLogCommands
  with KeyCommands
  with ListCommands
  with PubSubCommands
  with ScriptingCommands
  with SetCommands
  with SortedSetCommands
  with StreamCommands
  with StringCommands
  with JsonCommands
  //with SubscriberCommands
{
  override implicit val dispatcher: ExecutionContext =
    ExecutionContext.Implicits.global // TODO perhaps implement our own

  @volatile private var blockingInitialized = false

  /**
   * Lazily initialized [[scredis.ClusterBlockingClient]]: `BLPOP`, `BRPOP`, `BRPOPLPUSH` and `XREAD … BLOCK`, each
   * routed by its key to the master of the key's slot on a blocking connection of its own (see
   * [[scredis.ClusterBlockingClient]] for the rule when the slot's master changes).
   *
   * @since 1.1.0
   */
  lazy val blocking: ClusterBlockingClient = {
    blockingInitialized = true
    new ClusterBlockingClient(this, server => BlockingClient(
      host = server.host,
      port = server.port,
      authOpt = authOpt,
      database = 0,
      nameOpt = None,
      connectTimeout = connectTimeout,
      maxWriteBatchSize = maxWriteBatchSize,
      tcpSendBufferSizeHint = tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
      akkaListenerDispatcherPath = akkaListenerDispatcherPath,
      akkaIODispatcherPath = akkaIODispatcherPath,
      akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
      failCommandOnConnecting = failCommandOnConnecting
    )(actorSystem))
  }

  /**
   * WAIT on the connection to the master of `key`'s slot: blocks until the writes sent before it on that
   * connection have been acknowledged by at least `numReplicas` replicas, or the timeout is over.
   *
   * `WAIT numreplicas timeout` (`key` is not sent)
   *
   * WAIT has no key; Redis applies it to the connection it is sent on. `key` only chooses that connection: the one
   * to the master of its slot, as the slot cache has it, which is the connection the writes to keys of that slot
   * were sent on (this client has one connection per node, shared by everything sent to that node). Nothing else is
   * done on the client side: no MOVED is answered to WAIT, and when the cache is out of date the node it reaches may
   * be another one (a replica answers an error, which is returned). As [[scredis.commands.KeyCommands.wait]], it
   * blocks the commands sent after it to that node, and the receive timeout applies.
   *
   * @param key a key whose slot's master is the node to wait on
   * @param numReplicas the number of replicas that must have acknowledged the writes
   * @param timeout the longest time to block, in milliseconds on the wire; 0 blocks without limit
   * @return the number of replicas that acknowledged the writes, as Redis answers it
   *
   * @since 3.0.0
   */
  def wait(key: String, numReplicas: Int, timeout: FiniteDuration): Future[Long] =
    send(WaitRoutedBy(key, numReplicas, timeout.toMillis))

  /**
   * Loads a script into the script cache of every master (SCRIPT LOAD).
   *
   * `SCRIPT LOAD script`, sent to every master (read from the cluster with CLUSTER SLOTS first)
   *
   * SCRIPT LOAD has no key, and a script must be in the cache of the node that runs it (EVALSHA is routed by its
   * first key to the master of that key's slot). This is the routing a keyless script command needs on a cluster
   * (connection management, not a changed command): the result is the SHA1 once every master has answered it (it is
   * the same on every node); an error from any master fails the call with that error. Redis' own command tips name
   * all nodes for SCRIPT LOAD; replicas are not sent it, since this client sends EVALSHA to masters only.
   *
   * The script cache is not replicated: a master added later, or a replica promoted by a failover, does not have
   * the script, and EVALSHA there is answered `NOSCRIPT` (a [[scredis.exceptions.RedisErrorResponseException]]);
   * load it again (or use EVAL).
   *
   * @param script the Lua script
   * @return the SHA1 of the script
   *
   * @since 2.6.0
   */
  override def scriptLoad(script: String): Future[String] = sendToMasters(() => ScriptLoad(script)).map(_.head)

  /**
   * Whether scripts are in the script cache of every master (SCRIPT EXISTS).
   *
   * `SCRIPT EXISTS sha1 [sha1 ...]`, sent to every master (read from the cluster with CLUSTER SLOTS first)
   *
   * Routed as [[scriptLoad]] (connection management, not a changed command): a script is reported as existing when
   * every master has it, which is what EVALSHA on any key needs (Redis' command tips: all shards, logical AND of the
   * replies). An error from any master fails the call with that error.
   *
   * @param sha1s SHA1 digest(s) of the scripts
   * @return per SHA1, true if every master has the script
   *
   * @since 2.6.0
   */
  override def scriptExists(sha1s: String*): Future[Map[String, Boolean]] =
    sendToMasters(() => ScriptExists(sha1s: _*)).map { replies =>
      sha1s.map(sha1 => sha1 -> replies.forall(_.getOrElse(sha1, false))).toMap
    }

  /**
   * Removes all the scripts from the script cache of every master (SCRIPT FLUSH).
   *
   * `SCRIPT FLUSH`, sent to every master (read from the cluster with CLUSTER SLOTS first)
   *
   * Routed as [[scriptLoad]] (connection management, not a changed command); completes when every master has
   * answered, and an error from any master fails the call with that error.
   *
   * @since 2.6.0
   */
  override def scriptFlush(): Future[Unit] = sendToMasters(() => ScriptFlush()).map(_ => ())

  /** Closes the blocking client, if it was used, then the cluster's connections */
  override def quit(): Future[Unit] = {
    if (blockingInitialized) blocking.close()
    super.quit()
  }

  /**
    * Constructs a $redisCluster instance from a [[scredis.RedisConfig]].
    *
    * @return the constructed $redisCluster
    */
  def this(config: RedisConfig, systemOpt:Option[ActorSystem]) = this(
    nodes = config.Redis.ClusterNodes,
    maxRetries = 4,
    receiveTimeoutOpt = config.IO.ReceiveTimeoutOpt,
    connectTimeout = config.IO.ConnectTimeout,
    maxWriteBatchSize = config.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint = config.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint = config.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath = config.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath = config.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath = config.IO.Akka.DecoderDispatcherPath,
    tryAgainWait = config.IO.Cluster.TryAgainWait,
    clusterDownWait = config.IO.Cluster.ClusterDownWait,
    systemOpt = systemOpt,
    failCommandOnConnecting = config.Global.FailCommandOnConnecting,
    authOpt = config.Redis.AuthOpt
  )

  /**
    * Constructs a $redisCluster instance using the default config.
    *
    * @return the constructed $redisCluster
    */
  def this() = this(RedisConfig(), None)


  /**
    * Constructs a $redisCluster instance using the default config.
    *
    * @param system Actor system
    * @return the constructed $redisCluster
    */
  def this(system:ActorSystem) = this(RedisConfig(), Some(system))

}

/**
  * @define redisCluster [[scredis.RedisCluster]]
  * @define typesafeConfig com.typesafe.Config
  */
object RedisCluster {

  /**
    * Constructs a $redisCluster instance using provided parameters.
    *
    * @param nodes list of server nodes, used as seed nodes to initially connect to the cluster.
    * @param maxRetries maximum number of retries and redirects to perform for a single command
    * @param receiveTimeoutOpt optional batch receive timeout
    * @param connectTimeout connection timeout
    * @param maxWriteBatchSize max number of bytes to send as part of a batch
    * @param tcpSendBufferSizeHint size hint of the tcp send buffer, in bytes
    * @param tcpReceiveBufferSizeHint size hint of the tcp receive buffer, in bytes
    * @param akkaListenerDispatcherPath path to listener dispatcher definition
    * @param akkaIODispatcherPath path to io dispatcher definition
    * @param akkaDecoderDispatcherPath path to decoder dispatcher definition
    * @param tryAgainWait time to wait after a TRYAGAIN response by a cluster node
    * @param clusterDownWait time to wait for a retry after CLUSTERDOWN response
    * @param systemOpt Actor System (optionally)
    * @param failCommandOnConnecting indicates whether to fail fast on all requests until there is a working connection.
    * @param authOpt optional server authorization credentials
    * @return the constructed $redisCluster
    */
  def apply(
    nodes: Seq[Server] = RedisConfigDefaults.Redis.ClusterNodes,
    maxRetries: Int = 4,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
    tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
    clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait,
    systemOpt: Option[ActorSystem] = None,
    failCommandOnConnecting: Boolean = RedisConfigDefaults.Global.FailCommandOnConnecting,
    authOpt: Option[AuthConfig] = RedisConfigDefaults.Config.Redis.AuthOpt
  ) = new RedisCluster(
    nodes = nodes,
    maxRetries = maxRetries,
    receiveTimeoutOpt = receiveTimeoutOpt,
    connectTimeout = connectTimeout,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpSendBufferSizeHint,
    tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
    tryAgainWait = tryAgainWait,
    clusterDownWait = clusterDownWait,
    systemOpt = systemOpt,
    failCommandOnConnecting = failCommandOnConnecting,
    authOpt = authOpt
  )


  /**
    * Constructs a $redisCluster instance with given seed nodes, using the default config for all other parameters.
    *
    * @return the constructed $redisCluster
    */
  def apply(node: Server, nodes: Server*): RedisCluster = RedisCluster( nodes = node +: nodes)

  /**
    * Constructs a $redisCluster instance from a [[scredis.RedisConfig]].
    *
    * @param config a [[scredis.RedisConfig]]
    * @return the constructed $redisCluster
    */
  def apply(config: RedisConfig, systemOpt:Option[ActorSystem]) = new RedisCluster(config, systemOpt)

  /**
    * Constructs a $redisCluster instance from a $typesafeConfig.
    *
    * @note The config must contain the scredis object at its root.
    *
    * @param config a $typesafeConfig
    * @return the constructed $redisCluster
    */
  def apply(config: Config) = new RedisCluster(RedisConfig(config), None)


  /**
    * Constructs a $redisCluster instance from a config file.
    *
    * @note The config file must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * Redis(configName, "scredis")
    * }}}
    *
    * @param configName config filename
    * @return the constructed $redisCluster
    */
  def apply(configName: String): RedisCluster = new RedisCluster(RedisConfig(configName), None)

  /**
    * Constructs a $redisCluster instance from a config file and using the provided path.
    *
    * @note The path must include to the scredis object, e.g. x.y.scredis
    *
    * @param configName config filename
    * @param path path pointing to the scredis config object
    * @return the constructed $redisCluster
    */
  def apply(configName: String, path: String): RedisCluster = new RedisCluster(RedisConfig(configName, path), None)

}