import akka.actor.{Actor, ActorSelection, Props, Timers}
import scala.concurrent.duration._

import Global._

object PublishSubscribe {
  // Constructor
  def props(ip: String, port: Int): Props = Props(new PublishSubscribe(ip, port))

  // Messages
  final case class Neighbours(n: Set[String])
  final case class Publish(topic: String, message: String)
  final case class Subscribe(topic: String)
  final case class Unsubscribe(topic: String)
  final case class TopicUpdate(sender: String, senderTopics: Set[String])
  final case class AntiEntropy(sender: String, senderMsgs: Set[String], senderTopics: Set[String])
  final case class EagerPush(sender: String, topic: String, mid: String, msg: String, hop: Int)
  final case class EagerPushRequest(sender: String, mid: String)
  final case class LazyPush(sender: String, mid: String)
  final case object AntiEntropyTimer
  // Application Messages
  final case object LogNeighboursTopics
  final case object LogStats
}

// TODO: Limit delivered max size
// TODO: Better selectBestTarget -> can become deterministic ATM which is bad -> we always Pull from the same neighbour
class PublishSubscribe(ip: String, port: Int) extends Actor with Timers {
  import PublishSubscribe._
  import HyParView._
  import Application.PSDeliver

  // Type definitions
  type DeliveredMessage = (String, String, String, Int) // (topic, message_id, message, hop_count)

  // Constants
  val MYSELF: String = s"$ip:$port"
  val MAX_HOPS = 3
  val MESSAGE_SIZE_THRESHOLD = 65536  // bytes , Bigger messages are considered 'Big'

  // State
  var neighbours: Set[String] = Set()
  var topics: Set[String] = Set()
  var requested: Set[String] = Set()
  var delivered: Map[String, DeliveredMessage] = Map()
  var neighboursTopics: Map[String, Set[String]] = Map()

  // Benchmark state
  var totalMsgsSent = 0
  var totalMsgsReceived = 0
  var totalMsgsDelivered = 0

  // Timers
  timers.startPeriodicTimer(AntiEntropyTimer, AntiEntropyTimer, 15 seconds)

  // Init
  override def preStart(): Unit = {
    super.preStart()
    hyParViewActor ! GetNeighbours // Trigger GetNeighbours() request
  }

  // Receive
  override def receive: Receive = {
    case Neighbours(n) => handleNeighboursIndication(n)
    case Publish(topic, message) => handlePublish(topic, message)
    case Subscribe(topic) => handleSubscribe(topic)
    case Unsubscribe(topic) => handleUnsubscribe(topic)
    case TopicUpdate(sender, senderTopics) => handleTopicUpdates(sender, senderTopics)
    case AntiEntropyTimer => handleAntiEntropyTimer()
    case AntiEntropy(sender, senderMsgs, senderTopics) => handleAntiEntropy(sender, senderMsgs, senderTopics)
    case EagerPush(sender, topic, mid, msg, hop) => handleEagerPush(sender, topic, mid, msg, hop)
    case EagerPushRequest(sender, mid) => handleEagerPushRequest(sender, mid)
    case LazyPush(sender, mid) => handleLazyPush(sender, mid)
    // Application Messages
    case LogNeighboursTopics => handleLogNeighboursTopics()
    case LogStats => handleLogStats()
  }

  /* ---------------------------- Handlers ---------------------------- */

  private def handleNeighboursIndication(n: Set[String]): Unit = {
    var newNeighbours: Set[String] = null
    if (neighbours.isEmpty) {
      neighbours = n
      newNeighbours = n
      neighbours.foreach(p => neighboursTopics = neighboursTopics + (p -> Set()))
    } else {
      newNeighbours = mergeNeighbours(n)
    }
    if (topics.nonEmpty) {
      announceTopics(newNeighbours)
    }
  }

  private def handlePublish(topic: String, message: String): Unit = {
    val mid = generateUniqueID(topic, message)
    delivered = delivered + (mid -> (topic, mid, message, 0))
    if (topics.contains(topic)) {
      applicationActor ! PSDeliver(topic, message) // Trigger PSDeliver(topic, message) indication
      totalMsgsDelivered += 1 // BENCHMARK
    }
    neighbours.foreach(p => {
      remotePublishSubscribeActor(p) ! EagerPush(MYSELF, topic, mid, message, 1)
      totalMsgsSent += 1  // BENCHMARK
    })
  }

  private def handleSubscribe(topic: String): Unit = {
    topics = topics + topic
    neighbours.foreach(p => remotePublishSubscribeActor(p) ! TopicUpdate(MYSELF, topics))
  }

  private def handleUnsubscribe(topic: String): Unit = {
    topics = topics - topic
    neighbours.foreach(p => remotePublishSubscribeActor(p) ! TopicUpdate(MYSELF, topics))
  }

  private def handleTopicUpdates(sender: String, senderTopics: Set[String]): Unit = {
    neighboursTopics = neighboursTopics + (sender -> senderTopics)
  }

  private def handleAntiEntropyTimer(): Unit = {
    val target = selectBestTarget()
    if (target != null) {
      val knownMessages = delivered.keySet  // Set of all mids
      println(s"[Publish Subscribe] Sending AntiEntropy message to $target with knownMessages of size ${knownMessages.size}")
      remotePublishSubscribeActor(target) ! AntiEntropy(MYSELF, knownMessages, topics) // knownMessages might be empty
      totalMsgsSent += 1  // BENCHMARK
    }
  }

  private def handleAntiEntropy(sender: String, senderMsgs: Set[String], senderTopics: Set[String]): Unit = {
    delivered.values.foreach(m => {
      val (topic, mid, message, hop) = m
      if (!senderMsgs.contains(mid) && senderTopics.contains(topic)) {
        println(s"[Publish Subscribe] Sending AntiEntropy response to $sender with message $mid")
        remotePublishSubscribeActor(sender) ! EagerPush(MYSELF, topic, mid, message, hop + 1)
        totalMsgsSent += 1  // BENCHMARK
      }
    })
    totalMsgsReceived += 1  // BENCHMARK
  }

  private def handleEagerPush(sender: String, topic: String, mid: String, msg: String, hop: Int): Unit = {
    if (!delivered.contains(mid)) {
      delivered = delivered + (mid -> (topic, mid, msg, hop))
      requested = requested - mid // If it's there, removes
      if (topics.contains(topic)) {
        applicationActor ! PSDeliver(topic, msg) // Trigger PSDeliver(topic, message) indication
        totalMsgsDelivered += 1 // BENCHMARK
      }
      (neighbours diff Set(sender)).foreach(p => {
        if (hop <= MAX_HOPS || msg.length < MESSAGE_SIZE_THRESHOLD) {
          remotePublishSubscribeActor(p) ! EagerPush(MYSELF, topic, mid, msg, hop + 1)
          totalMsgsSent += 1  // BENCHMARK
        } else {
          remotePublishSubscribeActor(p) ! LazyPush(MYSELF, mid)
          totalMsgsSent += 1  // BENCHMARK
        }
      })
    }
    totalMsgsReceived += 1  // BENCHMARK
  }

  private def handleEagerPushRequest(sender: String, mid: String): Unit = {
    if (delivered.contains(mid)) {
      val (topic, _, msg, hop) = delivered(mid)
      remotePublishSubscribeActor(sender) ! EagerPush(MYSELF, topic, mid, msg, hop + 1)
      totalMsgsSent += 1  // BENCHMARK
    }
    totalMsgsReceived += 1  // BENCHMARK
  }

  private def handleLazyPush(sender: String, mid: String): Unit = {
    if (!delivered.contains(mid) && !requested.contains(mid)) {
      requested = requested + mid
      remotePublishSubscribeActor(sender) ! EagerPushRequest(MYSELF, mid)
      totalMsgsSent += 1  // BENCHMARK
    }
    totalMsgsReceived += 1  // BENCHMARK
  }

  /* ---------------------------- Procedures and Utils ---------------------------- */

  private def announceTopics(n: Set[String]): Unit = {
    n.foreach(p => remotePublishSubscribeActor(p) ! TopicUpdate(MYSELF, topics))
  }
  
  private def mergeNeighbours(n: Set[String]): Set[String] = {
    var newNeighbours: Set[String] = Set()
    neighbours.foreach(p => {
      if (!n.contains(p)) {
        neighbours = neighbours - p
        neighboursTopics = neighboursTopics - p
      }
    })
    n.foreach(p => {
      if (!neighbours.contains(p)) {
        newNeighbours = newNeighbours + p
        neighbours= neighbours + p
        neighboursTopics = neighboursTopics + (p -> Set())
      }
    })
    return newNeighbours
  }

  // Selects (one of) the node(s) which has more topics in common with us
  private def selectBestTarget(): String = {
    var target: String = null
    var bestCommonCount = 0
    neighbours.foreach(p => {
      var commonTopicsCounts = 0
      if (neighboursTopics.contains(p)) {
        neighboursTopics(p).foreach(t => {
          if (topics.contains(t)) {
            commonTopicsCounts += 1
          }
        })
      }
      if (commonTopicsCounts > bestCommonCount) {
        bestCommonCount = commonTopicsCounts
        target = p
      }
    })
    return target
  }

  private def generateUniqueID(topic: String, msg: String): String = {
    import java.lang.System.currentTimeMillis
    val localTimestamp = currentTimeMillis().toString
    return generateID(localTimestamp+MYSELF+topic+msg)
  }

  // MD5 implementation from the internet ¯\_(ツ)_/¯
  private def generateID(s: String): String = {
    import java.security.MessageDigest
    import java.math.BigInteger
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    return hashedString
  }

  /* ---------------------------- Application Handlers ---------------------------- */

  private def handleLogNeighboursTopics(): Unit = {
    println("###### Neighbours Topics ######")
    for ((p, topicsSet) <- neighboursTopics) {
      if (topicsSet.size > 1) {
        println(s"$p: [${topicsSet.reduceLeft((t1, t2) => s"$t1, $t2")}]")
      } else if (topicsSet.size == 1) {
        println(s"$p: [${topicsSet.toVector(0)}]")
      } else {
        println(s"$p: []")
      }
    }
    println()
  }

  private def handleLogStats(): Unit = {
    println("###### Stats ######")
    println(s"Total messages received: $totalMsgsReceived")
    println(s"Total messages sent: $totalMsgsSent")
    println(s"Total mssages delivered to application layer: $totalMsgsDelivered\n")
  }

  /* ---------------------------- Actors ---------------------------- */

  private def remotePublishSubscribeActor(id: String) : ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$id/user/$PUBLISH_SUBSCRIBE_ACTOR_NAME")
  }

  private def hyParViewActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$MYSELF/user/$HYPARVIEW_ACTOR_NAME")
  }

  private def applicationActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$MYSELF/user/$APPLICATION_ACTOR_NAME")
  }

}
