import akka.actor.{Actor, ActorSelection, Props, Timers}
import scala.collection.mutable.{HashMap, Map}
import scala.concurrent.duration._

object PublishSubscribe {
  // Constructor
  def props(ip: String, port: Int): Props =
    Props(new PublishSubscribe(ip, port))

  // Messages
  final case class Publish(topic: String, message: String)
  final case class Subscribe(topic: String)
  final case class Unsubscribe(topic: String)
  final case class Neighbours(n: Set[String])
  final case class Pull(sender: String, senderMsgs: Set[String], senderTopics: Set[String])
  final case class EagerPush(sender: String, topic: String, mid: String, msg: String, hop: Int)
  final case class EagerPushRequest(sender: String, mid: String)
  final case class LazyPush(sender: String, topic: String, mid: String)
  final case class TopicUpdate(sender: String, topics: Set[String])
  final case object AntiEntropyTimer
}

// TODO: TIRAR PRINTS
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
  var delivered: Map[String, DeliveredMessage] = HashMap()  // Mutable
  var neighboursTopics: Map[String, Set[String]] = HashMap()  // Mutable

  // Timers
  timers.startPeriodicTimer(AntiEntropyTimer, AntiEntropyTimer, 15 seconds)

  // Init
  override def preStart(): Unit = {
    super.preStart()
    hyParViewActor ! GetNeighbours // Trigger GetNeighbours() request
  }

  // Receive
  override def receive: Receive = {

    case Neighbours(n) =>
      if (neighbours.isEmpty) {
        neighbours = n
        neighbours.foreach(p => neighboursTopics(p) = Set())
        announceTopics(neighbours)
      } else {
        val newNeighbours = mergeNeighbours(n)
        announceTopics(newNeighbours)
      }

    case AntiEntropyTimer =>
      val target = selectBestTarget()
      if (target != null) {
        var knownMessages: Set[String] = Set()
        delivered.values.foreach(m => {
          val mid = m._2
          knownMessages = knownMessages + mid
        })
        remotePublishSubscribeActor(target) ! Pull(MYSELF, knownMessages, topics) // knownMessages might be an empty set
      }

    case Pull(sender, senderMsgs, senderTopics) =>
      delivered.values.foreach(m => {
        val (topic, mid, message, hop) = m
        if (!senderMsgs.contains(mid) && senderTopics.contains(topic)) {
          remotePublishSubscribeActor(sender) ! EagerPush(MYSELF, topic, mid, message, hop + 1)
        }
      })

    case EagerPush(sender, topic, mid, msg, hop) =>
      if (topics.contains(topic) && !delivered.contains(mid)) {
        applicationActor ! PSDeliver(topic, msg) // Trigger PSDeliver(topic, message) indication
        delivered(mid) = (topic, mid, msg, hop)
        requested = requested - mid // If it's there, removes
        (neighbours diff Set(sender)).foreach(p => {
          if (hop <= MAX_HOPS || msg.size < MESSAGE_SIZE_THRESHOLD) {
            remotePublishSubscribeActor(p) ! EagerPush(MYSELF, topic, mid, msg, hop + 1)
          } else {
            remotePublishSubscribeActor(p) ! LazyPush(MYSELF, topic, mid)
          }
        })
      }

    case LazyPush(sender, topic, mid) =>
      if (topics.contains(topic) && !delivered.contains(mid) && !requested.contains(mid)) {
        requested = requested + mid
        remotePublishSubscribeActor(sender) ! EagerPushRequest(MYSELF, mid)
      }

    case EagerPushRequest(sender, mid) =>
      if (delivered.contains(mid)) {
        val (topic, _, msg, hop) = delivered(mid)
        remotePublishSubscribeActor(sender) ! EagerPush(MYSELF, topic, mid, msg, hop + 1)
      }

    case Publish(topic, message) =>
      val mid = generateID(topic+message)
      if (topics.contains(topic) && !delivered.contains(mid)) {
        delivered(mid) = (topic, mid, message, 0)
        applicationActor ! PSDeliver(topic, message) // Trigger PSDeliver(topic, message) indication
      }
      neighbours.foreach(p => remotePublishSubscribeActor(p) ! EagerPush(MYSELF, topic, mid, message, 1))

    case Subscribe(topic) =>
      topics = topics + topic
      neighbours.foreach(p => remotePublishSubscribeActor(p) ! TopicUpdate(MYSELF, topics))

    case Unsubscribe(topic) =>
      topics = topics - topic
      neighbours.foreach(p => remotePublishSubscribeActor(p) ! TopicUpdate(MYSELF, topics))

    case TopicUpdate(sender, topics) =>
      neighboursTopics(sender) = topics

  }

  private def mergeNeighbours(n: Set[String]): Set[String] = {
    var newNeighbours: Set[String] = Set()
    neighbours.foreach(p => {
      if (!n.contains(p)) {
        neighbours= neighbours - p
        neighboursTopics = neighboursTopics - p
      }
    })
    n.foreach(p => {
      if (!neighbours.contains(p)) {
        newNeighbours = newNeighbours + p
        neighbours= neighbours + p
        neighboursTopics(p) = Set()
      }
    })
    return newNeighbours
  }

  private def announceTopics(n: Set[String]): Unit = {
    if (topics.nonEmpty) {
      n.foreach(p => remotePublishSubscribeActor(p) ! TopicUpdate(MYSELF, topics))
    }
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

  // MD5 implementation from the internet ¯\_(ツ)_/¯
  def generateID(s: String): String = {
    import java.security.MessageDigest
    import java.math.BigInteger
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    return hashedString
  }

  private def remotePublishSubscribeActor(id: String) : ActorSelection = {
    context.actorSelection(s"akka.tcp://${Global.SYSTEM_NAME}@$id/user/${Global.PUBLISH_SUBSCRIBE_ACTOR_NAME}")
  }

  private def hyParViewActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://${Global.SYSTEM_NAME}@$MYSELF/user/${Global.HYPARVIEW_ACTOR_NAME}")
  }

  private def applicationActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://${Global.SYSTEM_NAME}@$MYSELF/user/${Global.APPLICATION_ACTOR_NAME}")
  }
}
