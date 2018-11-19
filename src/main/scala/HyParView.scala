import akka.actor.{Actor, ActorSelection, Props, Timers}
import scala.math._
import scala.concurrent.duration._

import Global._

object HyParView {
  // Constructor
  def props(ip: String, port: Int, contact: String, nNodes: Int): Props =
    Props(new HyParView(ip, port, contact, nNodes))

  // Messages
  final case class Join(newNode: String)
  final case class ForwardJoin(sender: String, newNode: String, ttl: Int)
  final case class Connect(sender: String)
  final case class Disconnect(sender: String)
  final case class Heartbeat(sender: String)
  final case class NeighbourRequest(sender: String, urgent: Boolean)
  final case class NeighbourReply(sender: String, accepted: Boolean)
  final case class NeighbourRequestTimeout(peer: String)
  final case class ShuffleRequest(sender: String, issuer: String, sample: Set[String], ttl: Int)
  final case class ShuffleReply(requestSample: Set[String], replySample: Set[String])
  final case object GetNeighbours
  final case object NeighbourRequestTimer // non-periodic
  final case object HeartbeatTimer  // periodic
  final case object ShuffleTimer  // periodic
  final case object RequestNeighbourshipTimer // periodic
  // Application Messages
  final case object LogViews
}

// TODO: Check for missing neighbours indication (?) -> some neighbours don't get entry on PS neighbourTopics (rare)
// TODO: Check for timer colision -> passiveView ""keeps"" dead peers even though timeout executes -> shuffle's fault?
class HyParView(ip: String, port: Int, contact: String, nNodes: Int) extends Actor with Timers {
  import HyParView._
  import PublishSubscribe._

  // Constants
  val MYSELF: String = s"$ip:$port"
  val FANOUT: Int = log(nNodes.toDouble).toInt
  val ACTIVE_VIEW_MAX_SIZE: Int = FANOUT + 1
  val PASSIVE_VIEW_MAX_SIZE: Int = ACTIVE_VIEW_MAX_SIZE * 3 // K = 3
  val ARWL: Int = FANOUT  // Active Random Walk Length
  val PRWL: Int = ARWL/2  // Active Random Walk Length
  val Ka: Int = 4 // Max # of nodes to include from activeView on a Shuffle sample
  val Kp: Int = 3 // Max # of nodes to include from passiveView on a Shuffle sample
  val MAX_FAILED_HEARTBEATS: Int = 3  // Maximum number of sequent heartbeats that can be missed

  // State
  var activeView: Set[String] = Set()
  var passiveView: Set[String] = Set()
  var heartbeatMonitor: Map[String, (Boolean, Int)] = Map()

  // Timers
  timers.startPeriodicTimer(HeartbeatTimer, HeartbeatTimer, 15 seconds)
  timers.startPeriodicTimer(ShuffleTimer, ShuffleTimer, 15 seconds)
  timers.startPeriodicTimer(RequestNeighbourshipTimer, RequestNeighbourshipTimer, 15 seconds)

  // Init
  override def preStart(): Unit = {
    super.preStart()
    if (contact != null) {
      addNodeActiveView(contact)
      remoteHyParViewActor(contact) ! Join(MYSELF)
    }
  }

  // Receive
  override def receive: Receive = {
    case GetNeighbours => triggerNeighboursIndication()
    case Join(newNode) => handleJoin(newNode)
    case ForwardJoin(sender, newNode, ttl) => handleForwardJoin(sender, newNode, ttl)
    case Connect(sender) => handleConnect(sender)
    case Disconnect(sender) => handleDisconnect(sender)
    case Heartbeat(sender) => handleHeartbeat(sender)
    case HeartbeatTimer => handleHeartbeatTimer()
    case NeighbourRequest(sender, urgent) => handleNeighbourRequest(sender, urgent)
    case NeighbourReply(sender, accepted) => handleNeighbourReply(sender, accepted)
    case NeighbourRequestTimeout(peer) => handleNeighbourRequestTimeout(peer)
    case ShuffleTimer => handleShuffleTimer()
    case ShuffleRequest(sender, issuer, sample, ttl) => handleShuffleRequest(sender, issuer, sample, ttl)
    case ShuffleReply(requestSample, replySample) => handleShuffleReply(requestSample, replySample)
    case RequestNeighbourshipTimer => handleRequestNeighbourshipTimer()
    // Application Messages
    case LogViews => handleLogViews()
  }

  /* ---------------------------- Handlers ---------------------------- */

  private def triggerNeighboursIndication(): Unit = {
    publishSubscribeActor ! Neighbours(activeView) // Trigger neighbours(n) indication
  }

  private def handleJoin(newNode: String): Unit = {
    addNodeActiveView(newNode)
    activeView.foreach(node => {
      if (!node.equals(newNode)) {
        remoteHyParViewActor(node) ! ForwardJoin(MYSELF, newNode, ARWL)
      }
    })
  }

  private def handleForwardJoin(sender: String, newNode: String, ttl: Int): Unit = {
    if (ttl == 0 || activeView.size == 1) {
      if (addNodeActiveView(newNode)) { // Only sends Connect message if addNodeActiveView succeeds
        passiveView = passiveView - newNode // Removes if it's there
        remoteHyParViewActor(newNode) ! Connect(MYSELF)
      }
    } else {
      if (ttl == PRWL) {
        addNodePassiveView(newNode)
      }
      // If no node is found matching the requisites, then we send the message to no one (empty String)
      val node = activeView.find(n => !n.equals(sender) && !n.equals(newNode)) getOrElse ""
      remoteHyParViewActor(node) ! ForwardJoin(MYSELF, newNode, ttl-1)
    }
  }

  private def handleConnect(sender: String): Unit = {
    passiveView = passiveView - sender  // Removes if it's there
    addNodeActiveView(sender)
  }

  private def handleDisconnect(sender: String): Unit = {
    if (activeView.contains(sender)) {
      removeNodeActiveView(sender)
      addNodePassiveView(sender)
    }
  }

  private def handleHeartbeat(sender: String): Unit = {
    if (activeView.contains(sender)) {
      heartbeatMonitor = heartbeatMonitor + (sender -> (false, 0))  // Vitals line is not flat ; reset counter
    }
  }

  private def handleHeartbeatTimer(): Unit = {
    activeView.foreach(p => {
      // 1. Ping the peer so it knows we're alive
      remoteHyParViewActor(p) ! Heartbeat(MYSELF)

      // 2. 'Decide' if our peer might still be alive or not, and potentially act upon that (level of) certainty
      val (flatline, prevCounter) = heartbeatMonitor(p)
      val counter = prevCounter + 1
      if (!flatline) {  // Received heartbeat
        heartbeatMonitor = heartbeatMonitor + (p -> (true, 0))  // Flatten to start the new round
      } else if (counter == MAX_FAILED_HEARTBEATS) {
        patchActiveView(p)
      } else {
        heartbeatMonitor = heartbeatMonitor + (p -> (true, counter))  // Flatten to start the new round, incr counter
      }
    })
  }

  private def handleNeighbourRequest(sender: String, urgent: Boolean): Unit = {
    if (urgent || activeView.size < ACTIVE_VIEW_MAX_SIZE) {
      passiveView = passiveView - sender  // Removes if it's there
      addNodeActiveView(sender)
      remoteHyParViewActor(sender) ! NeighbourReply(MYSELF, accepted = true)
    } else {
      remoteHyParViewActor(sender) ! NeighbourReply(MYSELF, accepted = false)
    }
  }

  private def handleNeighbourReply(sender: String, accepted: Boolean): Unit = {
    // Stop the timeout timer
    val patchingActiveView = timers.isTimerActive(NeighbourRequestTimer)
    if (patchingActiveView) {
      timers.cancel(NeighbourRequestTimer)
    }

    if (accepted) {
      handleConnect(sender)
    } else {
      // We don't send the request to the previous guy
      if (patchingActiveView) {
        sendRandomNeighbourRequest(passiveView diff Set(sender))
      }
    }
  }

  private def handleNeighbourRequestTimeout(peer: String): Unit = {
    println(s"[HYPARVIEW] NEIGHBOUR REQUEST TIMEOUT: $peer")
    passiveView = passiveView - peer  // Removes if it's there
    sendRandomNeighbourRequest()
  }

  private def handleShuffleTimer(): Unit = {
    if (activeView.nonEmpty) {
      // 1. Create the sample set
      val activeViewSample = getRandomSubset(activeView, Ka)
      val passiveViewSample = getRandomSubset(passiveView, Kp)
      val sample = activeViewSample union passiveViewSample union Set(MYSELF)

      // 2. Select a peer at random and set it the Shuffle request containing the sample
      val randomPeer = getRandomElement(activeView)
      remoteHyParViewActor(randomPeer) ! ShuffleRequest(MYSELF, MYSELF, sample, ARWL)
    }
  }

  private def handleShuffleRequest(sender: String, issuer: String, sample: Set[String], ttl: Int): Unit = {
    val currentTTL = ttl - 1
    if (currentTTL > 0 && activeView.size > 1) {
      val randomPeer = getRandomElement(activeView - sender)
      remoteHyParViewActor(randomPeer) ! ShuffleRequest(MYSELF, issuer, sample, currentTTL)
    } else {
      val replySample = getRandomSubset(passiveView, sample.size)
      remoteHyParViewActor(issuer) ! ShuffleReply(sample, replySample)
      integrateSample(sample, replySample)
    }
  }

  private def handleShuffleReply(requestSample: Set[String], replySample: Set[String]): Unit = {
    integrateSample(replySample, requestSample)
  }

  // Periodic task to maximize the number of peer in our activeView
  private def handleRequestNeighbourshipTimer(): Unit = {
    if (activeView.size < ACTIVE_VIEW_MAX_SIZE) {
      val freeSlots = ACTIVE_VIEW_MAX_SIZE - activeView.size
      val passiveSample = getRandomSubset(passiveView, freeSlots)
      passiveSample.foreach(p => remoteHyParViewActor(p) ! NeighbourRequest(MYSELF, activeView.isEmpty))
    }
  }

  /* ---------------------------- Procedures and Utils ---------------------------- */

  private def addNodeActiveView(node: String): Boolean = {
    if (!node.equals(MYSELF) && !activeView.contains(node)) {
      if (activeView.size == ACTIVE_VIEW_MAX_SIZE) {
        dropRandomElementFromActiveView()
      }
      activeView = activeView + node
      // flatline = false because we consider node to be alive at this point
      heartbeatMonitor = heartbeatMonitor + (node -> (false, 0))

      triggerNeighboursIndication()
      return true
    }
    return false
  }

  private def removeNodeActiveView(node: String): Unit = {
    activeView = activeView - node
    triggerNeighboursIndication()
  }

  private def addNodePassiveView(node: String): Unit = {
    if (!node.equals(MYSELF) && !activeView.contains(node) && !passiveView.contains(node)) {
      if (passiveView.size == PASSIVE_VIEW_MAX_SIZE) {
        val nodeToExclude = getRandomElement(passiveView)
        passiveView = passiveView - nodeToExclude
      }
      passiveView = passiveView + node
    }
  }

  private def dropRandomElementFromActiveView(): Unit = {
    val node = getRandomElement(activeView)
    activeView = activeView - node  // Don't use removeNodeActiveView here, we don't need the neighbours indication
    addNodePassiveView(node)
    remoteHyParViewActor(node) ! Disconnect(MYSELF)
  }

  private def patchActiveView(deadPeer: String): Unit = {
    // Remove 'dead' peer from activeView
    removeNodeActiveView(deadPeer)
    remoteHyParViewActor(deadPeer) ! Disconnect(MYSELF) // Just to be sure

    sendRandomNeighbourRequest()
  }

  private def sendRandomNeighbourRequest(passiveSet: Set[String] = passiveView): Unit = {
    if (passiveSet.nonEmpty && activeView.size < ACTIVE_VIEW_MAX_SIZE) {
      // Pick random node from passiveView and try to establish a connection with it
      val node = getRandomElement(passiveSet)
      remoteHyParViewActor(node) ! NeighbourRequest(MYSELF, activeView.isEmpty)

      // Start a timeout timer for the request
      timers.startSingleTimer(NeighbourRequestTimer, NeighbourRequestTimeout(node), 15 seconds)
    }
  }

  private def integrateSample(receivedSample: Set[String], sentSample: Set[String]): Unit = {
    val newNodes = ((receivedSample diff Set(MYSELF)) diff activeView) diff passiveView
    var auxSet = newNodes

    // Fill the passiveView until it's full or there's no new nodes to integrate
    while (passiveView.size < PASSIVE_VIEW_MAX_SIZE && auxSet.nonEmpty) {
      val node = getRandomElement(auxSet)
      auxSet = auxSet - node
      addNodePassiveView(node)
    }

    // If we ran out of space in the passiveView before we ran out of new nodes, then:
    if (auxSet.nonEmpty) {
      var dropSet = passiveView intersect sentSample
      while (auxSet.nonEmpty) {
        // 1. Drop node from passiveView
        val nodeToDrop = if (dropSet.nonEmpty) getRandomElement(dropSet) else getRandomElement(passiveView)
        dropSet = dropSet - nodeToDrop  // Removes if it's there
        passiveView = passiveView - nodeToDrop

        // 2. Add new node to passiveView
        val nodeToAdd = getRandomElement(auxSet)
        auxSet = auxSet - nodeToAdd
        addNodePassiveView(nodeToAdd)
      }
    }
  }

  /* ---------------------------- Application Handlers ---------------------------- */

  private def handleLogViews(): Unit = {
    println(s"###### Active View: ${activeView.size} elements ######")
    activeView.foreach(println)
    println(s"###### Passive View: ${passiveView.size} elements ######")
    passiveView.foreach(println)
    println()
  }

  /* ---------------------------- Actors ---------------------------- */

  private def remoteHyParViewActor(id: String) : ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$id/user/$HYPARVIEW_ACTOR_NAME")
  }

  private def publishSubscribeActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://$SYSTEM_NAME@$MYSELF/user/$PUBLISH_SUBSCRIBE_ACTOR_NAME")
  }

}
