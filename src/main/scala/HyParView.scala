import akka.actor.{Actor, ActorSelection, Props, Timers}
import scala.collection.mutable.{HashMap, Map}
import scala.math._
import scala.util.Random
import scala.concurrent.duration._

object HyParView {
  // Constructor
  def props(ip: String, port: Int, contact: String, nNodes: Int): Props =
    Props(new HyParView(ip, port, contact, nNodes))

  // Messages
  final case class Join(id: String)
  final case class ForwardJoin(sender: String, newNode: String, ttl: Int)
  final case class Connect(sender: String)
  final case class Disconnect(sender: String)
  final case class Heartbeat(sender: String)
  final case class RequestNeighbourship(sender: String)
  final case class Shuffle(sender: String, issuer: String, sample: Set[String], ttl: Int)
  final case class ShuffleReply(newSample: Set[String], sentSample: Set[String])
  final case object GetNeighbours
  final case object HeartbeatTimer
  final case object RequestNeighbourshipTimer
  final case object ShuffleTimer
}

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

  // State
  var activeView: Map[String, (Boolean, Int)] = HashMap() // Maps a neighbour node to a (flatline, counter) tuple
  var passiveView: Set[String] = Set()  // Set of nodes

  // Timers
  timers.startPeriodicTimer(HeartbeatTimer, HeartbeatTimer, 15 seconds)
  timers.startPeriodicTimer(RequestNeighbourshipTimer, RequestNeighbourshipTimer, 15 seconds)
  timers.startPeriodicTimer(ShuffleTimer, ShuffleTimer, 15 seconds)

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

    case Join(newNode) =>
      addNodeActiveView(newNode)
      activeView.keySet.foreach(node => {
        if (!node.equals(newNode)) {
          remoteHyParViewActor(node) ! ForwardJoin(MYSELF, newNode, ARWL)
        }
      })

    case ForwardJoin(sender, newNode, ttl) =>
      if (ttl == 0 || activeView.size == 1) {
        if (addNodeActiveView(newNode)) {
          // Only sends Connect message if addNodeActiveView succeeds
          passiveView = passiveView - newNode // Removes if it's there
          remoteHyParViewActor(newNode) ! Connect(MYSELF)
        }
      } else {
        if (ttl == PRWL) {
          addNodePassiveView(newNode)
        }
        // If no node is found matching the requisites, then we send the message to no one (empty String)
        val node = activeView.keys.find(n => !n.equals(sender) && !n.equals(newNode)) getOrElse ""
        remoteHyParViewActor(node) ! ForwardJoin(MYSELF, newNode, ttl-1)
      }

    case Connect(sender) =>
      passiveView = passiveView - sender
      addNodeActiveView(sender)

    case Disconnect(sender) =>
      if (activeView.keySet.contains(sender)) {
        removeNodeActiveView(sender)
        addNodePassiveView(sender)
      }

    case HeartbeatTimer =>
      activeView.keySet.foreach(node => {
        // 1. Ping the peer so it knows we're alive
        remoteHyParViewActor(node) ! Heartbeat(MYSELF)

        // 'Decide' if our peer is alive or not, and act upon that (level of) certainty
        val (flatline, counter) = activeView(node)
        if (!flatline) {
          activeView(node) = (true, 0)
        } else if ((counter + 1) == 3) {
          patchActiveView(node)
        } else {
          activeView(node) = (true, counter + 1)
        }
      })

    case Heartbeat(sender) =>
      if (activeView.contains(sender)) {
        activeView(sender) = (false, 0) // Vitals line is not flat ; reset counter
      }

    case RequestNeighbourshipTimer =>
      val requestAmount = min(ACTIVE_VIEW_MAX_SIZE - activeView.size, passiveView.size)
      if(requestAmount > 0) {
        var auxSet: Set[String] = passiveView
        for (i <- 1 to requestAmount) {
          val randomPeer = auxSet.toVector(new Random().nextInt(auxSet.size))
          auxSet = auxSet - randomPeer
          remoteHyParViewActor(randomPeer) ! RequestNeighbourship(MYSELF)
        }
      }

    case RequestNeighbourship(sender) =>
      if(activeView.size < ACTIVE_VIEW_MAX_SIZE) {
        if(addNodeActiveView(sender)) {
          passiveView = passiveView - sender
          remoteHyParViewActor(sender) ! Connect(MYSELF)
        }
      }

    case ShuffleTimer =>
      // 1. Create the sample set
      var sample: Set[String] = Set(MYSELF) union activeView.keySet
      if (passiveView.nonEmpty) {
        var auxSet: Set[String] = passiveView
        for (i <- 1 to min(passiveView.size, PASSIVE_VIEW_MAX_SIZE - sample.size)) {
          val randomPeer = auxSet.toVector(new Random().nextInt(auxSet.size))
          auxSet = auxSet - randomPeer
          sample = sample + randomPeer
        }
      }

      // 2. Select a peer at random and set it the Shuffle request containing the sample
      if (activeView.nonEmpty) {
        val randomPeer = activeView.keySet.toVector(new Random().nextInt(activeView.size))
        remoteHyParViewActor(randomPeer) ! Shuffle(MYSELF, MYSELF, sample, ARWL)
      }

    case Shuffle(sender, issuer, sample, ttl) =>
      val currentTTL = ttl - 1
      if (currentTTL == 0 || activeView.size == 1) {
        // 1. Prepare the sample for the ShuffleReply
        var replySample: Set[String] = Set(MYSELF)
        if (passiveView.nonEmpty) {
          var auxSet: Set[String] = passiveView
          for (i <- 1 to min(passiveView.size, sample.size)) {
            val randomPeer = auxSet.toVector(new Random().nextInt(auxSet.size))
            auxSet = auxSet - randomPeer
            replySample = replySample + randomPeer
          }
        }

        // 2. Send the ShuffleReply
        remoteHyParViewActor(issuer) ! ShuffleReply(replySample, sample)

        // 3. Integrate sample elements
        integrateSample(sample, replySample)
      } else {
        val randomPeer = (activeView.keySet - sender).toVector(new Random().nextInt(activeView.size - 1))
        remoteHyParViewActor(randomPeer) ! Shuffle(MYSELF, issuer, sample, currentTTL)
      }

    case ShuffleReply(newSample, sentSample) =>
      integrateSample(newSample, sentSample)

    case GetNeighbours =>
      triggerNeighboursIndication()

  }

  private def addNodeActiveView(node: String): Boolean = {
    if (!node.equals(MYSELF) && !activeView.contains(node)) {
      if (activeView.size == ACTIVE_VIEW_MAX_SIZE) {
        dropRandomElementFromActiveView()
      }
      activeView(node) = (false, 0) // flatline = false because we consider node to be alive at this point
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
    if (!node.equals(MYSELF) && !activeView.keySet.contains(node) && !passiveView.contains(node)) {
      if (passiveView.size == PASSIVE_VIEW_MAX_SIZE) {
        val nodeToExclude = passiveView.toVector(new Random().nextInt(PASSIVE_VIEW_MAX_SIZE))
        passiveView = passiveView - nodeToExclude
      }
      passiveView = passiveView + node
    }
  }

  private def dropRandomElementFromActiveView(): Unit = {
    val node = activeView.keySet.toVector(new Random().nextInt(activeView.size))
    activeView = activeView - node  // Don't use removeNodeActiveView here, we don't need the neighbours indication
    addNodePassiveView(node)
    remoteHyParViewActor(node) ! Disconnect(MYSELF)
  }

  // FIXME: This method is wrong. Must be fixed according to:
  /**
    * When a node p suspects that one of the nodes present in its active view has failed (by either disconnecting or
    * blocking), it selects a random node q from its passive view and attempts to establish a TCP connection with q.
    * If the connection fails to establish, node q is considered failed and removed from p’s passive view;
    * another node q' is selected at random and a new attempt is made. The procedure is repeated until a connection is
    * established with success.
    */
  private def patchActiveView(deadPeer: String): Unit = {
    // 1. Remove 'dead' peer from activeView
    removeNodeActiveView(deadPeer)
    remoteHyParViewActor(deadPeer) ! Disconnect(MYSELF)

    // 2. Check if activeView is empty: if not, we don't need to execute the code below
    if (activeView.nonEmpty) {
      return
    }

    // 3. (Aggressively) Promote random node from passiveView to activeView
    if (passiveView.isEmpty) {
      return  // We can't patch the activeView
    }
    val node = passiveView.toVector(new Random().nextInt(passiveView.size))
    addNodeActiveView(node)
    remoteHyParViewActor(node) ! Connect(MYSELF)
    passiveView = passiveView - node
  }

  private def integrateSample(receivedSample: Set[String], sentSample: Set[String]): Unit = {
    val newNodes = ((receivedSample diff activeView.keySet) diff passiveView) diff Set(MYSELF)
    var auxSet = newNodes
    while (passiveView.size < PASSIVE_VIEW_MAX_SIZE && auxSet.nonEmpty) {
      val node = auxSet.toVector(new Random().nextInt(auxSet.size))
      auxSet = auxSet - node
      addNodePassiveView(node)
    }

    // If the code below executes, it means that we ran out of space in the active view before we ran out of new nodes
    if (auxSet.nonEmpty) {
      var dropSet: Set[String] = sentSample union (passiveView diff newNodes)
      while (auxSet.nonEmpty) {
        // 1. Remove from passiveView
        val nodeToDrop =
          if (dropSet.nonEmpty) dropSet.toVector(new Random().nextInt(dropSet.size))
          else passiveView.toVector(new Random().nextInt(passiveView.size))
        passiveView = passiveView - nodeToDrop

        // 2. Add new node to passiveView
        val nodeToAdd = auxSet.toVector(new Random().nextInt(auxSet.size))
        auxSet = auxSet - nodeToAdd
        addNodePassiveView(nodeToAdd)
      }
    }
  }

  private def triggerNeighboursIndication(): Unit = {
    var activeViewClone: Set[String] = Set()
    activeView.keySet.foreach(n => activeViewClone = activeViewClone + n)
    publishSubscribeActor ! Neighbours(activeViewClone) // Trigger neighbours(n) indication
  }

  private def remoteHyParViewActor(id: String) : ActorSelection = {
    context.actorSelection(s"akka.tcp://${Global.SYSTEM_NAME}@$id/user/${Global.HYPARVIEW_ACTOR_NAME}")
  }

  private def publishSubscribeActor: ActorSelection = {
    context.actorSelection(s"akka.tcp://${Global.SYSTEM_NAME}@$MYSELF/user/${Global.PUBLISH_SUBSCRIBE_ACTOR_NAME}")
  }
}
