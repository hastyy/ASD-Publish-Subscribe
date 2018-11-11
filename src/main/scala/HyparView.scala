import akka.actor.{Actor, ActorSelection, Props, Timers}
import scala.collection.mutable.{HashMap, Map}
import scala.collection.Set
import scala.math._
import scala.util.Random
import scala.concurrent.duration._

object HyparView {
  // Constructor
  def props(ip: String, port: Int, contact: String, nNodes: Int): Props =
    Props(new HyparView(ip, port, contact, nNodes))

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

class HyparView(ip: String, port: Int, contact: String, nNodes: Int) extends Actor with Timers {
  import HyparView._
  // import PublishSubscribe._

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
      getReference(contact) ! Join(MYSELF)
    }
  }

  // Receive
  override def receive: Receive = {

    case Join(newNode) =>
      println("Received Join from " + newNode)
      addNodeActiveView(newNode)
      activeView.keySet.foreach(node => {
        if (node != newNode) {
          getReference(node) ! ForwardJoin(MYSELF, newNode, ARWL)
        }
      })
      println("\n\n\n\n\n")

    case ForwardJoin(sender, newNode, ttl) =>
      println("Received ForwardJoin from " + sender + " : New node is " + newNode)
      if (ttl == 0 || activeView.size == 1) {
        println("Adding the node to active view.")
        if (addNodeActiveView(newNode)) {
          passiveView = passiveView - newNode // Added
          // Only sends Connect message if addNodeActiveView succeeds
          getReference(newNode) ! Connect(MYSELF)
        }
      } else {
        if (ttl == PRWL) {
          println("Adding the node to passive view.")
          addNodePassiveView(newNode)
        }
        // If no node is found matching the requisites, then we send the message to no one (empty String)
        val node = activeView.keys.find(n => n != sender && n != newNode) getOrElse ""
        println("Sending ForwardJoin message to " + node)
        getReference(node) ! ForwardJoin(MYSELF, newNode, ttl-1)
      }
      println("\n\n\n\n\n")

    case Connect(sender) =>
      println("Received Connect message from " + sender)
      passiveView = passiveView - sender
      addNodeActiveView(sender)
      println("\n\n\n\n\n")

    case Disconnect(sender) =>
      println("Received Disconnect message from " + sender + " - Removing it from active view if it's there.")
      if (activeView.keySet.contains(sender)) {
        removeNodeActiveView(sender)
        addNodePassiveView(sender)
      }
      println("\n\n\n\n\n")

    case HeartbeatTimer =>
      activeView.keySet.foreach(node => {
        // 1. Ping the peer so it knows we're alive
        getReference(node) ! Heartbeat(MYSELF)

        // 'Decide' if our peer is alive or not, and act upon that (level of) certainty
        val (flatline, counter) = activeView(node)
        if (!flatline) {
          activeView(node) = (true, 0)
        } else if ((counter + 1) == 3) {
          println(">>>> We think " + node + " has died. Replacing it in the active view...")
          println("\n\n\n\n\n")
          patchActiveView(node)
        } else {
          println(">>>> " + node + " missed a heartbeat. Counter: " + (counter + 1))
          println("\n\n\n\n\n")
          activeView(node) = (true, counter + 1)
        }
      })

    case Heartbeat(sender) =>
      if (activeView.contains(sender)) {
        // Received an heartbeat, vitals line IS NOT flat
        // Reset counter
        // println(">>>> Received heartbeat from " + sender)
        activeView(sender) = (false, 0)
      }

    case RequestNeighbourshipTimer =>
      /*println("VIEWS")
      printActiveView()
      printPassiveView()
      println("CLOSE VIEWS")
      println("\n\n\n\n\n")*/

      val requestAmount = min(ACTIVE_VIEW_MAX_SIZE - activeView.size, passiveView.size)
      if(requestAmount > 0) {
        // printPassiveView()
        //println(">>>> Slots left to fill in Active View: " + requestAmount)
        var auxSet: Set[String] = passiveView
        for (i <- 1 to requestAmount) {
          val randomPeer = auxSet.toVector(new Random().nextInt(auxSet.size))
          //println(">>>> Will send neighbor request to " + randomPeer)
          auxSet = auxSet - randomPeer
          getReference(randomPeer) ! RequestNeighbourship(MYSELF)
        }
      }

    case RequestNeighbourship(sender) =>
      if(activeView.size < ACTIVE_VIEW_MAX_SIZE) {
        println(">>>> Received a neighbour request from " + sender + "and have free slots.")
        if(addNodeActiveView(sender)) {
          println("Will accept request from " + sender)
          passiveView = passiveView - sender
          getReference(sender) ! Connect(MYSELF)
        }
        println("\n\n\n\n\n")
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
        getReference(randomPeer) ! Shuffle(MYSELF, MYSELF, sample, ARWL)
      }

    case Shuffle(sender, issuer, sample, ttl) =>
      val currentTTL = ttl - 1
      if (currentTTL == 0 || activeView.size == 1) {
        // 1. Prepare the sample for the ShuffleReply
        var replySample: Set[String] = Set(MYSELF)  // TODO: Should include MYSELF ?
        if (passiveView.nonEmpty) {
          var auxSet: Set[String] = passiveView
          for (i <- 1 to min(passiveView.size, sample.size)) {
            val randomPeer = auxSet.toVector(new Random().nextInt(auxSet.size))
            auxSet = auxSet - randomPeer
            replySample = replySample + randomPeer
          }
        }

        // 2. Send the ShuffleReply
        getReference(issuer) ! ShuffleReply(replySample, sample)

        // 3. Integrate sample elements
        integrateSample(sample, replySample)
      } else {
        val randomPeer = (activeView.keySet - sender).toVector(new Random().nextInt(activeView.size - 1))
        getReference(randomPeer) ! Shuffle(MYSELF, issuer, sample, currentTTL)
      }

    case ShuffleReply(newSample, sentSample) =>
      integrateSample(newSample, sentSample)

    case GetNeighbours =>
      getPublishSubscribeReference() ! PublishSubscribe.Neighbours(activeView.keySet) //Trigger neighbours(n) indication

  }

  private def addNodeActiveView(node: String): Boolean = {
    if (!node.equals(MYSELF) && !activeView.contains(node)) {
      if (activeView.size == ACTIVE_VIEW_MAX_SIZE) {
        dropRandomElementFromActiveView()
      }
      activeView(node) = (false, 0) // flatline = false because we consider node to be alive at this point
      getPublishSubscribeReference() ! PublishSubscribe.Neighbours(activeView.keySet) //Trigger neighbours(n) indication
      return true
    }
    return false
  }

  private def removeNodeActiveView(node: String): Unit = {
    activeView = activeView - node
    getPublishSubscribeReference() ! PublishSubscribe.Neighbours(activeView.keySet) // Trigger neighbours(n) indication
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
    getReference(node) ! Disconnect(MYSELF)
  }

  // TODO: This method is wrong. Must be fixed according to:
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
    getReference(deadPeer) ! Disconnect(MYSELF)

    // 2. Check if activeView is empty: if not, we don't need to execute the code below
    if (activeView.nonEmpty) {
      return
    }

    // 3. (Aggressively) Promote random node from passiveView to activeView
    if (passiveView.isEmpty) {
      // We can't patch the activeView
      return
    }
    val node = passiveView.toVector(new Random().nextInt(passiveView.size))
    addNodeActiveView(node)
    getReference(node) ! Connect(MYSELF)
    passiveView = passiveView - node
    printActiveView()
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
      var dropSet: Set[String] = sentSample union (passiveView diff newNodes) // TODO: Might only want sentSample
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

  private def getReference(id: String) : ActorSelection = {
    context.actorSelection("akka.tcp://"+ Global.SYSTEM_NAME +"@" + id + "/user/" + Global.HYPARVIEW_ACTOR_NAME)
  }

  private def getPublishSubscribeReference(): ActorSelection = {
    context.actorSelection("akka.tcp://"+ Global.SYSTEM_NAME +"@" + MYSELF + "/user/" + Global.PUBLISH_SUBSCRIBE_ACTOR_NAME)
  }

  private def printActiveView(): Unit = {
    println("\n ############ ACTIVE VIEW WAS UPDATED ############")
    activeView.keySet.foreach(node => println(node))
  }

  private def printPassiveView(): Unit = {
    println("\n ############ PASSIVE VIEW WAS UPDATED ############")
    passiveView.foreach(node => println(node))
  }

  private def isNetworkCorrect: Boolean = {
    val inter = activeView.keySet intersect passiveView
    return inter.isEmpty
  }
}
