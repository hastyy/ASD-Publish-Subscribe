import akka.actor.{Actor, ActorSelection, Props, Timers}
import scala.collection.mutable.{HashMap, Map}
import scala.math._
import scala.util.Random
import scala.concurrent.duration._

object HyparView {
  // Constructor
  def props(ip: String, port: Int, contact: String, nNodes: Int): Props =
    Props(new HyparView(ip, port, contact, nNodes))

  // Messages
  final case class Join(id: String)
  final case class ForwardJoin(newNode: String, ttl: Int, sender: String)
  final case class AcceptNeighbour(sender: String)  // TODO: rename to Connect (?)
  final case class Disconnect(sender: String)
  final case class HeartbeatSignal(sender: String)
  final case object GetNeighbours // TODO
  final case object Heartbeat
}

// TODO: Shuffle passive view - cyclon without age (amostra = myself + some of active + some of passive) - mal recebemos a amostra deitar fora da amostra o que já está na activa e passiva
// TODO: passiveView = K * activeView ? (enforce? how to get connections?)
class HyparView(ip: String, port: Int, contact: String, nNodes: Int) extends Actor with Timers {
  import HyparView._

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
  timers.startPeriodicTimer(Heartbeat, Heartbeat, 5 seconds)

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
          getReference(node) ! ForwardJoin(newNode, ARWL, MYSELF)
        }
      })

    case ForwardJoin(newNode, ttl, sender) =>
      println("Received ForwardJoin from " + sender + " : New node is " + newNode)
      if (ttl == 0 || activeView.size == 1) {
        println("Adding the node to active view.")
        if (addNodeActiveView(newNode)) {
          // Only sends AcceptNeighbour message if addNodeActiveView succeeds
          getReference(newNode) ! AcceptNeighbour(MYSELF)
        }
      } else {
        if (ttl == PRWL) {
          println("Adding the node to passive view.")
          addNodePassiveView(newNode)
        }
        // If no node is found matching the requisites, then we send the message to no one (empty String)
        val node = activeView.keys.find(n => n != sender && n != newNode) getOrElse ""
        println("Sending ForwardJoin message to " + node)
        getReference(node) ! ForwardJoin(newNode, ttl-1, MYSELF)
      }

    case AcceptNeighbour(sender) =>
      println("Received AcceptNeighbour message from " + sender)
      passiveView = passiveView - sender
      addNodeActiveView(sender)

    case Disconnect(sender) =>
      println("Received Disconnect message from " + sender + " - Removing it from active view if it's there.")
      if (activeView.keySet.contains(sender)) {
        activeView = activeView - sender
        addNodePassiveView(sender)
      }

    case Heartbeat =>
      activeView.keySet.foreach(node => {
        // 1. Ping the peer so it knows we're alive
        getReference(node) ! HeartbeatSignal(MYSELF)

        // 'Decide' if our peer is alive or not, and act upon that (level of) certainty
        val (flatline, counter) = activeView(node)
        if (!flatline) {
          println(">>>> " + node + " is healthy AF.")
          activeView(node) = (true, 0)
        } else if ((counter + 1) == 3) {
          println(">>>> We think " + node + " has died. Replacing it in the active view...")
          patchActiveView(node)
        } else {
          println(">>>> " + node + " missed a heartbeat. Counter: " + (counter + 1))
          activeView(node) = (true, counter + 1)
        }
      })

    case HeartbeatSignal(sender) =>
      if (activeView.contains(sender)) {
        // Received an heartbeat, vitals line IS NOT flat
        // Reset counter
        println(">>>> Received heartbeat from " + sender)
        activeView(sender) = (false, 0)
      }

  }

  private def addNodeActiveView(node: String): Boolean = {
    if (!node.equals(MYSELF) && !activeView.contains(node)) {
      if (activeView.size == ACTIVE_VIEW_MAX_SIZE) {
        dropRandomElementFromActiveView()
      }
      activeView(node) = (false, 0) // flatline = false because we consider node is alive at this point
      printActiveView()
      return true
    }
    return false
  }

  private def addNodePassiveView(node: String): Unit = {
    if (node != MYSELF && !activeView.keySet.contains(node) && !passiveView.contains(node)) {
      if (passiveView.size == PASSIVE_VIEW_MAX_SIZE) {
        val nodeToExclude = passiveView.toVector(new Random().nextInt(PASSIVE_VIEW_MAX_SIZE))
        passiveView = passiveView - nodeToExclude
      }
      passiveView = passiveView + node
      printPassiveView()
    }
  }

  private def dropRandomElementFromActiveView(): Unit = {
    val node = activeView.keySet.toVector(new Random().nextInt(activeView.size))
    activeView = activeView - node
    addNodePassiveView(node)
    getReference(node) ! Disconnect(MYSELF)
  }

  private def patchActiveView(deadPeer: String): Unit = {
    // 1. Remove 'dead' peer from activeView
    activeView = activeView - deadPeer
    getReference(deadPeer) ! Disconnect(MYSELF)

    // 2. Check if activeView is empty: if not, we don't need to execute the code below
    if (activeView.size > 0) {
      return
    }

    // 3. (Agressively) Promote random node from passiveView to activeView
    if (passiveView.size == 0) {
      // We can't patch the activeView
      return
    }
    val node = passiveView.toVector(new Random().nextInt(passiveView.size))
    addNodeActiveView(node)
    getReference(node) ! AcceptNeighbour(MYSELF)
    passiveView = passiveView - node
    printActiveView()
  }

  private def getReference(id: String) : ActorSelection = {
    context.actorSelection("akka.tcp://"+ Global.SYSTEM_NAME +"@" + id + "/user/" + Global.HYPARVIEW_ACTOR_NAME)
  }

  private def printActiveView(): Unit = {
    println("\n ############ ACTIVE VIEW WAS UPDATED ############")
    activeView.keySet.foreach(node => println(node))
  }

  private def printPassiveView(): Unit = {
    println("\n ############ PASSIVE VIEW WAS UPDATED ############")
    passiveView.foreach(node => println(node))
  }
}


/*var patchedActiveView = false
    do {
      val node = passiveView.toVector(new Random().nextInt(passiveView.size))
      patchedActiveView = addNodeActiveView(node)
      if (patchedActiveView) {
        printActiveView()
        getReference(node) ! AcceptNeighbour(MYSELF)
        passiveView = passiveView - node
        // TODO: Trigger neighbours indication
      }
    } while (!patchedActiveView)*/
