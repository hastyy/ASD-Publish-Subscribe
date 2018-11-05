import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import scala.math._
import scala.util.Random
import scala.collection.mutable.{HashMap, ListBuffer, Map}

object HyparView {
  case class Join(id: String)
  case class Disconnect()
  case class ForwardJoin(newNode: String, ARWL: Int, myself: String)
  case class AcceptNeighbor(myself: String)
}


class HyparView(address: String, port: Int, contactAddress: String, contactPort: Int) extends Actor {
  import HyparView._

  val myself = address + ":" + port
  val systemName = Static.systemName

  // Hyparview State
  val K: Int = 3;
  val maxProcessNumber: Int = 10;
  val fanout: Int = log(maxProcessNumber.toDouble).toInt
  val activeViewMaxSize: Int = fanout.toInt + 1;
  val passiveViewMaxSize: Int = activeViewMaxSize * K;
  val ARWL: Int = fanout;
  val PRWL: Int = (ARWL/2).toInt;

  var activeView: Map[String, (Boolean, Int)] = Map()
  var passiveView: ListBuffer[String] = new ListBuffer[String]()

  val random = new Random()

  override def preStart() {
    super.preStart()
    start()
  }


  // This is called everytime a message is received by this actor
  override def receive = {

    // Receiving a join request
    case Join(newNodeId) =>
      val newNode = getReference(newNodeId)
      addNodeActiveView(newNodeId)
      activeView.foreach(element => {
        val node = element._1
        val ttl: Int = ARWL;
        if(!node.equals(newNodeId)) {
          getReference(node) ! ForwardJoin(newNodeId, ttl, myself)
        }
      })

    case Disconnect() =>

    case ForwardJoin(newNodeId, ttl, sender) =>
      println("ForwardJoin")
      if(ttl == 0 || activeView.size == 1) {
        addNodeActiveView(newNodeId)
        getReference(newNodeId) ! AcceptNeighbor(myself)
      } else {
        if(ttl == PRWL) {
          addNodePassiveView(newNodeId)
        }
        val nextTarget = activeView.find(element => {
          !element.equals(sender) && !element.equals(newNodeId)
        })
        println("Next target is " + nextTarget)
      }

    case  AcceptNeighbor(sender) =>
      println(sender + "added you to his active view, add him too!")



  }

  private def start() {
    if(contactAddress != null) {
      val target = context.actorSelection("akka.tcp://"+systemName +"@"+contactAddress+":"+contactPort+"/user/hyparview")
      target ! Join(myself)
    }
  }

  private def getReference(id: String) : ActorSelection = {
    context.actorSelection("akka.tcp://"+systemName +"@"+id+"/user/hyparview")
  }

  // Receives the id of the node and adds it to active view
  private def addNodeActiveView(node: String) {
    if(!node.equals(myself) && !activeView.contains(node)) {
      if(activeView.size >= activeViewMaxSize) {
        dropRandomElementFromActiveView()
      }
      activeView = activeView + (node -> (true, 0))
    }
  }

  private def addNodePassiveView(node: String) {
    if(!node.equals(myself) && !activeView.contains(node) && !passiveView.contains(node)) {
      if(passiveView.size >= passiveViewMaxSize) {
        var random = passiveView.head
        passiveView -= random
      }
      passiveView += node
    }
  }


  private def dropRandomElementFromActiveView() {
    if(activeView.size > 0) {
      val randomIndex = random.nextInt(activeView.size)
      val activeViewArray: Array[(String, (Boolean,Int))] = activeView.toArray
      val randomElement = activeViewArray(randomIndex)

      activeView = activeView - randomElement._1
      passiveView += randomElement._1
      getReference(randomElement._1) ! Disconnect
    }
  }

  private def test() {

    //println(passiveView.get(0))
  }


  // private def selectRandom(view: Map[String, (Boolean, Int)]): Unit = {
  //   val randomIndex = random.nextInt(view.size)
  //   val viewArray: Array[(String, (Boolean,Int))] = view.toArray
  //   viewArray(randomIndex)
  // }
}




//passiveView = passiveView + (randomElement._1 -> randomElement._2)
// activeView = activeView + ("sjfslkd" -> (true,3))
// println("SIZE: " + activeView.size)
// println(activeView.get("sjfslkd"))
