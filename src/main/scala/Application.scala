import akka.actor.{Actor, ActorRef, Props}
import java.io._

object Application {

  def props(ip: String, port: Int, pubSubActor: ActorRef): Props =
      Props(new Application(ip, port, pubSubActor))

  final case class Publish(topic: String, message: String)
  final case class Subscribe(topic: String)
  final case class Unsubscribe(topic: String)
  final case class PSDeliver(topic: String, message: String)
  final case object GetTopics
  final case object Menu
}

class Application(ip: String, port: Int, pubSubActor: ActorRef) extends Actor {
  import Application._

  // Constants
  val MYSELF: String = s"$ip:$port"

  // State
  var logFile: File = null
  var fileWriter: PrintWriter = null
  var topics: Set[String] = Set()

  override def preStart(): Unit = {
    super.preStart()

    val directory: File = new File("messages/")
    if (!directory.exists()) {
      directory.mkdir()
    }
    logFile = new File("messages/" + MYSELF + ".log")
    fileWriter = new PrintWriter(logFile)
  }

  override def postStop(): Unit = {
    super.postStop()
    fileWriter.close()
  }

  override def receive: Receive = {
    case Publish(topic, message) =>
      val log: String = "  > Published message of topic " + topic + " with the following content: \n   " + message + "\n\n"
      println(log)
      fileWriter.write(log)

    case Subscribe(topic) =>
      val log = "  > Subscribed topic " + topic + ".\n\n"

      topics = topics + topic
      println(log)
      fileWriter.write(log)

    case Unsubscribe(topic) =>
      val log = "  > Unsubscribed topic " + topic + ".\n\n"

      topics = topics - topic
      println(log)
      fileWriter.write(log)

    case PSDeliver(topic, message) =>
      println("Message received.")

    case GetTopics =>
      if (topics.size > 0) {
        println("\n############### My Topics ###############\n")
        topics.foreach(topic => println("   > " + topic))
      } else {
        println("No subscribed topics yet.")
      }

    case Menu =>
      getHelpMenu()
  }

  private def getHelpMenu() {
    println("\n############### MENU ###############")
    println("Subscribe a topic: SUB topic")
    println("Unsubscribe a topic: UNSUB topic")
    println("Publish a message: PUB topic message")
    println("Get subscribed topics: TOPICS\n\n")
  }

}
