import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import scala.io.StdIn._
import util.control.Breaks._
import scala.util.matching._

import Global._
import Application._

object Main {

  // Commands
  val PUBLISH = "PUB"
  val SUBSCRIBE = "SUB"
  val UNSUBSCRIBE = "UNSUB"
  val TOPICS = "TOPICS"
  val VIEWS = "VIEWS"
  val NEIGH_TOPICS = "NTOPICS"
  val HELP = "HELP"
  val QUIT = "Q"

  // Command descriptions
  val cmdDescriptions: Map[String, String] = Map(
    PUBLISH -> "Publish a message: PUB <topic> <message>",
    SUBSCRIBE -> "Subscribe a topic: SUB <topic>",
    UNSUBSCRIBE -> "Unsubscribe a topic: UNSUB <topic>",
    TOPICS -> "Get subscribed topics: TOPICS",
    VIEWS -> "Shows the active and passive view: VIEWS",
    NEIGH_TOPICS -> "Shows all of our neighbours' topics: NTOPICS"
  )

  // Actors
  var hyParViewActor: ActorRef = _
  var publishSubscribeActor: ActorRef = _
  var applicationActor: ActorRef = _

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: [myAddress] [myPort] [nProcesses] [contactAddress]* [contactPort]*")
      return
    }

    val myAddress = args(0)
    val myPort = args(1)
    val nProcesses = args(2).toInt
    val contactID = if (args.length >= 5) args(3) + ':' + args(4) else null

    // Load main/resources/application.conf file as Config instance
    val config: Config = ConfigFactory.load("application.conf")

    // Create new Config instances for host address and port
    val hostConfig: Config = ConfigFactory.parseString("akka.remote.netty.tcp.host=" + myAddress)
    val portConfig: Config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + myPort)

    // Merge the newly created configurations with the loaded config to get the missing fields
    val combined: Config = hostConfig.withFallback(portConfig).withFallback(config)

    // Load the new configuration
    val complete: Config = ConfigFactory.load(combined)

    // Create the actor system
    val actorSystem: ActorSystem = ActorSystem(SYSTEM_NAME, complete)

    // Pulling the values out of the configuration tree (should be similar to myAddress and myPort)
    val host: String = actorSystem.settings.config.getString("akka.remote.netty.tcp.hostname")
    val port: String = actorSystem.settings.config.getString("akka.remote.netty.tcp.port")

    // Init actors
    hyParViewActor = actorSystem.actorOf(HyParView.props(host, port.toInt, contactID, nProcesses), HYPARVIEW_ACTOR_NAME)
    publishSubscribeActor = actorSystem.actorOf(PublishSubscribe.props(host, port.toInt), PUBLISH_SUBSCRIBE_ACTOR_NAME)
    applicationActor = actorSystem.actorOf(Application.props(host, port.toInt), APPLICATION_ACTOR_NAME)

    // Start the Application Menu
    applicationMenu()

    // Stop the actors
    actorSystem.stop(applicationActor)
    actorSystem.stop(publishSubscribeActor)
    actorSystem.stop(hyParViewActor)

    // Exit the process
    actorSystem.terminate()
  }

  private def applicationMenu(): Unit = {
    println("\n\n\nWelcome to Publish-Subscribe application. HELP for menu and Q to exit application.\n")

    val separator = new Regex("\"(.*?)\"|([^\\s]+)")
    breakable {
      do {
        val input = readLine("> ")
        println()

        val expression = separator.findAllMatchIn(input).toList
        val command = expression.map(element => element.toString).toArray
        val action = if (command.nonEmpty) command(0) else ""

        action match {
          case PUBLISH => handlePublish(command)
          case SUBSCRIBE => handleSubscribe(command)
          case UNSUBSCRIBE => handleUnsubscribe(command)
          case TOPICS => handleTopics(command)
          case VIEWS => handleViews(command)
          case NEIGH_TOPICS => handleNeighbourTopics(command)
          case HELP => handleHelp()
          case QUIT => break
          case "" => print("")
          case _ => println("Unknown command.\n")
        }

        // Helps the prompt not getting overwritten by messages output
        Thread.sleep(100)
      } while (true)
    }

    println("Goodbye. See you next time!")
  }

  private def handlePublish(command: Array[String]): Unit = {
    if(command.length == 3) {
      applicationActor ! Publish(command(1), command(2))
    } else {
      println("Wrong number of arguments.\n")
    }
  }

  private def handleSubscribe(command: Array[String]): Unit = {
    if(command.length == 2) {
      applicationActor ! Subscribe(command(1))
    } else {
      println("Wrong number of arguments.\n")
    }
  }

  private def handleUnsubscribe(command: Array[String]): Unit = {
    if(command.length == 2) {
      applicationActor ! Unsubscribe(command(1))
    } else {
      println("Wrong number of arguments.\n")
    }
  }

  private def handleTopics(command: Array[String]): Unit = {
    if(command.length == 1) {
      applicationActor ! GetTopics
    } else {
      println("Wrong number of arguments.\n")
    }
  }

  private def handleViews(command: Array[String]): Unit = {
    if(command.length == 1) {
      applicationActor ! ShowViews
    } else {
      println("Wrong number of arguments.\n")
    }
  }

  private def handleNeighbourTopics(command: Array[String]): Unit = {
    if(command.length == 1) {
      applicationActor ! ShowNeighbourTopics
    } else {
      println("Wrong number of arguments.\n")
    }
  }

  private def handleHelp(): Unit = {
    println("############### MENU ###############")
    cmdDescriptions.values.foreach(println)
    println()
  }

}
