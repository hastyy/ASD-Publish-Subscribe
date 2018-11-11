import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import scala.io.StdIn._
import util.control.Breaks._
import scala.util.matching._

import Application._


object Main {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Usage: [myAddress] [myPort] [nProcesses] [contactAddress]* [contactPort]*")
      return
    }

    val myAddress = args(0)
    val myPort = args(1).toInt
    val nProcesses = args(2).toInt
    val contactID = if (args.length >= 5) args(3) + ':' + args(4) else null

    // Load main/resources/application.conf file as Config instance
    val config = ConfigFactory.load("application.conf")
    // Create a new config instance with given property tree
    val myConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + myPort)
    // Merge the newly created configuration with the loaded config to get the missing fields
    val combined = myConfig.withFallback(config)
    // Load the new configuration
    val complete = ConfigFactory.load(combined)

    // Create the actor system
    val actorSystem: ActorSystem = ActorSystem(Global.SYSTEM_NAME, complete)

    // The code below is here for future reference
    val host = actorSystem.settings.config.getString("akka.remote.netty.tcp.hostname")
    val port = actorSystem.settings.config.getString("akka.remote.netty.tcp.port")

    // Actors
    val hyparView = actorSystem.actorOf(
      HyparView.props(host, port.toInt, contactID, nProcesses),
      Global.HYPARVIEW_ACTOR_NAME
    )
    val publishSubscribe = actorSystem.actorOf(
      PublishSubscribe.props(host, port.toInt),
      Global.PUBLISH_SUBSCRIBE_ACTOR_NAME
    )

    val application = actorSystem.actorOf(
      Application.props(host, port.toInt, null),
      Global.APPLICATION_ACTOR_NAME
    )

    println("Welcome to Publish-Subscribe application. HELP for menu and Q to exit application.")
    val regex = new Regex("\"(.*?)\"|([^\\s]+)")
    var input: String = null
    var command: Array[String] = null
    breakable {
      do {
        input = readLine("> ")
        println('\n')

        val expression = regex.findAllMatchIn(input).toList
        command = expression.map(element => element.toString).toArray
        val action = if (command.nonEmpty) command(0).toString else ""

        action match {
          case "SUB" =>
            if(command.length == 2) {
              application ! Subscribe(command(1))
            } else {
              println("Wrong number of arguments.")
            }
          case "UNSUB" =>
            if(command.length == 2) {
              application ! Unsubscribe(command(1))
            } else {
              println("Wrong number of arguments.")
            }
          case "PUB" =>
            if(command.length == 3) {
              application ! Publish(command(1), command(2))
            } else {
              println("Wrong number of arguments.")
            }
          case "TOPICS" =>
            if(command.length == 1) {
              application ! GetTopics
            } else {
              println("Wrong number of arguments.")
            }
          case "HELP" =>
            if (command.length == 1) {
              application ! Menu
            } else {
               println("Wrong number of arguments.")
            }
          case "Q" => break
          case _ => println("Unknown command.")
        }

        Thread.sleep(500)
      } while(true)
    }
    println("Goodbye. See you next time!")

    // Shutdown process
    actorSystem.stop(application)
    actorSystem.stop(publishSubscribe)
    actorSystem.stop(hyparView)
    actorSystem.terminate()
  }
}
