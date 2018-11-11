import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory


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
    val actorSystem = ActorSystem(Global.SYSTEM_NAME, complete)

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

  }
}
