import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import HyparView._


object Main {
  def main(args: Array[String]): Unit = {

    if(args.length < 2) {
      println("You must provide your address and port.");
      return;
    }

    // Collect input
    var myAddress = args(0)
    var myPort = args(1).toInt
    var contactAddress: String = null
    var contactPort: Int = -1

    if(args.length >= 4) {
      contactAddress = args(2)
      contactPort = args(3).toInt
    }

    // Create actor system
    val systemName = Static.systemName

    val config = ConfigFactory.load("application.conf")
    val myConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + myPort)
    val combined = myConfig.withFallback(config);
    val complete = ConfigFactory.load(combined);

    val actorSystem = ActorSystem(systemName, complete)

    val host = actorSystem.settings.config.getString("akka.remote.netty.tcp.hostname")
    val port = actorSystem.settings.config.getString("akka.remote.netty.tcp.port")

    println("My host is " + host + " and my port is " + port)

    val pubSubActor = actorSystem.actorOf(Props[PublishSubscribe], "pubSub")
    val hyparview = actorSystem.actorOf(Props(classOf [HyparView], host, port.toInt, contactAddress, contactPort), "hyparview") //"HyparView"

  }
}
