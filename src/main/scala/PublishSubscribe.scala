import akka.actor.{Actor, ActorSelection, Props, Timers}

object PublishSubscribe {
  // Constructor
  def props(): Props =
    Props(new PublishSubscribe())

  // Messages
  final case class Publish(topic: String, message: String)
  final case class Subscribe(topic: String)
  final case class Unsubscribe(topic: String)
}

class PublishSubscribe extends Actor {
  import PublishSubscribe._

  // Init
  override def preStart(): Unit = {
    super.preStart()
  }

  // Receive
  override def receive: Receive = {
    case Subscribe(topic) =>
      println("Subscribe the topic " + topic)
  }

  private def getReference(id: String) : ActorSelection = {
    context.actorSelection("akka.tcp://"+ Global.SYSTEM_NAME +"@" + id + "/user/" + Global.PUBLISH_SUBSCRIBE_ACTOR_NAME)
  }
}
