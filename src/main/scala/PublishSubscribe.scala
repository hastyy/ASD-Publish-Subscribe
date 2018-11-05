import akka.actor.Actor

class PublishSubscribe extends Actor {

  def receive = {
    case Subscribe(topic) =>
      println("Subscribe the topic " + topic)
  }
}
