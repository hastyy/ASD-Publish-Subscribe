import scala.math.min
import scala.util.Random

object Global {
  val SYSTEM_NAME = "PublishSubscribe"
  val HYPARVIEW_ACTOR_NAME = "HyParView"
  val PUBLISH_SUBSCRIBE_ACTOR_NAME = "PublishSubscribe"
  val APPLICATION_ACTOR_NAME = "ApplicationTest"

  def getRandomElement[T](set: Set[T]): T = {
    return set.toVector(new Random().nextInt(set.size))
  }

  def getRandomSubset[T](set: Set[T], size: Int): Set[T] = {
    var auxSet: Set[T] = set
    var sample: Set[T] = Set()
    if (set.nonEmpty) {
      for (_ <- 1 to min(size, set.size)) {
        val randomElement = getRandomElement(auxSet)
        auxSet = auxSet - randomElement
        sample = sample + randomElement
      }
    }
    return sample
  }
}
