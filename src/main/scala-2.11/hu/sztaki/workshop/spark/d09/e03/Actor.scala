package hu.sztaki.workshop.spark.d09.e03

import hu.sztaki.workshop.spark.d09.e03.{UnsubscribeReceiver, SubscribeReceiver}
import org.apache.spark.actor.Utils

import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import scala.util.Random

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.AkkaUtils
import org.apache.spark.streaming.receiver.ActorHelper

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * Sends the random content to every receiver subscribed with 1/2
 *  second delay.
 */
class FeederActor extends Actor {

  val rand = new Random()
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  val strings: Array[String] = Array("words ", "may ", "count ")

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }

  /*
   * A thread to generate random messages
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(500)
        receivers.foreach(_ ! makeMessage)
      }
    }
  }.start()

  def receive: Receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
      receivers = LinkedList(receiverActor) ++ receivers

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
      receivers = receivers.dropWhile(x => x eq receiverActor)

  }
}

/**
 * This receiver actor goes and subscribe to a typical
 * publisher/feeder actor and receives data.
 */
class SampleActorReceiver[T: ClassTag](urlOfPublisher: String)
  extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = remotePublisher ! SubscribeReceiver(context.self)

  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)

}

/**
 * A sample feeder actor
 *
 * Usage: FeederActor <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
 */
object FeederActor {

  def main(args: Array[String]) {
    if (args.length < 2){
      System.err.println("Usage: FeederActor <hostname> <port>\n")
      System.exit(1)
    }
    val Seq(host, port) = args.toSeq

    val conf = new SparkConf
    val actorSystem = Utils.get.createActorSystem("test", host, port.toInt, conf = conf,
      securityManager = Utils.security(conf))._1
    val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
 * A sample word count program demonstrating the use of plugging in
 * Actor as Receiver
 * Usage: ActorWordCount <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: ActorWordCount <hostname> <port>")
      System.exit(1)
    }

    val Seq(host, port) = args.toSeq
    /**
      * @todo[20] Configure spark and create the spark context.
      *           Set batch duration to 2 seconds.
      */
    val sparkConf = new SparkConf().setAppName("ActorWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /**
      * @todo[22] Create an `actorStream`.
      * @hint Create a `SimpleActorReceiver`.
      *       An important point to note:
      *       Since Actor may exist outside the spark framework,
      *       it is thus user's responsibility to ensure the type safety,
      *       i.e type of data received and InputDstream should be same.
      */
    val lines = ssc.actorStream[String](
      Props(new SampleActorReceiver[String]("akka.tcp://test@%s:%s/user/FeederActor".format(
        host, port.toInt))), "SampleReceiver")

    /**
      * @todo[23] Compute wordcount.
      */
    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()

    /**
      * @todo[24] He? Yes! Do it!
      */
    ssc.start()
    ssc.awaitTermination()
  }
}