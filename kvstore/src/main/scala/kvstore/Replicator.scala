package kvstore

import akka.actor._
import akka.event.LoggingReceive
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  var schedules = Map.empty[Long, Cancellable]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  //var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case Replicate(key, valueOpt, id) => {
      //System.out.println("Self %s Replicate %s %s %s".format(self.path.name, key,valueOpt,id))
      //pending = pending :+ Snapshot(key, valueOpt, seq)
      val s = nextSeq
      val cancel = context.system.scheduler.schedule(Duration(0, SECONDS), Duration(200, MILLISECONDS), replica, Snapshot(key, valueOpt, s))
      schedules = schedules + ((s, cancel))
      acks = acks + ((s, (sender, Replicate(key, valueOpt, id))))
    }
    case SnapshotAck(key, seq) => {
      acks.get(seq) match {
        case Some((x,y)) => {
          x ! Replicated(key, y.id)
        }
        case None => {
          //Do Nothing
        }
      }
      acks = acks - seq
      schedules.get(seq) match {
        case Some(c) => {
          c.cancel
        }
        case None => {}
      }
      schedules = schedules - seq
    }
  }

}
