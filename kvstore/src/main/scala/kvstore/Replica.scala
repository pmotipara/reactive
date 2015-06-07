package kvstore

import java.util.concurrent.TimeUnit._

import akka.actor._
import akka.event.LoggingReceive
import kvstore.Arbiter._
import org.scalatest.Canceled

import scala.collection.immutable.Map
import scala.concurrent.duration.Duration

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  val persist = context.actorOf(persistenceProps,context.self.path.name + "-persist")
  var awaitingAck = Map.empty[Long, ActorRef]
  var awaitingPersistence = Map.empty[Long, Cancellable]
  var awaitingReplicated = Map.empty[Long, Set[ActorRef]]
  var timeoutHandle = Map.empty[Long, Cancellable]

  var _seqCounter = -1L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter -= 1
    ret
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    arbiter ! Join
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {
    case Insert(key, value, id) => {
      kv = kv + ((key, value))
      handleUpdate(key, Some(value), id)
    }
    case Remove(key, id) => {
      kv = kv - key
      handleUpdate(key, None, id)
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Replicas(replicas) => {
      val srep :Set[(ActorRef,ActorRef)] = for {
        s <- replicas if (s != self)
        rep = secondaries.get(s) match {
          case Some(r)  => r
          case None => {
            val irep = context.actorOf(Replicator.props(s), "replicator-" + s.path.name + "-name")
            for ((k,v) <- kv) {
              irep ! Replicate(k, Option(v), nextSeq)
            }
            irep
          }
        }
      } yield (s,rep)

      var sec = srep.toMap[ActorRef, ActorRef]

      for ((k,v) <- secondaries if (!replicas.contains(k))) {
        context.stop(v)
        sec = sec - k
      }

      secondaries = sec
      replicators = secondaries.values.toSet
      awaitingReplicated = awaitingReplicated.mapValues(v => v.filter(replicators.contains(_)))
      for ((id, reps) <- awaitingReplicated) {
        if (reps.isEmpty) {
          awaitingReplicated = awaitingReplicated - id
          handleAcks(id)
        }
      }
    }
      // Acknowledgments
    case Replicated(key, id) => {
      //System.out.println("Received Rep acknowledgemnt!")
      val set: Set[ActorRef] = awaitingReplicated.get(id) match {
        case Some(x) => {
          x - sender
        }
        case None => Set.empty
      }
      if (set.isEmpty) {
        awaitingReplicated = awaitingReplicated - id
        handleAcks(id)
      } else {
        awaitingReplicated = awaitingReplicated + ((id, set))
      }

    }
    case Persisted(key, id) => {
      awaitingPersistence.get(id).foreach(_.cancel)
      awaitingPersistence = awaitingPersistence - id
      handleAcks(id)
    }
  }

  def handleUpdate(key: String, valopt: Option[String], id: Long) = {

    if (!replicators.isEmpty) {
      var msgs = Set.empty[ActorRef]
      replicators.foreach(
        r => {
          r ! Replicate(key, valopt, id)
          msgs = msgs + r
        }
      )
      awaitingReplicated = awaitingReplicated + ((id, msgs))
    }
    val requester = sender
    val pcan =
      context.system.scheduler.schedule(Duration(0, SECONDS), Duration(100, MILLISECONDS), persist, Persist(key, valopt, id))
    val cancel = context.system.scheduler.scheduleOnce(Duration(1, SECONDS))( {
      requester ! OperationFailed(id)
      //pcan.cancel
    })
    awaitingAck = awaitingAck + ((id, sender))
    awaitingPersistence = awaitingPersistence + ((id, pcan))
    timeoutHandle = timeoutHandle + ((id, cancel))
  }

  def handleAcks(id: Long) = {
    val repl = awaitingReplicated.get(id)
    val pers = awaitingPersistence.get(id)

    if (repl == None && pers == None) {
      for (ref <- awaitingAck.get(id)) {
        timeoutHandle.get(id).foreach(_.cancel)
        ref ! OperationAck(id)
        awaitingAck = awaitingAck - id
      }
    }
  }

  import context.dispatcher
  var expectedSeqNo = 0
  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key, value, seq) => {
      //System.out.println("Self: %s Snapshot: %s %s %s".format(self.path.name, key,value,seq))
      if (expectedSeqNo == seq) {
        if (!awaitingAck.keySet.contains(seq)) {
          kv = value match {
            case Some(v) => kv + ((key, v))
            case None => kv - key
          }
          val cancel = context.system.scheduler.schedule(Duration(0, SECONDS), Duration(100, MILLISECONDS), persist, Persist(key, value, seq))
          awaitingAck = awaitingAck + ((seq, sender))
          awaitingPersistence = awaitingPersistence + ((seq, cancel))
        }
      } else if (expectedSeqNo > seq) {
        sender ! SnapshotAck(key, seq)
      } else {
        // Do Nothing!
      }
    }
    case Persisted(key, id) => {
      val pers = awaitingPersistence.get(id)

      if (pers != None) {
        pers.foreach(_.cancel)
        expectedSeqNo += 1
        awaitingPersistence = awaitingPersistence - id
        for (ref <- awaitingAck.get(id)) {
          ref ! SnapshotAck(key, id)
          awaitingAck = awaitingAck - id
        }
      }
    }
  }
}

