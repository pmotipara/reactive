/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import akka.actor._

import scala.collection.immutable.Queue
import scala.concurrent.Future

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

  case object GCCopy

  case object GCFinished

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC => {
      log.debug(GC + " this: + " + self.path.name + " from Sender: " + sender.path.name)
      val newRoot = createRoot
      copyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case x: Operation => {
      log.debug(x + " this + " + self.path.name + " from Sender: " + sender.path.name)
      root ! x
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished => {
      log.debug(CopyFinished + " this + " + self.path.name + " from Sender: " + sender.path.name)
      log.debug("New root: " + newRoot.path.name)
      context.stop(root)
      root = newRoot
      while (!pendingQueue.isEmpty) {
        val tpl = pendingQueue.dequeue
        val op = tpl._1
        pendingQueue = tpl._2
        root ! op
      }
      context become receive
    }
    case x: Operation => {
      log.debug("Queueing Message " + x + " this + " + self.path.name + " from Sender: " + sender.path.name)
      pendingQueue = pendingQueue enqueue x
    }
    case OperationFinished (elem) => {
      log.debug(OperationFinished + " this + " + self.path.name + " from Sender: " + sender.path.name)
      self ! CopyFinished
    }
  }

  def copyTo(newRoot: ActorRef) = {
      log.debug("Sending CopyTo Message to Root: " + root.path.name + " from: " + self.path.name)
      root ! CopyTo(newRoot)
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  def receive = normal

  // optional
  val normal: Receive = {
    case Insert(ref, id, el) =>
      //log.debug(Insert(ref, id, el) + " this " + self.path.name + " from Sender: " + sender.path.name)
      if (el < elem) {
        subtrees.get(Left) match {
          case Some(a) => a forward Insert(ref, id, el)
          case None => {
            subtrees = subtrees + ((Left, context.actorOf(props(el, false), id + "." + Left.toString)))
            //sender ! OperationFinished(id)
            ref ! OperationFinished(id)
          }
        }
      } else if (el > elem) {
        subtrees.get(Right) match {
          case Some(a) => a forward Insert(ref, id, el)
          case None => {
            subtrees = subtrees + ((Right, context.actorOf(props(el, false), id + "." +  Right.toString)))
            //sender ! OperationFinished(id)
            ref ! OperationFinished(id)
          }
        }
      } else {
        removed = false
        //sender ! OperationFinished(id)
        ref ! OperationFinished(id)
      }
    case Remove(ref, id, el) =>
      //log.debug(Remove(ref, id, el) + " this + " + self.path.name + " Elem : " + elem + " from Sender: " + sender.path.name)
      if (el < elem) {
        subtrees.get(Left) match {
          case Some(a) => a ! Remove(ref, id, el)
          case None => ref ! OperationFinished(id)
        }
      } else if (el > elem) {
        subtrees.get(Right) match {
          case Some(a) => a ! Remove(ref, id, el)
          case None => ref ! OperationFinished(id)
        }
      } else {
        removed = true
        ref ! OperationFinished(id)
      }
    case Contains(ref,id,el) =>
      //log.debug(Contains(ref, id, el) + " this + " + self.path.name + " from Sender: " + sender.path.name)
      if (el < elem) {
        subtrees.get(Left) match {
          case Some(a) => a ! Contains(ref,id,el)
          case None => ref ! ContainsResult(id,false)
        }
      } else if (el > elem) {
        subtrees.get(Right) match {
          case Some(a) => a ! Contains(ref,id,el)
          case None => ref ! ContainsResult(id,false)
        }
      } else {
        ref ! ContainsResult(id, !removed)
      }
    case CopyTo(target) => {
      log.debug( CopyTo(target) + " this + " + self.path.name + "with parent: " + context.parent.path.name + " from Sender: " + sender.path.name)

      if (!removed) {
        target.asInstanceOf[ActorRef] ! Insert(self, elem, elem)
        log.debug("Switching to copying mode for " + self.path.name + " with set " + (subtrees.values.toSet + self))
        context.become(copying(subtrees.values.toSet + self, false))
      } else {
        log.debug("Switching to copying mode for " + self.path.name + " with set " + subtrees.values.toSet)
        context.become(copying(subtrees.values.toSet, false))
      }

      subtrees.values.foreach(a=> {
        a ! CopyTo(target)
      })
    }
  }



  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
*/
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) =>
      log.debug(OperationFinished(id) + ":" + elem + " this + " + self.path.name + " from Sender: " + sender.path.name + " with expected: ")
      expected.foreach(x => log.debug(x.path.name))
      val remaining = if (id == elem) {
        expected - self
      } else {
        expected - sender // Sent from child.
      }
      if (remaining.isEmpty) {
        self ! CopyFinished
        context.become(copying(remaining, true))
      } else {
        context.become(copying(remaining, false))
      }
   case CopyFinished =>
    log.debug(CopyFinished + " this + " + self.path.name + " from Sender: " + sender.path.name + " with parent: " + context.parent.path.name)
    context.parent ! OperationFinished (elem)
  }
}
