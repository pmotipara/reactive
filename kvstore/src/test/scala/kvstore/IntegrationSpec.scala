/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.{ Actor, Props, ActorRef, ActorSystem }
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import scala.concurrent.duration._
import org.scalatest.FunSuiteLike
import org.scalactic.ConversionCheckedTripleEquals

class IntegrationSpec(_system: ActorSystem) extends TestKit(_system)
    with FunSuiteLike
        with Matchers
    with BeforeAndAfterAll
    with ConversionCheckedTripleEquals
    with ImplicitSender
    with Tools {

  import Replica._
  import Replicator._
  import Arbiter._

  def this() = this(ActorSystem("ReplicatorSpec"))

  override def afterAll: Unit = system.shutdown()

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */

  test("Existing replicas in the cluster must not be assigned new replicators") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "kky1-primary")
    val user = session(primary)

    val secondary1 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "kky1-secondary-1")
    val secondary2 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "kky1-secondary-2")
    val secReplica = session(secondary1)

    arbiter.expectMsg(Join)
    arbiter.expectMsg(Join)
    arbiter.expectMsg(Join)

    arbiter.send(primary, JoinedPrimary)
    arbiter.send(secondary1, JoinedSecondary)
    arbiter.send(secondary2, JoinedSecondary)
    arbiter.send(primary, Replicas(Set(primary, secondary1, secondary2)))

    val ack1 = user.set("k1", "v1")
    user.waitAck(ack1)
    assert(secReplica.get("k1") === Some("v1"))

    val ack2 = user.set("k2", "v2")
    user.waitAck(ack2)
    assert(secReplica.get("k2") === Some("v2"))

    val ack3 = user.remove("k1")
    user.waitAck(ack3)
    assert(secReplica.get("k1") === None)

    arbiter.send(primary, Replicas(Set(primary, secondary1)))
    val ack4 = user.remove("k2")
    user.waitAck(ack4)
    assert(secReplica.get("k2") === None)

  }

  private def randomInt(implicit n: Int = 16) = (Math.random * n).toInt

  private def randomQuery(client: Session) {
    val rnd = Math.random
    if (rnd < 0.3) client.setAcked(s"k$randomInt", s"v$randomInt")
    else if (rnd < 0.6) client.removeAcked(s"k$randomInt")
    else client.getAndVerify(s"k$randomInt")
  }

  test("case1: Random ops") {
    val arbiter = TestProbe()

    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case1-primary")
    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val secondary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case1-secondary")
    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    arbiter.send(primary, Replicas(Set(primary, secondary)))

    val client = session(primary)
    for (_ <- 0 until 100) randomQuery(client)
  }

  test("case2: Random ops with 3 secondaries") {
    val arbiter = TestProbe()

    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case2-primary")
    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val secondaries = (1 to 3).map(id => system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), s"case2-secondary-$id"))

    secondaries.foreach(secondary => {
      arbiter.expectMsg(Join)
      arbiter.send(secondary, JoinedSecondary)
    })

    val client = session(primary)
    for (i <- 0 until 1000) {
      randomQuery(client)
      if      (i == 100) arbiter.send(primary, Replicas(Set(secondaries(0))))
      else if (i == 200) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1))))
      else if (i == 300) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1), secondaries(2))))
      else if (i == 400) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1))))
      else if (i == 500) arbiter.send(primary, Replicas(Set(secondaries(0))))
      else if (i == 600) arbiter.send(primary, Replicas(Set()))
    }
  }
  }
