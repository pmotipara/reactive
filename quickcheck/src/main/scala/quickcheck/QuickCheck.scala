package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("gmin1") = forAll(Gen.choose(Int.MinValue,Int.MaxValue-1))  { a =>
    val h = insert(a+1, insert(a,empty))
    findMin(h) == a
  }

  property("gmin2") = forAll(Gen.choose(Int.MinValue,Int.MaxValue-1)) { a =>
    val h = insert(a, insert(a+1,empty))
    findMin(h) == a
  }

  property("seq") = forAll { (h: H) =>
    def helper(h: H, lastVal: Int): Boolean = {
      if (isEmpty(h)) {
        true
      } else {
        val fval = findMin(h)
        (helper(deleteMin(h), fval) && fval >= lastVal)
      }
    }

    helper(h, Int.MinValue)
  }

  property("ssisso") = forAll { (plint : List[Int]) =>
    def remove(h: H, acc: Array[Int]): Array[Int] = {
      if (isEmpty(h)) {
        acc
      } else {
        remove(deleteMin(h), acc.:+(findMin(h)))
      }
    }

    def add(pl:List[Int], h: H): H = {
      pl match {
        case Nil => h
        case t::ts => add(ts, insert(t,h))
      }
    }

    val hp = add(plint,empty)
    val si = plint.sorted.toArray

    val result = remove(hp, Array())

    si.deep == result.deep

  }

  property("del1") = forAll { (a: Int) =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  property("meldmin") = forAll { (h1: H, h2: H) =>
    val h3 = meld(h1,h2)

    val a = findMin(h3)

    (a == findMin(h1) || a == findMin(h2))
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }

  lazy val genHeap: Gen[H] = for {
    x <- arbitrary[Int]
    hp <- oneOf(const(empty), genHeap)
  } yield insert(x,hp)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
