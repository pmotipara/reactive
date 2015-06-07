package calculator

import math._

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal( delta(a(),b(),c()))
  }

  private def delta(a: Double, b: Double, c: Double) : Double = {
    pow(b,2) - (4 * a * c)
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal(computeSol(a(),b(),c(),delta()))
  }

  private def computeSol(a: Double, b: Double, c: Double, delta: Double) : Set[Double] = {
    if (delta < 0) {
      Set()
    } else {
      Set((-b + sqrt(delta)) / (2 * a), (-b - sqrt(delta)) / (2 * a))
    }
  }
}
