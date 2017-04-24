package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /**
   * Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def helper(chars: Array[Char], acc: Int): Int = {
      if (chars.isEmpty) acc
      else if (chars.head == '(') helper(chars.tail, acc + 1)
      else if (chars.head == ')') {
        if (acc > 0) helper(chars.tail, acc - 1)
        else -1
      } else helper(chars.tail, acc)
    }
    return helper(chars, 0) == 0;
  }

  /**
   * Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, a1: Int, a2: Int): (Int, Int) = {
      var b = (a1, a2)
      var i = idx
      while (i < until) {
        val c = chars(i)
        if (c == '(') b = (b._1 + 1, b._2)

        if (c == ')') {
          if (b._1 > 0)
            b = (b._1 - 1, b._2)
          else
            b = (b._1, b._2 + 1)
        }
        i = i + 1
      }
      b
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = from + (until - from) / 2
        val (l, r) = parallel(
          reduce(from, mid),
          reduce(mid, until))

        val matched = scala.math.min(l._1, r._2)
        (l._1 + r._1 - matched, l._2 + r._2 - matched)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
