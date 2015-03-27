/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package akka.dispatch.verification

// Source:
// https://github.com/gnieh/tekstlib/blob/5559ff124be54b474a7106b3198caed6aba1269f/src/main/scala/gnieh/diff/DynamicProgLcs.scala

import scala.annotation.tailrec

/** The interface to classes that computes the longest common subsequence between
 *  two sequences of elements
 *
 *  @author Lucas Satabin
 */
trait Lcs[T] {
  /** Computes the longest commons subsequence between both inputs.
   *  Returns an ordered list containing the indices in the first sequence and in the second sequence.
   */
  def lcs(seq1: Seq[T], seq2: Seq[T]): List[(Int, Int)] =
    lcs(seq1, seq2, 0, seq1.size, 0, seq2.size)

  /** Computest the longest common subsequence between both input slices.
   *  Returns an ordered list containing the indices in the first sequence and in the second sequence.
   */
  def lcs(seq1: Seq[T], seq2: Seq[T], low1: Int, high1: Int, low2: Int, high2: Int): List[(Int, Int)]
}

/** Implementation of the LCS using dynamic programming.
 *
 *  @author Lucas Satabin
 */
class DynamicProgLcs[T] extends Lcs[T] {

  def lcs(s1: Seq[T], s2: Seq[T], low1: Int, high1: Int, low2: Int, high2: Int): List[(Int, Int)] = {
    val seq1 = s1.slice(low1, high1)
    val seq2 = s2.slice(low2, high2)
    if (seq1.isEmpty || seq2.isEmpty) {
      // shortcut if at least on sequence is empty, the lcs, is empty as well
      Nil
    } else if (seq1 == seq2) {
      // both sequences are equal, the lcs is either of them
      seq1.indices.map(i => (i + low1, i + low2)).toList
    } else if (seq1.startsWith(seq2)) {
      // the second sequence is a prefix of the first one
      // the lcs is the second sequence
      seq2.indices.map(i => (i + low1, i + low2)).toList
    } else if (seq2.startsWith(seq1)) {
      // the first sequence is a prefix of the second one
      // the lcs is the first sequence
      seq1.indices.map(i => (i + low1, i + low2)).toList
    } else {
      // we try to reduce the problem by stripping common suffix and prefix
      val (prefix, middle1, middle2, suffix) = splitPrefixSuffix(seq1, seq2, low1, low2)
      val offset = prefix.size
      val lengths = Array.ofDim[Int](middle1.size + 1, middle2.size + 1)
      // fill up the length matrix
      for {
        i <- 0 until middle1.size
        j <- 0 until middle2.size
      } if (middle1(i) == middle2(j))
        lengths(i + 1)(j + 1) = lengths(i)(j) + 1
      else
        lengths(i + 1)(j + 1) = math.max(lengths(i + 1)(j), lengths(i)(j + 1))
      // and compute the lcs out of the matrix
      @tailrec
      def loop(idx1: Int, idx2: Int, acc: List[(Int, Int)]): List[(Int, Int)] =
        if (idx1 == 0 || idx2 == 0) {
          acc
        } else if (lengths(idx1)(idx2) == lengths(idx1 - 1)(idx2)) {
          loop(idx1 - 1, idx2, acc)
        } else if (lengths(idx1)(idx2) == lengths(idx1)(idx2 - 1)) {
          loop(idx1, idx2 - 1, acc)
        } else {
          assert(seq1(offset + idx1 - 1) == seq2(offset + idx2 - 1))
          loop(idx1 - 1, idx2 - 1, (low1 + offset + idx1 - 1, low2 + offset + idx2 - 1) :: acc)
        }

      prefix ++ loop(middle1.size, middle2.size, Nil) ++ suffix
    }
  }

  /* Extract common prefix and suffix from both sequences */
  private def splitPrefixSuffix(seq1: Seq[T], seq2: Seq[T], low1: Int, low2: Int): (List[(Int, Int)], Seq[T], Seq[T], List[(Int, Int)]) = {
    val size1 = seq1.size
    val size2 = seq2.size
    val prefix =
      seq1.zip(seq2).takeWhile {
        case (e1, e2) => e1 == e2
      }.indices.map(i => (i + low1, i + low2)).toList
    val suffix =
      seq1.reverse.zip(seq2.reverse).takeWhile {
        case (e1, e2) => e1 == e2
      }.indices.map(i => (size1 - i - 1 + low1, size2 - i - 1 + low2)).toList.reverse
    (prefix, seq1.drop(prefix.size).dropRight(suffix.size), seq2.drop(prefix.size).dropRight(suffix.size), suffix)
  }
}
