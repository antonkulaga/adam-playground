package test

import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground._
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._

class SequenceExtensionsTest extends WordSpec with Matchers {

  val str = "ATTCGCGAGCTAGCTAGCGTAC"
  val cmp = "TAAGCGCTCGATCGATCGCATG"

  "String extensions" should {
    "write complementary DNA" in {
      str.complement shouldEqual cmp
    }

    "show inclusions" in {
      val str = "ATTCGCGAGCTAGCTAGCGTAC"
      "GC".inclusionsInto(str) shouldEqual List(4, 8, 12, 16)
    }

    "search with mismatches" in {
      val str = "ATTCGCGAGCTAGCTAGCGTAC"
      val matches = "GCGA".partialMatchesIn(str, 1).toSet
      matches.map(v=>str.substring(v,v + 4 )) shouldEqual Set("GCGA", "GCTA", "GCGT")
    }
  }
}
