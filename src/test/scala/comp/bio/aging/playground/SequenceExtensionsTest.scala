package comp.bio.aging.playground

import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.scalatest.{Matchers, WordSpec}


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
  }
}
