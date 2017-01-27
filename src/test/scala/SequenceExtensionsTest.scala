import org.scalatest.{Matchers, WordSpec}

import org.bdgenomics.utils.misc
import com.holdenkarau.spark.testing

class SequenceExtensionsTest extends WordSpec with Matchers {

  import org.comp.bio.aging.playground.extensions.stringext._

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
