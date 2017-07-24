package comp.bio.aging.playground


import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.apache.spark.rdd.RDD
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.Nil


class ExtensionsTests extends WordSpec with Matchers with SharedSparkContext {

  val text: String =
    """
      |The vast majority of animal species undergo the process of aging. Whilst aging is a nearly universal occurrence, it should be noted that other medical problems such as muscle wastage leading to sarcopenia, reduction in bone mass and density leading to osteoporosis, increased arterial hardening resulting in hypertension, atherosclerosis, and brain tissue atrophy resulting in dementia, all of which are nearly universal in humans, are classified as diseases in need of medical interventions (
    """.stripMargin

//  "spark test" in {
//    val words: List[String] = text.split(" ").toList
//    val rdd: RDD[String] = sc.parallelize(words)
//    }

  def sparkContext = sc

  val dnas: Seq[String] = Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT",
    "CAGCTGATCTCCAGATATGACCATGGGTTT",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
  )


  lazy val test = "test"
  
  val merged = dnas.reduce(_ + _)

  val record = SequenceRecord("test", merged.length)

  protected def makeFragment(str: String, start: Long) = {

    NucleotideContigFragment.newBuilder()
      .setContigName("test")
      .setStart(start)
      .setLength(str.length: Long)
      .setSequence(str)
      .setEnd(start + str.length)
      .build()
  }

  def dnas2fragments(dnas: Seq[String]): List[NucleotideContigFragment] = {
    val (_, frags) = dnas.foldLeft((0L, List.empty[NucleotideContigFragment]))
    {
      case ((start, acc), str) => (start + str.length, makeFragment(str, start)::acc)
    }
    frags.reverse
  }


  "Extended nucleotide fragments RDD" should {

    "find right regions" in {

      val dic = new SequenceDictionary(Vector(record))
      val frags = sc.parallelize(dnas2fragments(dnas))
      val fragments = NucleotideContigFragmentRDD(frags, dic)

      val byRegion = fragments.rdd.keyBy(ReferenceRegion(_))

      val regions = List(
        new ReferenceRegion(test, 0, 5),
        new ReferenceRegion(test, 25, 35),
        new ReferenceRegion(test, 40, 50),
        new ReferenceRegion(test, 50, 70)
      )

      val results: Set[(ReferenceRegion, String)] = fragments.extractRegions(regions).collect().toSet
      val seqs= regions.zip(List("ACAGC", "GGGTTCAGCT", "CCAGATATGA", "CCATGGGTTTCCAGAAGTTT")).toSet
      seqs shouldEqual results
    }

    "find regions for sequences" in {

      val dic = new SequenceDictionary(Vector(record))
      val frags = sc.parallelize(dnas2fragments(dnas))
      val fragments = NucleotideContigFragmentRDD(frags, dic)

      val seqs = List("ACAGC" ,"CAGCTG", "TGAGCCACAAACCC")

      val regs: RDD[(String, List[ReferenceRegion])] = fragments.findRegions(seqs, false)
      val regions = regs.values.collect().toList.flatten
      val extracted = fragments.extractRegions(regions)
      extracted.values.collect.toSet shouldEqual seqs.toSet

      val seqsSpecial = List("NGG")
      val special = fragments.findSpecialRegions(seqsSpecial)(stringSeqExtensions.seqsInclusionsInto)
      val regionsSpecial = special.values.collect().toList.flatten
      val extractedSpecial = fragments.extractRegions(regionsSpecial)
      extractedSpecial.values.collect.toSet shouldEqual Set("TGG", "GGG")
    }


    "find regions with mismatches" in {
      val dic = new SequenceDictionary(Vector(record))
      val frags = sc.parallelize(dnas2fragments(dnas))
      val fragments = NucleotideContigFragmentRDD(frags, dic)

      val seqsSpecial = List("TTT")
      val special = fragments.findRegionsWithMismatches(seqsSpecial, 1)
      val regionsSpecial = special.values.collect().toList.flatten
      val extractedSpecial = fragments.extractRegions(regionsSpecial)
      extractedSpecial.values.collect.toSet shouldEqual Set("TTT", "GTT", "TCT", "TAT", "TTG")
    }

    "search with mismatches in strings" in {
      val dic = new SequenceDictionary(Vector(record))
      val frags = sc.parallelize(dnas2fragments(dnas))
      val fragments = NucleotideContigFragmentRDD(frags, dic)

      val seqsSpecial = List("TTT")
      val special = fragments.findRegionsWithMismatches(seqsSpecial, 1)
      val regionsSpecial = special.values.collect().toList.flatten
      val extractedSpecial = fragments.extractRegions(regionsSpecial)
      extractedSpecial.values.collect.toSet shouldEqual Set("TTT", "GTT", "TCT", "TAT", "TTG")
    }

    "search with mismatches in fragments" in {
      val dic = new SequenceDictionary(Vector(SequenceRecord("test", merged.length)))
      val rdd = sc.parallelize(dnas2fragments(dnas))
      val fragments = NucleotideContigFragmentRDD(rdd, dic)
      fragments.findRegionsWithMismatches(List("CTGATCTCCAGATATGACCATGG"), 2).collect().head._2.size shouldEqual 2

      val one = Vector(
        "CTGATCTCCAGATATGACCATGG",
        "CTGATCTCCAAATATGACCATGG",
        "CAGATCTCCAAATATGACCATGG",
        "CTGATCTCCAGATATGACCATGG".reverse.complement
      )
      val efragments = NucleotideContigFragmentRDD(sc.parallelize(dnas2fragments(one)), dic)
      efragments.findRegionsWithMismatches(List("CTGATCTCCAGATATGACCATGG"), 0, false, true).collect().head._2.size shouldEqual 1
      efragments.findRegionsWithMismatches(List("CTGATCTCCAGATATGACCATGG"), 0, true, true).collect().head._2.size shouldEqual 2
      efragments.findRegionsWithMismatches(List("CTGATCTCCAGATATGACCATGG"), 2, false, true).collect().head._2.size shouldEqual 3
      efragments.findRegionsWithMismatches(List("CTGATCTCCAGATATGACCATGG"), 2, true, true).collect().head._2.size shouldEqual 4
    }

  }


  /**
    * Finds the URL of a "test file," usually in the src/test/resources directory.
    *
    * @param path The path of the file inside src/test/resources
    * @return The URL of the file
    */
  def resourceUrl(path: String): URL = {
    ClassLoader.getSystemClassLoader.getResource(path)
  }

  /**
    * Finds the full path of a "test file," usually in the src/test/resources directory.
    *
    * @param name The path of the file w/r/t src/test/resources
    * @return The absolute path of the file
    * @throws IllegalArgumentException if the file doesn't exist
    */
  def testFile(name: String): String = {
    val url = resourceUrl(name)
    if (url == null) {
      throw new IllegalArgumentException("Couldn't find resource \"%s\"".format(name))
    }
    url.getFile
  }
}