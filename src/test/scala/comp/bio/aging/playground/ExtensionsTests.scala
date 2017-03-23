package comp.bio.aging.playground


import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import comp.bio.aging.playground.extensions._
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
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA")

  def contig() = {
    val c= new Contig()
    c.setContigName("test")
    c
  }

  protected def makeFragment(str: String, start: Long) = {

    NucleotideContigFragment.newBuilder()
      .setContig(contig())
      .setFragmentStartPosition(start)
      .setFragmentLength(str.length: Long)
      .setFragmentSequence(str)
      .setFragmentEndPosition(start + str.length)
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
      val dic = new SequenceDictionary()
      val frags = sc.parallelize(dnas2fragments(dnas))

      val fragments = new NucleotideContigFragmentRDD(frags, dic)
      val byRegion = fragments.rdd.keyBy(ReferenceRegion(_))


      val regions = List(
        new ReferenceRegion(contig().getContigName, 0, 5),
        new ReferenceRegion(contig().getContigName, 25, 35),
        new ReferenceRegion(contig().getContigName, 40, 50),
        new ReferenceRegion(contig().getContigName, 50, 70)
      )

      val places: RDD[(ReferenceRegion, (ReferenceRegion, String))] = byRegion
        .flatMap{
          case (Some(reg), fragment) if  regions.exists(reg.overlaps) =>
            regions.collect{
              case region if reg.overlaps(region) => (region, fragments.trimStringByRegion((reg, fragment), region))
            }

          case _ => Nil
        }
      println("===========")
      pprint.pprintln(places.collect().toList)


      val results= fragments.extractRegions(regions).collect().toSet

      val seqs= regions.zip(regions.zip(List("ACAGC", "GGGTTCAGCT", "CCAGATATGA", "CCATGGGTTTCCAGAAGTTT"))).toSet
      seqs shouldEqual results
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