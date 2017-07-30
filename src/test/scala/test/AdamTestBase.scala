package test

import java.net.URL

import com.holdenkarau.spark.testing._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment, Strand}
import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground._
import comp.bio.aging.playground.extensions._
import org.apache.spark.SparkConf


abstract class AdamTestBase extends  WordSpec with Matchers with SharedSparkContext/*DatasetSuiteBase*/ {

  val text: String =
    """
      |The vast majority of animal species undergo the process of aging. Whilst aging is a nearly universal occurrence, it should be noted that other medical problems such as muscle wastage leading to sarcopenia, reduction in bone mass and density leading to osteoporosis, increased arterial hardening resulting in hypertension, atherosclerosis, and brain tissue atrophy resulting in dementia, all of which are nearly universal in humans, are classified as diseases in need of medical interventions (
    """.stripMargin

  def sparkContext = sc

  override def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator").
      set("spark.kryoserializer.buffer", "16m").
      set("spark.kryoserializer.buffer.max.mb", "1024").
      set("spark.kryo.referenceTracking", "true")
  }



  //override implicit def enableHiveSupport: Boolean = false

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
      .setContigName(test)
      .setStart(start)
      .setLength(str.length: Long)
      .setSequence(str)
      .setEnd(start + str.length)
      .build()
  }

  def makeFeature(sequence: String, start: Long,
                  contigName: String,
                  featureType: FeatureType,
                  geneId: String = "",
                  transcriptId: String = "",
                  exondId: String = ""
                 ): Feature = {
    Feature.newBuilder()
      .setStart(start)
      .setEnd(start + sequence.length)
      .setGeneId(geneId)
      .setTranscriptId(transcriptId)
      .setExonId(exondId)
      .setFeatureType(featureType.entryName)
      .setContigName(contigName)
      .setStrand(Strand.FORWARD)
      .clearAttributes()
      .build()
  }


  def dnas2fragments(dnas: Seq[String]): List[NucleotideContigFragment] = {
    val (_, frags) = dnas.foldLeft((0L, List.empty[NucleotideContigFragment]))
    {
      case ((start, acc), str) => (start + str.length, makeFragment(str, start)::acc)
    }
    frags.reverse
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