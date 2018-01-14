package test

import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._
import comp.bio.aging.playground.trees.SimpleTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment}
import org.scalatest.{Matchers, WordSpec}


class TreesTest extends AdamTestBase {

  "Syntax trees" should {

    "find common subsequences" in {
      val utterances = List(
        "Hello world !",
        "How are you today?",
        "How are you doing?",
        "are you there?",
        "What a beautiful world !"
      )
      val rdd = sparkContext.parallelize(utterances)
      val tree = SimpleTree(rdd)
      tree.repeatsOfMin(12, 1) shouldEqual Map( ("How are you ", 2) )
      tree.findInclusion("world").map(_._1) shouldEqual Set("Hello world !", "What a beautiful world !")
    }
  }
}