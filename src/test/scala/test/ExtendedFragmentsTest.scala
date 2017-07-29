package test

import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment}
import org.scalatest.{Matchers, WordSpec}


class ExtendedFragmentsTest extends AdamTestBase {

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

  "extended features should" in {

    def makeFeature(sequence: String, start: Long,
                    contigName: String,
                    featureType: FeatureType,
                    geneId: String = "",
                    transcriptId: String = "",
                    exondId: String = ""
                   ): Feature = {
      val f = new Feature()
        f.setStart(start)
        f.setEnd(start + sequence.length)
        f.setGeneId(geneId)
        f.setTranscriptId(transcriptId)
        f.setExonId(exondId)
        f
      }
  }
}