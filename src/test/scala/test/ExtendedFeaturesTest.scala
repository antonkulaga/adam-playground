package test

import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment, Strand}
import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground._
import comp.bio.aging.playground.extensions._
import org.apache.spark.rdd.RDD

class ExtendedFeaturesTest extends AdamTestBase {
  lazy val dnas2 = Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT" + "intron",
    "CAGCTGATCTCCAGATATGACCATGGGTTT" + "intron",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
  )

  val all = dnas2.reduce(_ + _)

  "extended features" should {

    "extract features transcript" in {



      val dic = new SequenceDictionary(Vector(record))
      val fragments = NucleotideContigFragmentRDD(sc.parallelize(dnas2fragments(dnas2)), dic)
      val exon1 = makeFeature(dnas.head, 0L, test, FeatureType.Exon, "gene", "transcript", "exon1")
      val intron1 = makeFeature("intron", 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon2 = makeFeature(dnas(1),30L  + 6L,test, FeatureType.Exon, "gene", "transcript", "exon2")
      val intron2 = makeFeature("intron", 30L  + 6L + 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon3 = makeFeature(dnas(2), 30L  + 6L + 30L + 6L,test, FeatureType.Exon, "gene", "transcript", "exon3")
      val tfs = Seq(exon1, intron1, exon2, intron2, exon3)
      val features = FeatureRDD(sc.parallelize(tfs))

      val fs = features.toDF()
      val featureType: FeatureType = FeatureType.Exon
      val tp = featureType.entryName
      val grouping = featureType match {
        case FeatureType.Gene => "geneId"
        case FeatureType.Exon => "exonId"
        case _ => "transcriptId"
      }
      val frags = fragments.toDF()
        .withColumnRenamed("start", "fragment_start")
        .withColumnRenamed("end", "fragment_end")


      val joined = frags.join(fs,
        fs("start") <= frags("fragment_end")
          &&  fs("end") >= frags("fragment_start")
          && fs("contigName") === frags("contigName")
          && fs("featureType") === tp
      )

      val es = features.rdd.map(f=>f.getExonId).collect().toSet
      val exons = fragments.extractFeatures(features, FeatureType.Exon, es)(f=>f.getExonId).mapValues(_._2).collect.toSet
      exons shouldEqual Set(
        "exon1" -> "ACAGCTGATCTCCAGATATGACCATGGGTT",
        "exon2" -> "CAGCTGATCTCCAGATATGACCATGGGTTT",
        "exon3" -> "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
      )


      val trs = features.rdd.map(f=>f.getTranscriptId).collect().toSet
      val transcripts: Map[String, String] = fragments.extractTranscripts(features, trs).collectAsMap().toMap
      transcripts shouldEqual Map( "transcript" ->  "ACAGCTGATCTCCAGATATGACCATGGGTTCAGCTGATCTCCAGATATGACCATGGGTTTCCAGAAGTTTGAGCCACAAACCCATGGTCA")

    }

    "extract 20 nts before start" in {

      val dic = new SequenceDictionary(Vector(record))
      val fragments = NucleotideContigFragmentRDD(sc.parallelize(dnas2fragments(dnas2)), dic)
      //val exon1 = makeFeature(dnas.head, 0L, test, FeatureType.Exon, "gene", "transcript", "exon1")
      val gene = makeFeature(all, 0L, test, FeatureType.Gene, "gene")
      val tr = makeFeature(all.drop(30), 30L, test, FeatureType.Transcript, "gene", "transcript")
      val intron1 = makeFeature("intron", 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon2 = makeFeature(dnas(1),30L  + 6L,test, FeatureType.Exon, "gene", "transcript", "exon2")
      val intron2 = makeFeature("start", 30L  + 6L + 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon3 = makeFeature(dnas(2), 30L  + 6L + 30L + 6L,test, FeatureType.Exon, "gene", "transcript", "exon3")
      val tfs = Seq(gene, intron1, exon2, intron2, exon3, tr)
      val features = FeatureRDD(sc.parallelize(tfs))

      val fs = features.toDF()
      val featureType: FeatureType = FeatureType.Exon
      val tp = featureType.entryName
      val grouping = featureType match {
        case FeatureType.Gene => "geneId"
        case FeatureType.Exon => "exonId"
        case _ => "transcriptId"
      }
      val frags = fragments.toDF()
        .withColumnRenamed("start", "fragment_start")
        .withColumnRenamed("end", "fragment_end")


      val joined = frags.join(fs,
        fs("start") <= frags("fragment_end")
          &&  fs("end") >= frags("fragment_start")
          && fs("contigName") === frags("contigName")
          && fs("featureType") === tp
      )

      val es = features.rdd.map(f=>f.getExonId).collect().toSet
      val exons = fragments.extractFeatures(features, FeatureType.Exon, es)(f=>f.getExonId).mapValues(_._2).collect.toSet
      exons shouldEqual Set(
        //"exon1" -> "ACAGCTGATCTCCAGATATGACCATGGGTT",
        "exon2" -> "CAGCTGATCTCCAGATATGACCATGGGTTT",
        "exon3" -> "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
      )


      val trs = features.rdd.map(f=>f.getTranscriptId).collect().toSet
      val transcripts: Map[String, String] = fragments.extractTranscripts(features, trs).collectAsMap().toMap
      transcripts shouldEqual Map( "transcript" ->  "CAGCTGATCTCCAGATATGACCATGGGTTTCCAGAAGTTTGAGCCACAAACCCATGGTCA")

      //println("FEATURES")
      //pprint.pprintln(features.rdd.collect())
      val genesMap = fragments.extractFeatures(features, FeatureType.Gene, Set("gene"))(f=>f.getGeneId).collectAsMap().toMap
      genesMap shouldEqual Map("gene" -> (ReferenceRegion(test,0,all.length, Strand.FORWARD), all))
      val transcriptMap = fragments.extractFeatures(features, FeatureType.Transcript, Set("transcript"))(f=>f.getTranscriptId).collectAsMap().toMap
      transcriptMap shouldEqual Map("transcript" -> (ReferenceRegion(test, 30L, all.length, Strand.FORWARD), all.drop(30)))

      val before = fragments.extractBeforeTranscripts(features, "gene", 20)
      before.collect().toList shouldEqual List(
        ("transcript", (ReferenceRegion(test, 10L, 30L, Strand.FORWARD), "TCCAGATATGACCATGGGTT"))
      )
      //fragments.extractBefore(features, "gene", 10)

    }

  }

}