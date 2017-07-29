package test

import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment, Strand}
import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground._
import comp.bio.aging.playground.extensions._
import org.apache.spark.rdd.RDD

class ExtendedFeaturesTest extends AdamTestBase {

  "extended features" should {

    implicit val session = this.spark


    "extract features transcript" in {

      val dnas2 = Vector(
        "ACAGCTGATCTCCAGATATGACCATGGGTT" + "intron",
        "CAGCTGATCTCCAGATATGACCATGGGTTT" + "intron",
        "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
      )

      val dic = new SequenceDictionary(Vector(record))
      val fragments = NucleotideContigFragmentRDD(sc.parallelize(dnas2fragments(dnas2)), dic)
      val exon1 = makeFeature(dnas.head, 0L, test, FeatureType.Exon, "gene", "transcript", "exon1")
      val intron1 = makeFeature("intron", 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon2 = makeFeature(dnas(1),30L  + 6L,test, FeatureType.Exon, "gene", "transcript", "exon2")
      val intron2 = makeFeature("intron", 30L  + 6L + 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon3 = makeFeature(dnas(2), 30L  + 6L + 30L + 6L,test, FeatureType.Exon, "gene", "transcript", "exon3")
      val tfs = Seq(exon1, intron1, exon2, intron2, exon3)
      val features = FeatureRDD(sc.parallelize(tfs))

      import session.implicits._
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
      //val j: Array[Row] = joined.collect()
      //pprint.pprintln(j.toList.map(r=>r.toString()))
      println("let us start grouping!")
      /*
      val grouped = joined.groupByKey{r=>
          pprint.pprintln(r)
          val t = r.getAs[String]("transcriptId")
          println("==TTTT===")
          println(t)
          t
      }
      */
      //println("FS SCHEMA----------------------")
      //fs.printSchema()
      //println("JOINED SCHEMA===============")
     // joined.printSchema()
      //val s = fs.select($"transcriptId")//.groupByKey(row=>row.getAs[String]("transcriptId"))
      //pprint.pprintln(s)
      //println("/////////////////////////////////////")
      //pprint.pprintln(j.toList.map(r=>r.schema.toString()))
      //println("-----------------------------------")
      //println(grouped.count().count())

      /*
      val resultDataset: Dataset[(String, String)] = fragments.extractFeatures(features, FeatureType.Exon, r=>r.getAs[String]("transcriptId"))
      val result = resultDataset.collect.toSet
      result.size shouldEqual(3)
      result shouldEqual Set(
        ("exon1", "ACAGCTGATCTCCAGATATGACCATGGGTT"),
        ("exon2", "CAGCTGATCTCCAGATATGACCATGGGTTT"),
        ("exon3" , "CCAGAAGTTTGAGCCACAAACCCATGGTCA")
      )
      */

    }
  }
}