package comp.bio.aging.playground

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Extensions for standard ADAM classes
  */
package object extensions  extends ReadExtensions with DataFrameExtensions {

  implicit def nucleotideContigFragmentRDDExtended(fragments: NucleotideContigFragmentRDD): NucleotideContigFragmentRDDExt = new NucleotideContigFragmentRDDExt(fragments)
  implicit def featuresRDDExtended(features: FeatureRDD): FeatureRDDExt = new FeatureRDDExt(features)

  implicit def nucleotideContigFragmentExtended(fragment: NucleotideContigFragment): NucleotideContigFragmentExt = new NucleotideContigFragmentExt(fragment)
  implicit def featuresExtended(feature: Feature): FeatureExt = new FeatureExt(feature)



  implicit class spExt(val sparkContext: SparkContext) extends HDFSFilesExtensions {

    def mergeFeatures(features: List[FeatureRDD]): Option[FeatureRDD] = features match {
      case Nil => None
      case head :: Nil => Some(head)
      case head :: tail =>
        val merged = tail.foldLeft(head){
          case (acc, feature) =>
            val joined = acc.broadcastRegionJoin(feature)
            acc.transform(_ => joined.rdd.map{
              case (one, two) =>
                one.setStart(Math.min(one.getStart, two.getStart))
                one.setEnd(Math.max(one.getEnd, two.getEnd))
                one
            })
        }
        Some(merged)
    }

    def joinDataFrames(dfs: Seq[DataFrame], fields: Seq[String], joinType: String = "inner"): DataFrame = {
      dfs.reduce((a, b)=> a.join(b, fields, joinType))
    }
  }

}
