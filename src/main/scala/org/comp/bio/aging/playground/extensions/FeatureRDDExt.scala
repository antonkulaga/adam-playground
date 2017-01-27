package org.comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import scala.collection.immutable._

/**
  * Created by antonkulaga on 1/27/17.
  */
class FeatureRDDExt(val features: FeatureRDD) {

  lazy val featuresByRegion = features.rdd.keyBy(f=>f.getRegion)


  def filterByContainedRegions(regions: Seq[ReferenceRegion]): RDD[(Feature, Seq[ReferenceRegion])] = {
    featuresByRegion.collect{
      case (fr, f) if regions.exists(r=>fr.contains(r))=>
        f -> regions.filter(r=>fr.contains(r))
    }
  }

  def filterByCoveredRegions(regions: Seq[ReferenceRegion]): RDD[(Feature, Seq[ReferenceRegion])] = {
    featuresByRegion.collect{
      case (fr, f) if regions.exists(r=>fr.covers(r))=>
        f -> regions.filter(r=>fr.covers(r))
    }
  }

}
