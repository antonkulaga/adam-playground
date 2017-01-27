package org.comp.bio.aging.playground

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._

object Main {

  def main(args: Array[String]): Unit = {
    val sc = spark.SparkContext.getOrCreate()
    val file = sc.textFile("hdfs://user/antonkulaga/text.txt")
  }

  /*
  def searchFragments(fragments:  NucleotideContigFragmentRDD, sequence: String, forceMerge: Boolean = false) {
    val contigs = if(forceMerge) fragments.mergeFragments() else fragments
    contigs.rdd.filter(c => c.getFragmentSequence.contains(sequence))
  }


  def searchSequence(fragments:  NucleotideContigFragmentRDD,
                     search: String => Boolean): RDD[(ReferenceRegion, NucleotideContigFragment)] = {
    fragments.rdd.filter(hasValidRegion).keyBy(c=>ReferenceRegion(c).get)
  }

  def getGenes(features: RDD[Feature]) = features.filter(f => f.getFeatureType == "gene")
*/

}
