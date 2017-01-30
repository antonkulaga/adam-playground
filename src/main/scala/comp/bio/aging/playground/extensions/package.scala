package comp.bio.aging.playground

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Extensions for standard ADAM classes
  */
package object extensions {

  implicit def nucleotideContigFragmentRDDExtended(fragments: NucleotideContigFragmentRDD): NucleotideContigFragmentRDDExt = new NucleotideContigFragmentRDDExt(fragments)
  implicit def featuresRDDExtended(features: FeatureRDD): FeatureRDDExt = new FeatureRDDExt(features)

  implicit def nucleotideContigFragmentExtended(fragment: NucleotideContigFragment): NucleotideContigFragmentExt = new NucleotideContigFragmentExt(fragment)
  implicit def featuresExtended(feature: Feature): FeatureExt = new FeatureExt(feature)

}
