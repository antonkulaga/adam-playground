package comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._

class NucleotideContigFragmentExt(val fragment: NucleotideContigFragment) extends AnyVal{

  def regionOpt = ReferenceRegion(fragment)

  def hasRegion = regionOpt.isDefined

  def region = regionOpt.get
}
