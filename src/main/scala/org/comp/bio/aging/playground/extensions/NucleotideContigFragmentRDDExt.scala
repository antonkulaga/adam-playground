package org.comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._

import scala.collection.JavaConverters._
import scala.collection.immutable._

class NucleotideContigFragmentRDDExt(val fragments: NucleotideContigFragmentRDD) extends AnyVal{

  def extract(feature: Feature): String =
  {
    val region: ReferenceRegion = ReferenceRegion.apply(feature.getContigName, feature.getStart, feature.getEnd, feature.getStrand)
    fragments.extract(region)
  }

  def search(sequence: String, forceMerge: Boolean = false): RDD[ReferenceRegion] = {
    import stringext._
    val rdd = if(forceMerge) fragments.mergeFragments().rdd else fragments.rdd
    rdd.collect{
      case frag if frag.hasRegion && frag.getFragmentSequence.contains(sequence) =>
        val region = frag.region
        sequence.inclusionsInto(frag.getFragmentSequence)
          .map(i=>region.copy(start = region.start + i,
            end = region.start +i + sequence.length))
    }.flatMap(r=>r)
  }

  def search(sequence: String, features: FeatureRDD): RDD[(Feature, Seq[ReferenceRegion])] = {
    val regions: List[ReferenceRegion] = this.search(sequence).collect().toList
    features.filterByContainedRegions(regions)
  }
}
