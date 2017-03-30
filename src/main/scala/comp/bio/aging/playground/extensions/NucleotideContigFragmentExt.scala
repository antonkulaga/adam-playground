package comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import comp.bio.aging.playground.extensions.stringSeqExtensions._

class NucleotideContigFragmentExt(val fragment: NucleotideContigFragment) extends AnyVal{

  def regionOpt = ReferenceRegion(fragment)

  def hasRegion: Boolean = regionOpt.isDefined

  def region: ReferenceRegion = regionOpt.get

  def subfragments(substring: String): List[NucleotideContigFragment] = {
    substring.inclusionsInto(fragment.getFragmentSequence).map(l=> subfragment(substring, l:Long))
  }

  def subregions(substring: String, inclusionsInto: (String, String) => List[Int]): List[ReferenceRegion] = {
    val r = this.region
    val len = substring.length
    inclusionsInto(substring, fragment.getFragmentSequence)
      .map{i=>
        val s = r.start + i.toLong
        val e = s + len
        r.copy(start = s, end = e)
      }
  }

  def subregions(substring: String): List[ReferenceRegion] = {
    val r = this.region
    val len = substring.length
    substring.inclusionsInto(fragment.getFragmentSequence)
      .map{i=>
        val s = r.start + i.toLong
        val e = s + len
        r.copy(start = s, end = e)
      }
  }

  def subfragment(substring: String, index: Long): NucleotideContigFragment = {
    NucleotideContigFragment
      .newBuilder(fragment)
      .setFragmentStartPosition(fragment.getFragmentStartPosition + index)
      .setFragmentEndPosition(fragment.getFragmentStartPosition + (index + substring.length))
      .setFragmentNumber(null)
      .setFragmentSequence(substring)
      .setFragmentLength(substring.length: Long)
      .build()
  }

}
