package comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import comp.bio.aging.playground.extensions.stringSeqExtensions._

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.math.max


class NucleotideContigFragmentRDDExt(val fragments: NucleotideContigFragmentRDD) extends AnyVal{


  def transformSequences(collectFunction: PartialFunction[SequenceRecord, SequenceRecord]) = {
    val newDic = new SequenceDictionary(fragments.sequences.records.collect(collectFunction))
    fragments.copy(sequences = newDic)
  }

  def trimStringByRegion(fragment: (ReferenceRegion, NucleotideContigFragment), region: ReferenceRegion): (ReferenceRegion, String) = {
    val trimStart = max(0, region.start - fragment._1.start).toInt
    val trimEnd = max(0, fragment._1.end - region.end).toInt

    val fragmentSequence: String = fragment._2.getFragmentSequence

    val str = fragmentSequence.drop(trimStart)
      .dropRight(trimEnd)
    val reg = new ReferenceRegion(
      fragment._1.referenceName,
      fragment._1.start + trimStart,
      fragment._1.end - trimEnd
    )
    (reg, str)
  }


  protected def reduceRegionSequences(
                   kv1: (ReferenceRegion, String),
                   kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
    assert(kv1._1.isAdjacent(kv2._1), "Regions being joined must be adjacent. For: " +
      kv1 + ", " + kv2)

    (kv1._1.merge(kv2._1), if (kv1._1.compareTo(kv2._1) <= 0) {
      kv1._2 + kv2._2
    } else {
      kv2._2 + kv1._2
    })
  }


  def extractRegions(regions: List[ReferenceRegion]): RDD[(ReferenceRegion, (ReferenceRegion, String))] = {
      val byRegion = fragments.rdd.keyBy(ReferenceRegion(_))
      val places: RDD[(ReferenceRegion, (ReferenceRegion, String))] = byRegion
          .flatMap{
            case (Some(reg), fragment) if  regions.exists(reg.overlaps) =>
              regions.collect{
                case region if reg.overlaps(region) => (region, trimStringByRegion((reg, fragment), region))
              }

            case _ => Nil
          }
      places.reduceByKey(reduceRegionSequences)
  }

  /**
    * Extracts feature
    * @param feature
    * @param withStrand there is a bug in ADAM and sometimes reverse strand is not extracted
    * @return Sequence of the feature
    */
  def extract(feature: Feature, withStrand: Boolean = false): String =
  {
    val region: ReferenceRegion = {
      if(withStrand)
        ReferenceRegion.apply(feature.getContigName, feature.getStart, feature.getEnd, feature.getStrand)
      else
        ReferenceRegion.apply(feature.getContigName, feature.getStart, feature.getEnd)
    }
    fragments.extract(region)
  }

  def search(sequence: String, flank: Boolean = true): RDD[ReferenceRegion] = {
    val rdd: RDD[NucleotideContigFragment] = if (flank) fragments.flankAdjacentFragments(sequence.length - 1).rdd else fragments.rdd
    val found = rdd.collect{
      case frag if frag.hasRegion && frag.getFragmentSequence.contains(sequence) =>
        val region = frag.region
        sequence.inclusionsInto(frag.getFragmentSequence)
          .map(i=>region.copy(start = region.start + i,
            end = region.start +i + sequence.length))
    }
    found.flatMap(list=>list)
  }

  def search(sequence: String, features: FeatureRDD): RDD[(Feature, Seq[ReferenceRegion])] = {
    val regions: List[ReferenceRegion] = this.search(sequence).collect().toList
    features.filterByContainedRegions(regions)
  }

  def saveContig(path: String, name: String) ={
    val filtered = fragments.transform(tr=>tr.filter(c=>c.getContig.getContigName==name))
    val cont = filtered.transformSequences{ case s if s.name==name => s }
    cont.saveAsParquet(s"${path}/${name}.adam")
  }

}
