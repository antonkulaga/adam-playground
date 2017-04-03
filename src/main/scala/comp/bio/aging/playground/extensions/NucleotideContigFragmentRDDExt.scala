package comp.bio.aging.playground.extensions

import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._

import scala.collection.immutable._


class NucleotideContigFragmentRDDExt(val fragments: NucleotideContigFragmentRDD) extends AnyVal{

  def transformSequences(collectFunction: PartialFunction[SequenceRecord, SequenceRecord]): NucleotideContigFragmentRDD = {
    val newDic = new SequenceDictionary(fragments.sequences.records.collect(collectFunction))
    fragments.copy(sequences = newDic)
  }

  def getTotalLength: Double = fragments.rdd.map(f=>f.region.length()).sum()

  def coveredByFeatures(featureRDD: FeatureRDD): RDD[NucleotideContigFragment] = {
    fragments.broadcastRegionJoin(featureRDD).rdd.filter{
      case (fragment, feature) => feature.region.covers(fragment.region)
    }.keys.distinct()
  }

  def findRegions(sequences: List[String], flank: Boolean = false): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences.distinct:_*) else fragments
    frags.rdd.flatMap{ frag =>
      val subregions = sequences.map{  str=> str -> frag.subregions(str) }
      subregions
    }.reduceByKey(_ ++ _)
  }

  def findSpecialRegions(sequences: List[String], flank: Boolean = false)
                        (inclusionsInto: (String, String) => List[Int]): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences.distinct:_*) else fragments
    frags.rdd.flatMap{ frag =>
      val subregions = sequences.map{  str=> str -> frag.subregions(str, inclusionsInto) }
      subregions
    }.reduceByKey(_ ++ _)
  }

   def extractRegions(regionsList: List[ReferenceRegion]): RDD[(ReferenceRegion, String)] = {

    val regions = regionsList.distinct //to avoid duplicates

    def extractSequence(fragmentRegion: ReferenceRegion, fragment: NucleotideContigFragment, region: ReferenceRegion): (ReferenceRegion, String) = {
      val merged = fragmentRegion.intersection(region)
      val start = (merged.start - fragmentRegion.start).toInt
      val end = (merged.end - fragmentRegion.start).toInt
      val fragmentSequence: String = fragment.getFragmentSequence
      (merged, fragmentSequence.substring(start, end))
    }

    def reduceRegionSequences(
                               kv1: (ReferenceRegion, String),
                               kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
      (kv1._1.merge(kv2._1), if (kv1._1.compareTo(kv2._1) <= 0) {
        kv1._2 + kv2._2
      } else {
        kv2._2 + kv1._2
      })
    }

    val byRegion: RDD[(Option[ReferenceRegion], NucleotideContigFragment)] = fragments.rdd.keyBy(ReferenceRegion(_))
    val places: RDD[(ReferenceRegion, (ReferenceRegion, String))] = byRegion
        .flatMap{
          case (Some(fragmentRegion), fragment) if  regions.exists(fragmentRegion.overlaps) =>
            regions.collect{
              case region if fragmentRegion.overlaps(region) => (region, extractSequence(fragmentRegion, fragment, region))
            }
          case _ => Nil
        }.sortByKey()

    places.reduceByKey(reduceRegionSequences).values
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

  def flankFrom(sequences: String*): NucleotideContigFragmentRDD = sequences.maxBy(_.length).length - 1 match {
    case value if value > 0 => fragments.flankAdjacentFragments(value)
    case _ => fragments
  }

  def search(sequence: String): RDD[ReferenceRegion] = {
    val found = fragments.rdd.collect{
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

  def saveContig(path: String, name: String): Unit ={
    val filtered = fragments.transform(tr=>tr.filter(c=>c.getContig.getContigName==name))
    val cont = filtered.transformSequences{ case s if s.name==name => s }
    cont.saveAsParquet(s"${path}/${name}.adam")
  }

}
