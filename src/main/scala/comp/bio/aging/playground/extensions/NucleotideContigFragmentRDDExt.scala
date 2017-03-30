package comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import comp.bio.aging.playground.extensions.stringSeqExtensions._
import comp.bio.aging.playground.extensions._
import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.math.max


class NucleotideContigFragmentRDDExt(val fragments: NucleotideContigFragmentRDD) extends AnyVal{


  def transformSequences(collectFunction: PartialFunction[SequenceRecord, SequenceRecord]): NucleotideContigFragmentRDD = {
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


  def findRegions(sequences: List[String], flank: Boolean = false): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences:_*) else fragments
    frags.rdd.flatMap{ frag =>
      val subregions = sequences.map{  str=> str -> frag.subregions(str) }
      subregions
    }.reduceByKey(_ ++ _)
  }

  def findSpecialRegions(sequences: List[String], flank: Boolean = false)
                        (inclusionsInto: (String, String) => List[Int]): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences:_*) else fragments
    frags.rdd.flatMap{ frag =>
      val subregions = sequences.map{  str=> str -> frag.subregions(str, inclusionsInto) }
      subregions
    }.reduceByKey(_ ++ _)
  }

  def extractRegions(regions: Array[ReferenceRegion]): RDD[(ReferenceRegion, String)] = extractRegions(regions.toList)

  def extractRegions(regions: List[ReferenceRegion]): RDD[(ReferenceRegion, String)] = {

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
          case (Some(reg), fragment) if  regions.exists(reg.overlaps) =>
            regions.collect{
              case region if reg.overlaps(region) => (region, trimStringByRegion((reg, fragment), region))
            }

          case _ => Nil
        }

    val byKey =  places.groupByKey
      .filter{
        case (_, iter) if iter.size >= 2 =>
          iter.sliding(2).map(i=>(i.head, i.tail.head)).exists{ case ((one,_), (two, _))=> !one.isAdjacent(two)}
        case _ => false
      }.mapValues(_.toList.map(_._1))

    places.reduceByKey(reduceRegionSequences).mapValues{ case (_, str) => str}
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
