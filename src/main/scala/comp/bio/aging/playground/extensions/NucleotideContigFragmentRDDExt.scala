package comp.bio.aging.playground.extensions

import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.bdgenomics.adam.models.{ReferenceRegion, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._

import scala.collection.immutable._


class NucleotideContigFragmentRDDExt(val fragments: NucleotideContigFragmentRDD) extends AnyVal {

  def transformSequences(collectFunction: PartialFunction[SequenceRecord, SequenceRecord]): NucleotideContigFragmentRDD = {
    val newDic = new SequenceDictionary(fragments.sequences.records.collect(collectFunction))
    fragments.replaceSequences(newDic)
  }

  def getTotalLength: Double = fragments.rdd.map(f=>f.region.length()).sum()

  def coveredByFeatures(featureRDD: FeatureRDD): NucleotideContigFragmentRDD = {
    fragments.transform{rdd=>
      fragments.broadcastRegionJoin(featureRDD).rdd.filter{
      case (fragment, feature) => feature.region.covers(fragment.region)
      }.keys.distinct()
    }
  }

  def splitByCoverage(features2: FeatureRDD) = {
    val joined = fragments.leftOuterShuffleRegionJoin(features2).rdd.cache()
    val notCovered = joined.filter{
      case (f, None) => true
      case (f1, Some(f2)) => !f2.region.covers(f1.region)
    }.keys.distinct()
    val covered = joined.filter{
      case (f1, Some(f2)) => f2.region.covers(f1.region)
      case _ => false
    }.keys.distinct()
    (fragments.transform(_ => covered), fragments.transform(_ => notCovered))
  }

  def findRegions(sequences: List[String], flank: Boolean = false): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences.distinct:_*) else fragments
    frags.rdd.flatMap{ frag =>
      val subregions = sequences.map{  str=> str -> frag.subregions(str) }
      subregions
    }.reduceByKey(_ ++ _)
  }

  protected def regionsWithMismatches(frags: NucleotideContigFragmentRDD,
                                          sequences: List[String],
                                          maxMismatches: Int): RDD[(String, List[ReferenceRegion])] = {
    frags.rdd.flatMap{ frag =>
      sequences.map{ str=> str -> frag.subregionsWithMismatches(str, maxMismatches) }
    }.reduceByKey(_ ++ _)
  }

  def findRegionsWithMismatches(sequences: List[String],
                                maxMismatches: Int,
                                reverseComplement: Boolean = false,
                                flank: Boolean = false): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences.distinct:_*) else fragments
    if(reverseComplement) {
      val pairs = sequences.map(s=> s-> s.reverse.complement)
      val regions = frags.rdd.flatMap{ frag =>
        pairs.map{
          case (str, rev) =>
              str -> (frag.subregionsWithMismatches(str, maxMismatches) ++
                  frag.subregionsWithMismatches(rev, maxMismatches)
          )
        }
      }
      regions.reduceByKey(_ ++ _)
    } else regionsWithMismatches(frags, sequences, maxMismatches)

  }

  def findSpecialRegions(sequences: List[String], flank: Boolean = false)
                        (inclusionsInto: (String, String) => List[Int]): RDD[(String, List[ReferenceRegion])] = {
    val frags = if(flank) flankFrom(sequences.distinct:_*) else fragments
    frags.rdd.flatMap{ frag =>
      val subregions = sequences.map{  str=> str -> frag.subregions(str, inclusionsInto) }
      subregions
    }.reduceByKey(_ ++ _)
  }
  /*
  def findRegionsWithMismatches(sequences: List[String], flank: Boolean = false) = findSpecialRegions(sequences){
    case ()
  }
  */

  /**
    * From a set of contigs, returns a list of sequences based on reference regions provided
    * @param regions List of Reference regions over which to get sequences
    * @return RDD[(ReferenceRegion, String)] of region -> sequence pairs.
    */
   def extractRegions(regions: Iterable[ReferenceRegion]): RDD[(ReferenceRegion, String)] = {

    def extractSequence(fragmentRegion: ReferenceRegion, fragment: NucleotideContigFragment, region: ReferenceRegion): (ReferenceRegion, String) = {
      val merged = fragmentRegion.intersection(region)
      val start = (merged.start - fragmentRegion.start).toInt
      val end = (merged.end - fragmentRegion.start).toInt
      val fragmentSequence: String = fragment.getSequence
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
      case frag if frag.hasRegion && frag.getSequence.contains(sequence) =>
        val region = frag.region
        sequence.inclusionsInto(frag.getSequence)
          .map(i=>region.copy(start = region.start + i,
            end = region.start +i + sequence.length))
    }
    found.flatMap(list=>list)
  }

  def search(sequence: String, features: FeatureRDD): RDD[(Feature, Seq[ReferenceRegion])] = {
    val regions: List[ReferenceRegion] = this.search(sequence).collect().toList
    features.filterByContainedRegions(regions)
  }

  def filterByContigNames(filterFun: String => Boolean): Unit ={
    fragments
      .transform(rdd=>rdd.filter(c=>filterFun(c.getContigName)))
      .transformSequences{ case s if filterFun(s.name) => s }
  }

  def saveContig(path: String, name: String): Unit ={
    val filtered = fragments.transform(tr=>tr.filter(c=>c.getContigName==name))
    val cont = filtered.transformSequences{ case s if s.name==name => s }
    cont.saveAsParquet(s"${path}/${name}.adam")
  }

  /*
  def extractFeatures(features: Iterable[Feature]) = {

    //fragments.extractRegions()
  }
  */

  def mergeFeatures(key: String, iter: Iterator[Row]) = {
    val sorted = iter.toList.sortBy(r=>r.getAs[Long]("fragment_start"))
    val sequence = iter.toList match {
      case head::Nil =>
        val start =  head.getAs[Long]("start")
        val end =  head.getAs[Long]("end")
        val fragmentStart = head.getAs[Long]("fragment_start")
        val fragmentEnd = head.getAs[Long]("fragment_end")
        val startIndex = (Math.max(start, fragmentEnd) - fragmentEnd).toInt
        val endIndex = (Math.min(end, fragmentEnd) - fragmentStart).toInt
        val str: String = head.getAs[String]("sequence").substring(startIndex, endIndex)
        //if(head.getAs[String]("strand").toUpperCase == "REVERSE") str.reverse else str
        str

      case head::tail =>
        val seq = head.getAs[String]("sequence")
        val start =  head.getAs[Long]("start")
        val end =  head.getAs[Long]("end")
        val str = tail.foldLeft(seq){
          case (acc, el) =>
            val fragmentStart = el.getAs[Long]("fragment_start")
            val fragmentEnd = el.getAs[Long]("fragment_end")
            val startIndex = (Math.max(start, fragmentEnd) - fragmentEnd).toInt
            val endIndex = (Math.min(end, fragmentEnd) - fragmentStart).toInt
            val str = head.getAs[String]("sequence").substring(startIndex, endIndex)
            acc + el
        }
        //if( (end - start).toInt != str.length) print(s"LENGTH IS DIFFERENT!")
        //if(head.getAs[String]("strand").toUpperCase == "REVERSE") str.reverse else str
        str

      case _ => ""
    }
    key -> sequence
  }

  def extractFeatures(features: FeatureRDD,
                      featureType: FeatureType,
                      getKey: Row => String)(implicit session: SparkSession): Dataset[(String, String)] = {
    import session.implicits._
    val encode = Encoders.tuple[String, String](Encoders.STRING, Encoders.STRING)
    val grouped: KeyValueGroupedDataset[String, Row] = extractFeatureGroups(features, featureType, getKey)(session)
    //grouped.mapGroups{ (key, iter) => ("", "") }.as[Tuple2[String, String]](encode)
    grouped.mapGroups[(String, String)]{ (key: String, iter: Iterator[Row]) => mergeFeatures(key, iter) }(encode)
  }

  def extractFeatureGroups(features: FeatureRDD,
                              featureType: FeatureType,
                              getKey: Row => String
                             )(implicit session: SparkSession): KeyValueGroupedDataset[String, Row]
    /*(implicit session: org.apache.spark.sql.SparkSession)*/ = {
    import session.implicits._
    val frags = fragments.toDF()
      .withColumnRenamed("start", "fragment_start")
      .withColumnRenamed("end", "fragment_end")
    val fs = features.dataset
    val tp = featureType.entryName
    val grouping = featureType match {
      case FeatureType.Gene => "geneId"
      case FeatureType.Exon => "exonId"
      case _ => "transcriptId"
    }

    frags.join(fs,
      fs("start") <= frags("fragment_end")
        &&  fs("end") >= frags("fragment_start")
        && fs("contigName") === frags("contigName")
        && fs("featureType") === tp
    ).groupByKey(getKey)
  }

}
