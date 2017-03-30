package comp.bio.aging.playground.extensions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import scala.collection.JavaConverters._
import org.bdgenomics.adam.rdd.ADAMContext._

import scala.Iterable
import scala.collection.immutable._

/**
  * Extends features RDD with some useful methods
  * @param features
  */
class FeatureRDDExt(val features: FeatureRDD) {

  lazy val featuresByRegion = features.rdd.keyBy(f=>f.getRegion)

  def transformSequences(collectFunction: PartialFunction[SequenceRecord, SequenceRecord]) = {
    val newDic = new SequenceDictionary(features.sequences.records.collect(collectFunction))
    features.copy(sequences = newDic)
  }


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

  def ofType(tp: String): FeatureRDD = features.transform(rdd=>rdd.filter(f=>f.getFeatureType==tp))

  def transcripts: FeatureRDD = ofType("transcript")

  def genes: FeatureRDD = ofType("gene")

  def exons: FeatureRDD = ofType("exon")

  def byStrand(strand: Strand): RDD[Feature] = features.rdd.filter(f=>f.getStrand == Strand.INDEPENDENT || f.getStrand == Strand.UNKNOWN)

  def byType: RDD[(String, Iterable[Feature])] = features.rdd.groupBy(f=>f.getFeatureType)

  def filterAttributes(filterFun: scala.collection.Map[String, String] => Boolean): FeatureRDD = features.transform(rdd =>
    rdd.filter{ f=> filterFun(f.getAttributes.asScala) }
  )

  def filterByAttribute(name: String)(filterFun: String => Boolean): FeatureRDD =features.transform(rdd =>
    rdd.filter{ f=> f.getAttributes.containsKey(name) && filterFun(f.getAttributes.get(name)) }
  )

  def byContig(name: String): FeatureRDD = {
    val filtered = features.transform(tr=>tr.filter(c=>c.getContigName==name))
    filtered.transformSequences{ case s if s.name==name => s }
  }

  def saveContigFeatures(path: String, name: String): Unit ={
    this.byContig(name).saveAsParquet(s"${path}/${name}Features.adam")
  }


}
