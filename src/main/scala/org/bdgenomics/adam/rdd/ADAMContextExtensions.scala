package org.bdgenomics.adam.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.FastaConverter
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.utils.instrumentation.Metrics
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Feature

/**
  * Created by antonkulaga on 3/27/17.
  */
object ADAMContextExtensions {

  implicit class spExt(val sparkContext: SparkContext) extends HDFSFilesExtensions{

    def loadFastaPersistent(
                   filePath: String,
                   fragmentLength: Long = 10000L): NucleotideContigFragmentRDD = {
      val fastaData: RDD[(LongWritable, Text)] = sparkContext.newAPIHadoopFile(
        filePath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text]
      )
      if (Metrics.isRecording) fastaData.instrument() else fastaData

      val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

      // convert rdd and cache
      val fragmentRdd = FastaConverter(remapData, fragmentLength)
        .persist(StorageLevels.MEMORY_AND_DISK)

      NucleotideContigFragmentRDD(fragmentRdd)
    }

    def mergeFeatures(features: List[FeatureRDD]): Option[FeatureRDD] = features match {
      case Nil => None
      case head :: Nil => Some(head)
      case head :: tail =>
        val merged = tail.foldLeft(head){
          case (acc, feature) =>
            val joined = acc.broadcastRegionJoin(feature)
            acc.transform(_ => joined.rdd.map{
              case (one, two) =>
                one.setStart(Math.min(one.getStart, two.getStart))
                one.setEnd(Math.max(one.getEnd, two.getEnd))
                one
            })
        }
        Some(merged)
    }

  }


}
