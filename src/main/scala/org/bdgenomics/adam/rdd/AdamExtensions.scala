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

/**
  * Created by antonkulaga on 3/27/17.
  */
object AdamExtensions {

  implicit class spExt(sc: SparkContext) {

    def loadFastaPersistent(
                   filePath: String,
                   fragmentLength: Long = 10000L): NucleotideContigFragmentRDD = {
      val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(
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

  }


}
