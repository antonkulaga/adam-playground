package comp.bio.aging.playground

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}

class SparkLogger(sparkContext: SparkContext) {

  import scala.reflect.ClassTag

  def log[T: ClassTag](rdd: RDD[T]): RDD[T] = {
    org.apache.spark.rdd.MetricsContext.rddToInstrumentedRDD[T](rdd).instrument
  }

  lazy val metricsListener = new MetricsListener(new RecordedMetrics())

  def init(): SparkLogger = {
    //note is called by default
    Metrics.initialize(sparkContext)
    sparkContext.addSparkListener(metricsListener)
    this
  }

  def lines: List[String] = {
    val stages = metricsListener.metrics.sparkMetrics.stageTimes.toList
    for (stage <- stages)
      yield s"${stage.stageName.getOrElse("")} ${stage.duration} id = ${stage.stageId}"
  }

  def stop = {
    Metrics.stopRecording()
    this
  }

  def prettyPrint() = {
    import java.io.{PrintWriter, StringWriter}
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    val result = stringWriter.toString
    writer.close()
    result
  }

  init()
}
