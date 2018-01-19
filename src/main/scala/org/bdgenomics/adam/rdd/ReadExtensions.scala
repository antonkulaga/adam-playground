package org.bdgenomics.adam.rdd

trait ReadExtensions {
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
  import org.apache.spark.sql.types.StructType


  import scala.reflect.runtime.universe._

  implicit class SparkSessionExtensions(session: SparkSession) {
    import session.implicits._

    def readTSV(path: String, header: Boolean = false, sep: String = "\t"): DataFrame = session.read
      .option("sep", sep)
      .option("header", header)
      .csv(path)

    def readTypedTSV[T <: Product](path: String, header: Boolean = false, sep: String = "\t")
                                  (implicit tag: TypeTag[T]) = {
      val encoder: StructType = Encoders.product[T](tag).schema
      session.read
        .option("sep", sep)
        .option("header", header)
        .schema(encoder)
        .csv(path).as[T]
    }

  }
}
