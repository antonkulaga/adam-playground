package comp.bio.aging.playground.extensions

import org.apache.spark.sql.{ColumnName, Dataset}
import org.apache.spark.sql.expressions.Window

trait ReadExtensions {

  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

  import scala.reflect.runtime.universe._

  implicit class SparkSessionExtensions(session: SparkSession) {
    import session.implicits._

    def readTSV(path: String, header: Boolean = false, sep: String = "\t"): DataFrame = session.read
      .option("sep", sep)
      .option("comment", "#")
      .option("inferSchema", true)
      .option("header", header)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("maxColumns", 150000)
      .csv(path)

    def readTypedTSV[T <: Product](path: String, header: Boolean = false, sep: String = "\t")
                                  (implicit tag: TypeTag[T]): Dataset[T] = {
      implicit val encoder: StructType = Encoders.product[T](tag).schema
      session.read
        .option("sep", sep)
        .option("comment", "#")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .option("header", header)
        .schema(encoder)
        .csv(path).as[T]
    }

    def rank(df: DataFrame, name: String, rankSuffix: String = "_rank") =
      df.withColumn(name + rankSuffix, org.apache.spark.sql.functions.dense_rank().over(Window.orderBy(new ColumnName(name).desc)))

    def ranks(df: DataFrame,
              names: Seq[String],
              rankSuffix: String = "_rank") = names.foldLeft(df){
      case (f, n)=> rank(f, n, rankSuffix)
    }



  }
}
