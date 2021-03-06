package comp.bio.aging.playground.extensions

import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

trait DataFrameExtensions {


  implicit class DataFrameExtensions(dataFrame: DataFrame) {

    def writeTSV(path: String, header: Boolean = true, sep: String = "\t"): Unit =
      dataFrame.write.option("sep", sep).option("header",header).csv(path)

    def rank(name: String, rankSuffix: String = "_rank") =
      dataFrame.withColumn(name + rankSuffix, org.apache.spark.sql.functions.dense_rank().over(Window.orderBy(new ColumnName(name).desc)))

    def ranks(names: Seq[String],
              rankSuffix: String = "_rank") = names.foldLeft(dataFrame){
      case (f, n)=> f.rank(n, rankSuffix)
    }

    def toVectors(columns: Seq[String], output: String): DataFrame = {
      import org.apache.spark.ml.feature.VectorAssembler
      import org.apache.spark.ml.linalg.Vectors

      val assembler = new VectorAssembler()
        .setInputCols(columns.toArray)
        .setOutputCol(output)

      assembler.transform(dataFrame.na.fill(0.0, columns).na.fill(0.0)).select(output)
    }

    import org.apache.spark.ml.feature.PCA
    import org.apache.spark.ml.linalg.Vectors
    def fitPCA(columns: Seq[String], k: Int)(implicit sparkSession: SparkSession): PCAModel = {
      val df = dataFrame.toVectors(columns, "features")
      new PCA()
        .setInputCol("features")
        .setOutputCol("PCA")
        .setK(k)
        .fit(df)
    }


    def doPCA(columns: Seq[String], k: Int)(implicit sparkSession: SparkSession): DataFrame = {
      val pca = dataFrame.fitPCA(columns, k)
      pca.transform(dataFrame)
    }


    protected def convertCorrellationMatrix(matrix: Matrix, columns: Seq[String]) = {
      require(columns.size == matrix.numCols)
      for(r <- 0 until matrix.numRows) yield {
        val seq = for(c <- 0 until matrix.numCols) yield matrix(r, c)
        Row.fromSeq(columns(r)::seq.toList)
      }
    }

    import org.apache.spark.sql.types._

    def doublesByColumns(columns: Seq[String]) = columns.map(c=>StructField(c, DoubleType, false)).toList

    def transformCorrellationMatrix(dataFrame: DataFrame, columns: Seq[String])(implicit sparkSession: SparkSession) = {
      import sparkSession.implicits._
      val rows  = dataFrame.rdd
        .flatMap{ case Row(matrix: Matrix) => convertCorrellationMatrix(matrix, columns) }
      sparkSession.createDataFrame(rows, StructType(StructField("column", StringType, false)::doublesByColumns(columns)))
    }

    def pearsonCorrellation(columns: Seq[String])(implicit sparkSession: SparkSession)  = {
      val cor = dataFrame.toVectors(columns.toSeq, "features")
      val df = Correlation.corr(cor, "features")
      transformCorrellationMatrix(df, columns)
    }


    def spearmanCorrellation(columns: Seq[String])(implicit sparkSession: SparkSession)  = {
      val cor = dataFrame.toVectors(columns.toSeq, "features").persist(StorageLevel.MEMORY_AND_DISK)
      import org.apache.spark.ml.linalg.Matrix
      val df = Correlation.corr(cor, "features", method = "spearman")
      transformCorrellationMatrix(df, columns)
    }

    def rename(renamings: Map[String, String]) =   {
      val newColumns = dataFrame.columns.map(c=> renamings.getOrElse(c, c))
      dataFrame.toDF(newColumns:_*)
    }


  }

}
