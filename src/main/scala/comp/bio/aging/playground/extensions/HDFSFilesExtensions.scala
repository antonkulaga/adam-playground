package comp.bio.aging.playground.extensions

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
/**
  * Adds HDFS-related features
  */
trait HDFSFilesExtensions {

  def sparkContext: SparkContext

  def openFolder(where: String): (List[String], List[String]) = {
    import org.apache.hadoop.fs._
    val hadoopConfig = sparkContext.hadoopConfiguration
    val fs = FileSystem.get( hadoopConfig )
    val pathes = fs.listStatus(new Path(where)).toList
    val (dirs, files) = pathes.partition(s=>s.isDirectory && !s.getPath.getName.toLowerCase.endsWith(".adam"))
    val dirPathes = dirs.map(d=>d.getPath.toString)
    val filePathes = files.map(d=>d.getPath.toString)
    (dirPathes, filePathes)
  }

  def openFolderRecursive(where: String): List[String] = {
    import org.apache.hadoop.fs._
    val hadoopConfig = sparkContext.hadoopConfiguration
    val fs = FileSystem.get( hadoopConfig )
    val pathes = fs.listStatus(new Path(where)).toList
    val (dirs, files) = pathes.partition(s=>s.isDirectory && !s.getPath.getName.toLowerCase.endsWith(".adam"))
    files.map(d=>d.getPath.toString) ++
      dirs.map(d=>d.getPath.toString).flatMap(openFolderRecursive)
  }


  def loadNarrowPeaksByContig(where: String, contigName: String): List[FeatureRDD] = {
    openFolderRecursive(where)
      .map(sparkContext.loadNarrowPeak(_)
        .byContig(contigName))
  }

  def loadBedsByContig(where: String, contigName: String, innerJoin: Boolean = true): List[FeatureRDD] = {
    openFolderRecursive(where)
      .map(sparkContext.loadBed(_)
        .byContig(contigName))
  }

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs._

  def merge(srcPath: String, dstPath: String, deleteAfterMerge: Boolean = false): Boolean =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), deleteAfterMerge, hadoopConfig, null)
    // the "true" setting deletes the source files once they are merged into the new output
  }


}
