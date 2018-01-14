package comp.bio.aging.playground.trees

import gstlib.GeneralizedSuffixTree
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD

case object SimpleTree {
  def apply(fragments: NucleotideContigFragmentRDD): SimpleTree =
  {
    SimpleTree(fragments.rdd.map(_.getSequence))
  }

  def apply(strings: RDD[String]): SimpleTree = {
    SimpleTree(GeneralizedSuffixTree(strings.collect():_*))
  }

  def apply(utterances: Seq[String]): SimpleTree = {
    SimpleTree(GeneralizedSuffixTree(utterances:_*))
  }
}

case class SimpleTree(tree: GeneralizedSuffixTree[Char, String]) {

  def repeats: Iterator[(Int, String)] = tree.bulkMultipleCommonSubsequence()

  def repeatsOfMin(minLength: Int, minOccurence: Int): Map[String, Int] = {
    repeats.collect{
      case (occ, pattern) if pattern.length >= minLength &&  occ >= minOccurence =>
        (pattern, occ)
    }.toMap
  }

  def findInclusion(pattern: String): Set[(String, Int)] = {
    tree.find(pattern).map{ case (id, position) => tree.getSequenceBy(id) -> position}.toSet
  }
}