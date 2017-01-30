package comp.bio.aging.playground

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.contig._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._


object Main {

  val seq = "tgcggaccgggcagggggaccctttggtttatccccaatgtggcctacgttcgggaagaggaggcaccccaaaaagctgacccctttagcctacaagcagtttatccccaatgtggccgaccccttagcctacgagaagaccctaggcgccagcggaaggtatgaaggacccctttagctacggaagatctccagaaactccgagcgatttaaggaactcacccccaattacaaccccgacatcatatttaaggatgaagaaaacaccggagcggacaggctgatgactcagaggtgtaaggacaagttgaacgctttggccatctcggtgatgaaccagtggccaggagtgaaactgcgggtgaccgagggctgggacgaagatggccaccactcagaggagtctctgcactacgagggccgcgcagtggacatcaccacgtctgaccgcgaccgcagcaagtacggcatgctggcccgcctggcggtggaggccggcttcgactgggtgtactacgagtccaaggcacatatccactgctcggtgaaagcagagaactcggtggcggccaaatcgggaggctgcttcccgggctcggccacggtgcacctggagcagggcggcaccaagctggtgaaggacctgagccccggggaccgcgtgctggcggcggacgaccagggccggctgctctacagcgacttcctcactttcctggaccgcgacgacggcgccaagaaggtcttctacgtgatcgagacgcgggagccgcgcgagcgcctgctgctcaccgccgcgcacctgctctttgtggcgccgcacaacgactcggccaccggggagcccgaggcgtcctcgggctcggggccgccttccgggggcgcactggggcctcgggcgctgttcgccagccgcgtgcgcccgggccagcgcgtgtacgtggtggccgagcgtgacggggaccgccggctcctgcccgccgctgtgcacagcgtgaccctaagcgaggaggccgcgggcgcctacgcgccgctcacggcccagggcaccattctcatcaaccgggtgctggcctcgtgctacgcggtcatcgaggagcacagctgggcgcaccgggccttcgcgcccttccgcctggcgcacgcgctcctggctgcactggcgcccgcgcgcacggaccgcggcggggacagcggcggcggggaccgcgggggcggcggcggcagagtagccctaaccgctccaggtgctgccgacgctccgggtgcgggggccaccgcgggcatccactggtactcgcagctgctctaccaaataggcacctggctcctggacagcgaggccctgcacccgctgggcatggcggtcaagtccagc"
  val search = "gaccg"


  def time[R](name: String)(block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block
    // call-by-name
    val t1 = System.nanoTime()
    println(s"${name} took: " + (t1 - t0) + "ns")
    (result, t1 - t0)
  }

  def main(args: Array[String]): Unit = {

  }
}
