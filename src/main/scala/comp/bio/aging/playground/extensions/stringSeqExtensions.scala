package comp.bio.aging.playground.extensions

import scala.annotation.tailrec
import scala.collection.immutable._

/**
  * Created by antonkulaga on 1/27/17.
  */
object stringSeqExtensions {

  implicit def convert(str: String): StringSeq = new StringSeq(str)

  /*  *
    * Implicit class that extends string with sequences with extra methods
    * @param str
    */
  class StringSeq(val string: String) extends AnyVal
  {
    def complement = string.toUpperCase.map{
      case 'A' => 'T'
      case 'T' => 'A'
      case 'G' => 'C'
      case 'C' => 'G'
      case other => other //not sure if I should throw here
    }

    @tailrec final def inclusionsInto(where: String, start: Int = 0, acc: List[Int] = Nil): List[Int] = where.indexOf(string, start) match {
      case -1 => acc.reverse
      case index => inclusionsInto(where, index +1, index :: acc)
    }


    /*
    @tailrec final def inclusionsIntoKMP(where: String,
                                         start: Int = 0,
                                         acc: List[Int] = Nil,
                                         prefixes: Array[Int] = Array.empty[Int]): List[Int] = {
      val prefs = if(prefixes.isEmpty) string.prefixesKMP else prefixes
      where.indexOfKMP(string, prefs, start) match {
        case -1 => acc.reverse
        case index => this.inclusionsIntoKMP(where, index +1, index :: acc, prefs)
      }
    }

    def indexOfKMP(pattern: String, start: Int): Int =  {
      val prefixes = pattern.prefixesKMP
      println("prefixes = "+prefixes.toList)
      indexOfKMP(pattern, prefixes, start)
    }

    def indexOfKMP(pattern: String, prefixes: Array[Int], start: Int): Int =
      if(string.length < pattern.length + start)
        -1
      else
      {
        var j = 0
        for(i <- start until string.length) {
          while(j > 0 && string.charAt(i) != pattern.charAt(j)){
            j = prefixes(j - 1)
          }
          if(string.charAt(i)==pattern.charAt(j)) {
            j = j+1
            if(j==pattern.length) return i - (j - 1)
          }
        }
        -1
      }

    def prefixesKMP: Array[Int] = {
      val prefixes = new Array[Int](string.length)
      for(i <- 1 until prefixes.length){
        var j = prefixes(i-1)
        while(i > 0 && string.charAt(i) != string.charAt(i))
          j = prefixes(i -1)
        if(string.charAt(i)==string.charAt(j)) j = j+1
        prefixes(i) = j
      }
      prefixes
    }
    */


  }


}


