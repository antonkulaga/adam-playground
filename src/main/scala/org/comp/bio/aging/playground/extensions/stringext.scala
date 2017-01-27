package org.comp.bio.aging.playground.extensions

import scala.annotation.tailrec

/**
  * Created by antonkulaga on 1/27/17.
  */
object stringext {

  implicit def convert(str: String): StringSeq = new StringSeq(str)

  /**
    * Implicit class that extends string with sequences with extra methods
    * @param str
    */
  class StringSeq(val str: String) extends AnyVal
  {
    def complement = str.toUpperCase.map{
      case 'A' => 'T'
      case 'T' => 'A'
      case 'G' => 'C'
      case 'C' => 'G'
      case other => other //not sure if I should throw here
    }

    @tailrec final def inclusionsInto(where: String, start: Int = 0, acc: List[Int] = Nil): List[Int] = where.indexOf(str, start) match {
      case -1 => acc.reverse
      case index => inclusionsInto(where, index +1, index :: acc)
    }

  }


}


