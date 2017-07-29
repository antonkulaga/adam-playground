package test
import comp.bio.aging.playground._
import comp.bio.aging.playground.extensions._
object Main {

  def time[R](name: String)(block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis
    val result = block
    // call-by-name
    val t1 = System.currentTimeMillis
    val span = t1 - t0
    writeTime(name, span)
    (result, span)
  }

  def writeTime(name: String, millis: Long): Long = {
    val totalSecs = millis / 1000
    val hours = totalSecs / 3600
    val minutes = (totalSecs % 3600) / 60
    val seconds = (totalSecs % 3600) % 60
    def t(num: Long): String = if(num<10) s"0${num}" else num.toString
    val message = s"${name} took: ${t(hours)} : ${t(minutes)} : ${t(seconds)}"
    println(message)
    totalSecs
  }

}
