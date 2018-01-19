package comp.bio.aging.playground.extras.diamond

import scala.util.Try
trait ProteinSearch{
  def id: String
  def e: String
}

case class BlastResult(id: String, score: Double, e: String) extends ProteinSearch
case class PfamResult(domain: String, id: String, e: String) extends ProteinSearch


case object ProteinPrediction {
  def extractPrediction(description: String, sequence: String, contigName: String = ""): ProteinPrediction = {
    val gene::transcript::rest::Nil = description.split("::").toList
    val id::orf::tp::len_string::params::tr::Nil = rest.split(' ').filter(_!="").toList
    val orf_type: String = tp.substring(tp.indexOf(':') + 1)
    val _::etc::Nil = tr.split(':').toList
    val str::score_string::other = params.split(',').toList
    val strand: Char = if(str.contains("-")) '-' else '+'
    val len = Integer.parseInt(
      len_string.substring(len_string.indexOf(':') + 1)
    )
    val (diamondHits, pfamHits) = other.foldLeft((List.empty[BlastResult], List.empty[PfamResult])){
      case ((b, p), el) =>
        val id::value::e::Nil = el.split('|').toList
        Try(value.toDouble).map(
          v=>
            (BlastResult(id, v, e)::b, p)
        ).getOrElse(
          (b, PfamResult(id, value, e)::p)
        )
    }
    val span = etc.substring(0, etc.indexOf('('))
    val start::end::Nil = span.split('-').map(_.toLong).toList
    val score = score_string.substring(score_string.indexOf('=') + 1).toDouble
    ProteinPrediction(
      transcript, id, sequence, orf_type, score, start, end, len, strand, diamondHits, pfamHits, contigName
    )
  }
}
case class ProteinPrediction(transcript: String,
                             id: String,
                             sequence: String,
                             orf_type: String,
                             score: Double,
                             start: Long,
                             end: Long,
                             len: Int,
                             strand: Char,
                             diamondHits: List[BlastResult],
                             pfamHits: List[PfamResult],
                             contigName: String
                            ){
  //def full_id() = s"${gene}::${transcript}::${rest}"
}
  //Gene.10640::c10000_g1_i2::g.10640::m.10640
