package comp.bio.aging.playground.extensions

import enumeratum._

sealed abstract class FeatureType(override val entryName: String) extends EnumEntry

object FeatureType extends Enum[FeatureType] {

  val values = findValues

  case object StopCodon extends FeatureType("stop_codon")
  case object Transcript extends FeatureType("transcript")
  case object Gene extends FeatureType("gene")
  case object StartCodon extends FeatureType("start_codon")
  case object Selenocysteine extends FeatureType("Selenocysteine")
  case object Exon extends FeatureType("exon")
  case object UTR extends FeatureType("UTR")
  case object CDS extends FeatureType("CDS")
}