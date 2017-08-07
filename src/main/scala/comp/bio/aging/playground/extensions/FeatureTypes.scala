package comp.bio.aging.playground.extensions

import enumeratum._

sealed trait FeatureType extends EnumEntry

object FeatureType extends Enum[FeatureType] {

  val values = findValues

  case object StopCodon extends FeatureType{override val entryName: String ="stop_codon"}
  case object Transcript extends FeatureType{override val entryName: String ="transcript"}
  case object Gene extends FeatureType{override val entryName: String ="gene"}
  case object StartCodon extends FeatureType{override val entryName: String ="start_codon"}
  case object Selenocysteine extends FeatureType{override val entryName: String ="Selenocysteine"}
  case object Exon extends FeatureType{override val entryName: String ="exon"}
  case object UTR extends FeatureType{override val entryName: String ="UTR"}
  case object CDS extends FeatureType{override val entryName: String ="CDS"}
}