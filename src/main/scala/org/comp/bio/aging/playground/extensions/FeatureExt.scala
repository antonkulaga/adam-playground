package org.comp.bio.aging.playground.extensions

import org.bdgenomics.adam.models._
import org.bdgenomics.formats.avro._

class FeatureExt(val feature: Feature) extends AnyVal{

  def getRegion: ReferenceRegion = new ReferenceRegion(feature.getContigName, feature.getStart, feature.getEnd, feature.getStrand)

  //TODO: remake
  def relativeRegion(referenceRegion: ReferenceRegion): ReferenceRegion = {
    referenceRegion.copy(start = referenceRegion.start - feature.getStart, end = referenceRegion.end - feature.getEnd)
  }
}
