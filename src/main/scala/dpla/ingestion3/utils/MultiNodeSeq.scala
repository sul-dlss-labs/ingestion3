package dpla.ingestion3.utils

import scala.xml.NodeSeq

/**
  * This class is an enhancement on the Scala XML path walking syntax that allows for the
  * union of several alternate paths expressed as a collection of strings.
  * @param targetNodeSeq NodeSeq to be matched against
  */
class MultiNodeSeq(targetNodeSeq: NodeSeq) {
  def \(thats: Iterable[String]): NodeSeq = {
    (for (that <- thats) yield targetNodeSeq \ that).flatten.toSeq
  }
  def \\(thats: Iterable[String]): NodeSeq = {
    (for (that <- thats) yield targetNodeSeq \\ that).flatten.toSeq
  }
}


object MultiNodeSeq {
  //implicit makes the helper automatically work
  implicit def nodeSeqToMultiNodeSeq(targetNodeSeq: NodeSeq): MultiNodeSeq = new MultiNodeSeq(targetNodeSeq)
}
