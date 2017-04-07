package dpla.ingestion3.mappers.nara

import java.net.URI

import org.scalatest.FlatSpec

import scala.io.Source

class NARADocumentBuilderTest extends FlatSpec {

  "A NARADocumentBuilder" should "build a document from NARA data without failing" in {
    val xml: String = Source.fromURL(getClass.getResource("/nara-item.xml")).mkString
    val doc = new NARADocumentBuilder().buildFromXml(xml)
    println(doc)
  }

}
