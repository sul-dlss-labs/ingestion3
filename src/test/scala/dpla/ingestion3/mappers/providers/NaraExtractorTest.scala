package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.util.{Failure, Success}
import scala.xml.XML

class NaraExtractorTest extends FlatSpec with BeforeAndAfter {

  val shortName = "nara"
  val xmlString: String = new FlatFileIO().readFileAsString("/nara.xml")
  val xml = XML.loadString(xmlString)
  val itemUri = new URI("http://catalog.archives.gov/id/2132862")
  val extractor = new NaraExtractor(xmlString, shortName)

  "A NaraExtractor" should "successfully extract from a valid document" in {
    extractor.build() match {
      case Success(data) => succeed
      case Failure(exception) => fail(exception)
    }
  }

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName())

  it should "pass through the short name to ID minting" in
    assert(extractor.getProviderName() === shortName)

  it should "construct the correct item uri" in
    assert(extractor.itemUri(xml) === itemUri)

  it should "have the correct DPLA ID" in {
    val dplaUri = extractor.build().getOrElse(fail("Extraction failed.")).dplaUri
    assert(dplaUri === new URI("http://dp.la/api/items/6ed2c7c1c0f1325617426d6266e3140d"))
  }

  it should "express the right hub details" in {
    val agent = extractor.agent
    assert(agent.name === Some("National Archives and Records Administration"))
    assert(agent.uri === Some(new URI("http://dp.la/api/contributor/nara")))
  }

  it should "extract collections" in {
    val collections = extractor.collection(xml)
    assert(collections === Seq("424", "Records of the Forest Service", "95"))
  }

  //todo add contributors to test data
  it should "extract contributors" in {
    val contributors = extractor.contributor(xml)
    assert(contributors === Seq())
  }

  it should "extract creators" in {
    val creators = extractor.creator(xml)
    assert(creators === Seq("Department of Agriculture. Forest Service. Region 9 (Eastern Region). 1965-"))
  }

  //todo better coverage of date possibilities?
  it should "extract dates" in {
    val dates = extractor.date(xml)
    assert(dates === Seq(stringOnlyTimeSpan("1967-10")))
  }

  it should "extract descriptions" in {
    val descriptions = build().sourceResource.description
    assert(descriptions === Seq("Original caption: Aerial view of Silver Island Lake, from inlet, looking north, with Perent Lake in background."))
  }

  //todo add extents to test data
  it should "extract extents" in {
    val extents = build().sourceResource.extent
    assert(extents === Seq())
  }

  it should "extract formats" in {
    val formats = extractor.format(xml)
    assert(formats === Seq("Aerial views", "Photographic Print"))
  }

  it should "extract identifiers" in {
    val identifiers = build().sourceResource.identifier
    assert(identifiers === Seq("2132862"))
  }

  //todo find test data with languages
  it should "extract languages" in {
    val languages = build().sourceResource.language
    assert(languages === Seq())
  }

  it should "extract places" in {
    val places = build().sourceResource.place
    assert(places === Seq(nameOnlyPlace("Superior National Forest (Minn.)")))
  }

  //todo add publishers to test data
  it should "extract publishers" in {
    val publishers = extractor.publisher(xml)
    assert(publishers === Seq())
  }

  //todo add relations to test data
  it should "extract relations" in {
    val relations = extractor.relation(xml)
    assert(relations === Seq())
  }

  it should "extract rights" in {
    val rights = extractor.rights(xml)
    //todo this mapping is probably wrong in the way it concatenates values
    assert(rights.head.contains("Unrestricted"))
  }

  it should "extract subjects" in {
    val subjects = build().sourceResource.subject
    assert(subjects === Seq(nameOnlyConcept("Recreation"), nameOnlyConcept("Wilderness areas")))
  }

  it should "extract titles" in {
    val titles = build().sourceResource.title
    assert(titles === Seq("Photograph of Aerial View of Silver Island Lake"))
  }

  it should "extract types" in {
    val types = extractor.types(xml)
    assert(types.head.contains("image"))
  }

  it should "extract dataProviders" in {
    val dataProvider = extractor.dataProvider(xml)
    assert(dataProvider === nameOnlyAgent("National Archives at Chicago"))
  }

  it should "contain the original record" in {
    assert(xmlString === build.originalRecord)
  }

  it should "contain the hub agent as the provider" in {
    assert(
      build().provider === EdmAgent(
        name = Some("National Archives and Records Administration"),
        uri = Some(new URI("http://dp.la/api/contributor/nara"))
      )
    )
  }

  it should "contain the correct isShownAt" in {
    assert(build().isShownAt === uriOnlyWebResource(itemUri))
  }

  //todo should we eliminate these default thumbnails?
  it should "find the item previews" in {
    assert(build().preview === Some(uriOnlyWebResource(new URI("http://media.nara.gov/great-lakes/001/517805_t.jpg"))))
  }

  def build(): OreAggregation = extractor
    .build()
    .getOrElse(fail("Extraction failed."))
}
