package dpla.ingestion3.mappers.nara

import dpla.ingestion3.mappers.rdf._
import org.eclipse.rdf4j.model._

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class NARAMapper(document: NARADocument) extends MappingUtils {

  def map(): Model = {
    assert(document.urlItem.isDefined)

    val naraUri = iri("http://dp.la/api/contributor/nara")
    val providerLabel = "National Archives and Records Administration"

    val itemIri = iri(document.urlItem.get)
    val thumbnailIri: Option[IRI] = document.thumbnail match {
      case Some(thumbnail) => Try(iri(thumbnail)) match {
        case Success(iri) => Some(iri)
        case Failure(ex) => None
      }

      case _ => None
    }

    registerNamespaces(defaultVocabularies)
    mapItemWebResource(itemIri)
    mapContributingAgent(naraUri, providerLabel)

    if (thumbnailIri.isDefined)
      mapThumbWebResource(iri(document.thumbnail.get))

    val aggregatedCHO = mapAggregatedCHO(ChoData(
      //todo: this doesn't map start + end
      dates = mapDates(document.dates.distinct.flatMap(date => date.key)),
      titles = mapStrings(document.titles.distinct),
      identifiers = mapStrings(document.identifiers.distinct),
      rights = mapStrings(document.rights.distinct),
      collections = mapStrings(document.collectionNames.distinct),
      contributors = mapStrings(document.contributors.distinct),
      creators = mapStrings(document.creators.distinct),
      publishers = mapStrings(document.publishers.distinct),
      types = mapStrings(document.types.distinct)
    ))

    mapAggregation(AggregationData(
      aggregatedCHO = aggregatedCHO,
      isShownAt = iri(document.urlItem.get),
      preview = thumbnailIri,
      provider = itemIri,
      originalRecord = mapOriginalRecord(),
      dataProvider = mapDataProvider(providerLabel)
    ))

    build()
  }
}
