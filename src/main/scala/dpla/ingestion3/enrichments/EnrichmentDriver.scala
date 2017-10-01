package dpla.ingestion3.enrichments

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.model._

import scala.util.Try


class EnrichmentDriver(conf: i3Conf) extends Serializable {
  /**
    * Reads Twofishes hostname and port from application config file
    *
    * @see dpla.ingestion3.enrichments.Twofisher
    * @see SpatialEnrichmentIntegrationTest
    */
  object Geocoder extends Twofisher {
    override def hostname = conf.twofishes.hostname.getOrElse("localhost")
    override def port = conf.twofishes.port.getOrElse("8081")
  }

  val stringEnrichment = new StringEnrichments()
  val dateEnrichment = new ParseDateEnrichment()
  val spatialEnrichment = new SpatialEnrichment(Geocoder)
  val langEnrichment = LanguageMapper

  /**
    * Applies a set of common enrichments that need to be run for all providers
    *   * Spatial
    *   * Language
    *   * Type
    *   * Date
    *
    * @param record The mapped record
    * @return An enriched record
    */
  def enrich(record: OreAggregation): Try[OreAggregation] = Try {
    /**
      * Notes on geo code runtime for PA
      *
      * 2 minutes w/o geocode
      * 7 hours with geocode using geo-prod from local
      * 35 minutes with geocode using localhost
      */
    record.copy(
      sourceResource = record.sourceResource.copy(
        date = record.sourceResource.date.map(d => dateEnrichment.parse(d)),
        language = record.sourceResource.language.map(l => LanguageMapper.mapLanguage(l))
        ,place = record.sourceResource.place.map(p => spatialEnrichment.enrich(p))
      ))
  }
}
