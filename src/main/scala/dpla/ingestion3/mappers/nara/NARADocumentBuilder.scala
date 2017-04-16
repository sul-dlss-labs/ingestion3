package dpla.ingestion3.mappers.nara


import dpla.ingestion3.mappers.xml.XmlExtractionUtils

import scala.xml.Node
import scala.xml.NodeSeq
import scala.xml.XML

/**
  * This class assumes it's being shown an xml Node that corresponds to
  * "item", "itemAv", or "fileUnit"
  */

class NARADocumentBuilder extends XmlExtractionUtils {

  //Implicit that loops over multiple potential paths and returns their union

  import dpla.ingestion3.utils.MultiNodeSeq._

  val urlPrefix = "http://catalog.archives.gov/id/"

  def buildFromXml(rawData: String): NARADocument = {
    val xml = XML.loadString(rawData)

    val document = NARADocument(
      urlItem = urlItem(xml),
      thumbnail = previewUri(xml),
      titles = titles(xml),
      identifiers = identifiers(xml),
      dates = dates(xml),
      rights = rights(xml),
      collectionNames = collections(xml),
      publishers = publishers(xml),
      types = types(xml),
      dataProvider = dataProvider(xml),
      contributors = contributors(xml),
      creators = creators(xml),
      descriptions = descriptions(xml),
      formats = formats(xml),
      languages = languages(xml),
      spatials = spatials(xml),
      relations = relations(xml),
      subjects = subjects(xml)
    )
    document
  }

  def contributors(item: Node): Seq[String] =
    for {
      contributor <- item \
        Seq(
          "organizationalContributorArray",
          "personalContributorArray",
          "organizationalContributor",
          "personalContributor"
        )
      if !(contributor \ "contributorType" \ "termName")
        .map(_.text).contains("Publisher")

      contributorName <- contributor \ "termName"

    } yield contributorName.text

  def creators(item: Node): Seq[String] =
    for {
      creator <- item \
        "parentSeries" \
        Seq(
          "creatingOrganizationArray",
          "creatingIndividualArray"
        ) \
        Seq(
          "creatingOrganization",
          "creatingIndividual"
        ) \
        "creator" \
        "termName"
    } yield creator.text

  def descriptions(item: Node): Seq[String] = {
    val scopes =
      for (scope <- item \ "scopeAndContentNote")
        yield scope.text

    val notes =
      for (note <- item \ "generalNoteArray" \ "generalNote" \ "note")
        yield note.text

    scopes ++ notes
  }

  def formats(item: Node): Seq[String] =
    for (format <- item \ "specificRecordsTypeArray" \ "specificRecordsType" \ "termName")
      yield format.text

  def languages(item: Node): Seq[String] =
    for (lang <- item \ "languageArray" \ "language" \ "termName")
      yield lang.text

  def relations(item: Node): Seq[String] =
    for {
      relation <- item \ "parentFileUnit" \ "parentSeries" \ "parentRecordGroup" \ "parentCollection"

    } yield {

      val topTitles = relation \ "title"
      val midGroup = relation \ "parentSeries"
      val midTitles = midGroup \ "title"

      val botTitles1 = midGroup \ Seq("parentRecordGroup", "parentCollection") \ "title"
      val botTitles2 = relation \ "parentRecordGroup" \ "title"
      val botTitles3 = relation \ "parentCollection" \ "title"

      //there's probably a less obtuse way of doing this
      val botTitles = Seq(botTitles1, botTitles2, botTitles3)
        .foldLeft(NodeSeq.Empty)(
          (seq, nodeSeq) => if (seq.isEmpty) nodeSeq else seq
        )

      Seq(botTitles.headOption, midTitles.headOption, topTitles.headOption).mkString("; ")
    }

  def spatials(item: Node): Seq[String] =
    for (place <- item \ "geographicReferenceArray" \ "geographicPlaceName" \ "termName")
      yield place.text

  def subjects(item: Node): Seq[String] =
    for (concept <- item \ "topicalSubjectArray" \ "topicalSubject" \ "termName")
      yield concept.text

  def publishers(item: Node): Seq[String] =
    for {
      contributor <- item \
        Seq("organizationalContributorArray", "personalContributorArray") \
        Seq("organizationalContributor", "personalContributor")

      if (contributor \ "contributorType" \ "termName")
        .map(x => x.text).contains("Publisher")

      publisher <- contributor \ "termName"

    } yield publisher.text

  def types(item: Node): Seq[String] =
    for (dctype <- item \ "generalRecordsTypeArray" \ "generalRecordsType" \ "termName")
      yield dctype.text

  def dates(item: Node): Seq[DateField] = {

    val coverageDates =
      for {
        date <- item \ "coverageDates"
        dateField <- dateField(date)
      } yield dateField

    if (coverageDates.nonEmpty) coverageDates
    else for {
      date <- item \
        Seq(
          "copyrightDateArray",
          "productionDateArray",
          "broadcastDateArray",
          "releaseDateArray",
          "proposableQualifiableDate"
        )
      dateField <- dateField(date)
    } yield dateField
  }

  def dateField(date: Node): Option[DateField] = {
    DateField(
      key = formatDate(Some(date)),
      start = formatDate((date \ "coverageStartDate").headOption),
      end = formatDate((date \ "coverageEndDate").headOption)
    ) match {
      case DateField(None, None, None) => None
      case x @ DateField(_,_,_) => Some(x)
    }
  }

  def formatDate(dateNodeOption: Option[Node]): Option[String] = dateNodeOption match {
    case None => None
    case Some(dateNode) => {
      val year = (dateNode \ "year").map(_.text).headOption
      val month = (dateNode \ "month").map(_.text).headOption
      val day = (dateNode \ "day").map(_.text).headOption
      val qualifier = (dateNode \ "dateQualifier").map(_.text).headOption

      (year, month, day, qualifier) match {

        case (Some(yearText), Some(monthText), Some(dayText), Some("?")) =>
          Some(s"$yearText-$monthText-$dayText?")

        case (Some(yearText), Some(monthText), Some(dayText), Some(qualifierString)) =>
          Some(s"$qualifierString $yearText-$monthText-$dayText")

        case (Some(yearText), Some(monthText), Some(dayText), None) =>
          Some(s"$yearText-$monthText-$dayText")

        case _ => None
      }
    }
  }

  def identifiers(item: Node): Seq[String] =
    for {
      variantControlNumber <- item \ "variantControlNumberArray" \ "variantControlNumber"
      number = (variantControlNumber \ "number").headOption
      termName = (variantControlNumber \ "type" \ "termName").headOption
    } yield (termName ++ number).map(_.text).mkString(": ")

  def titles(item: Node): Seq[String] =
    for (title <- item \ "title")
      yield title.text

  def dataProvider(item: Node): Option[String] =
    for {
    // paths broken onto separate lines
    // \ is a path traversal method, not a line continuation
      name <- (item \
        "physicalOccurrenceArray" \
        ("itemPhysicalOccurrence" ++ "itemAvPhysicalOccurrence" ++ "fileUnitPhysicalOccurrence") \
        "referenceUnitArray" \
        "referenceUnit" \
        "name").headOption
    } yield name.text

  def urlItem(item: Node): Option[String] =
    for (naId <- (item \ "naId").headOption)
      yield "http://catalog.archives.gov/id/" + naId.text

  def previewUri(item: Node): Option[String] =
    for (thumbnail <- (item \ "digitalObjectArray" \ "digitalObject" \ "thumbnailFilename").headOption)
      yield thumbnail.text

  def collections(item: Node): Seq[String] =
    for (collection <- item \ "parentFileUnit" \ "title")
      yield collection.text

  def rights(item: Node): Seq[String] =
    for (rights <- item \ "useRestriction") yield {

      val useRestrictions = {
        for {
          termName <- rights \ "specificUseRestrictionArray" \ "termName"
          text = termName.text
          if !text.isEmpty
        } yield text
      }.mkString(", ")

      val note = (rights \ "note").headOption
      val status = (rights \ "status" \ "termName").headOption

      val generalRights = {
        for {
          elem <- note ++ status
          text = elem.text
          if !text.isEmpty
        } yield text
      }.mkString(" ")

      Seq(useRestrictions, generalRights).filter(!_.isEmpty).mkString(": ")
    }

}
