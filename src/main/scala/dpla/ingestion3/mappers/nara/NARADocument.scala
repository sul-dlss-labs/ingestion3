package dpla.ingestion3.mappers.nara

case class NARADocument (
                          urlItem: Option[String] = None,
                          thumbnail: Option[String] = None,
                          titles: Seq[String] = Seq(),
                          identifiers: Seq[String] = Seq(),
                          dates: Seq[DateField] = Seq(),
                          rights: Seq[String] = Seq(),
                          collectionNames: Seq[String] = Seq(),
                          publishers: Seq[String] = Seq(),
                          contributors: Seq[String] = Seq(),
                          creators: Seq[String] = Seq(),
                          descriptions: Seq[String] = Seq(),
                          formats: Seq[String] = Seq(),
                          languages: Seq[String] = Seq(),
                          spatials:  Seq[String] = Seq(),
                          relations: Seq[String] = Seq(),
                          subjects: Seq[String] = Seq(),
                          types: Seq[String] = Seq(),
                          dataProvider: Option[String] = None
                        )
case class DateField (
                    key: Option[String] = None,
                    start: Option[String] = None,
                    end: Option[String] = None
                    )