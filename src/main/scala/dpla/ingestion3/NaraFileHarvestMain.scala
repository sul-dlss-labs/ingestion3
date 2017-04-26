package dpla.ingestion3

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


import scala.xml.XML

object NaraFileHarvestMain {

  val logger = LogManager.getLogger(NaraFileHarvestMain.getClass)

  val schemaStr: String =
    """
    {
      "namespace": "la.dp.avro",
      "type": "record",
      "name": "MAPRecord.v1",
      "doc": "Prototype harvested record",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "document", "type": "string"}
      ]
    }
    """

  def main(args: Array[String]): Unit = {

    if(args.length != 3 ) {
      logger.error("Bad number of args: master input_file output_file")
      sys.exit(-1)
    }

    val master = args(0)
    val infile = args(1)
    val outfile = args(2)

    Utils.deleteRecursively(new File(outfile))
    val startTime = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .setAppName("NARA Harvest")
      .setMaster(master)

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val strings = spark.read.avro(infile)
    val stringPairs = strings.flatMap(row => handleXmlFile(row.getString(1)))
    val recordCount = stringPairs.count()
    stringPairs.toDF("id", "document").write.format("com.databricks.spark.avro").option("avroSchema", schemaStr).avro(outfile)
    sc.stop()
    val endTime = System.currentTimeMillis()
    Utils.printResults(endTime - startTime, recordCount)

  }

  import dpla.ingestion3.utils.MultiNodeSeq._

  def handleXmlFile(xmlString: String): Seq[(String, String)] = for {
    xml <- XML.loadString(xmlString)
    item <- xml \\ Seq("item", "itemAv", "fileUnit")
    if (item \\ "digitalObjectArray").nonEmpty
    id <- item \ "digitalObjectArray" \ "digitalObject" \ "objectIdentifier"
    outputXML = Utils.xmlToString(item)
  } yield (id.text, outputXML)

}
