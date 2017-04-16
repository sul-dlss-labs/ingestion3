package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel._

import scala.util.Try
import scala.xml.XML
import com.databricks.spark.avro._
import dpla.ingestion3.utils.MultiNodeSeq
import org.apache.spark.storage.StorageLevel

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

  def main(args: Array[String]) = {

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
    strings.persist(StorageLevel.MEMORY_AND_DISK)
    val stringPairs = strings.flatMap(row => handleXmlFile(row.getString(1)))
    stringPairs.toDF("id", "document").write.format("com.databricks.spark.avro").option("avroSchema", schemaStr).avro(outfile)
    val recordCount = stringPairs.count()
    sc.stop()
    val endTime = System.currentTimeMillis()
    Utils.printResults(endTime - startTime, recordCount)

  }

  import dpla.ingestion3.utils.MultiNodeSeq._

  def handleXmlFile(xmlString: String): Seq[Tuple2[String, String]] = for {
    xml <- XML.loadString(xmlString)
    item <- xml \\ Seq("item", "itemAv", "fileUnit")
    if (item \\ "digitalObjectArray").nonEmpty
    id <- item \ "digitalObjectArray" \ "digitalObject" \ "objectIdentifier"
    outputXML = Utils.xmlToString(item)
  } yield (id.text, outputXML)

}
