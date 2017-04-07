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

    val sparkConf = new SparkConf()
      .setAppName("NARA Harvest")
      .setMaster(master)
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val strings  = sc.sequenceFile[String, String](infile)
    val xml = strings.map(row => (row._1, Try(XML.loadString(new String(row._2))))).filter(row => row._2.isSuccess).map(row => (row._1, row._2.get))
    val dosXml = xml.filter(row => (row._2 \\ "digitalObjectArray").nonEmpty)
    val items = dosXml.flatMap(row => row._2 \\ "item")
    val itemAvs = dosXml.flatMap(row => row._2 \\ "itemAv")
    val fileUnit = dosXml.flatMap(row => row._2 \\ "fileUnit")
    val allItems = sc.union(items, itemAvs, fileUnit)
    strings.unpersist(true)
    allItems.persist(MEMORY_AND_DISK)
    val doItems = allItems.filter(row => (row \\ "digitalObjectArray").nonEmpty)
    val doItemPairs = doItems.map( node => ((node \ "digitalObjectArray" \ "digitalObject" \ "objectIdentifier").headOption, node)).filter(row => row._1.isDefined).map(row => (row._1.get.text, row._2))
    val stringPairs: RDD[(String, String)] = doItemPairs.map(row => { (row._1, Utils.xmlToString(row._2))})
    stringPairs.toDF("id", "document").write.format("com.databricks.spark.avro").option("avroSchema", schemaStr).avro(outfile)
  }

}
