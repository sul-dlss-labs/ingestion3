package dpla.ingestion3

import java.io.File
import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter

import dpla.ingestion3.enrichments.Spatial
import dpla.ingestion3.mappers.nara.NARADocumentBuilder
import dpla.ingestion3.mappers.nara.NARAMapper
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import com.databricks.spark.avro._

object NaraMapMain {

  val schemaStr: String =
    """
    {
      "namespace": "la.dp.avro",
      "type": "record",
      "name": "MAPRecord.v1",
      "doc": "Prototype mapped record",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "document", "type": "string"}
      ]
    }
    """
  private[this] val logger = LogManager.getLogger(EnricherMain.getClass)

  /*
   * args:
   *   master: String -- Spark master descriptor
   *   inputURI: String -- URI of Avro input
   *   outputURI: String -- URI of Avro output
   */
  def main(args: Array[String]) {

    if (args.length != 3) {
      logger.error("Arguments should be <master>, <inputURI>, <outputURI>")
      sys.exit(-1)
    }
    val sparkMaster = args(0)
    val inputURI = args(1)
    val outputURI = args(2)

    Utils.deleteRecursively(new File(outputURI))

    val start_time = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setAppName("Nara Mapper")
      .setMaster(sparkMaster)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val data = spark.read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaStr)
      .load(inputURI).rdd

    val mappedDocuments = data.map(row => (row.getString(0), map(row.getString(1))))

    val recordCount = mappedDocuments.count()

    mappedDocuments.toDF.write
      .format("com.databricks.spark.avro")
      .avro(outputURI)

    sc.stop()

    val end_time = System.currentTimeMillis()

    Utils.printResults(end_time - start_time, recordCount)
  }

  def map(inDoc: String): String = {
    val documentBuilder = new NARADocumentBuilder()
    val document = documentBuilder.buildFromXml(inDoc)
    serialize(new NARAMapper(document).map())
  }

  def serialize(model: Model): String = {
    val out = new StringWriter()
    Rio.write(model, out, RDFFormat.TURTLE)
    out.toString
  }


}
