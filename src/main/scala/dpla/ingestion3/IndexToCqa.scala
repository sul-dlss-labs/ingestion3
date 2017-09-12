package dpla.ingestion3

import dpla.ingestion3.model.{DplaMap, DplaMapData}
import org.apache.log4j.LogManager
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object IndexToCqa {

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(MappingEntry.getClass)

    if (args.length != 2)
      logger.error("Incorrect number of parameters provided. Expected <input> <output>")

    // Get the input data
    val data = args(0)


    val sparkConf = new SparkConf()
      .setAppName("CQA Indexer")
      // TODO there should be a central place to store the sparkMaster
      .setMaster("local[*]")
      // TODO: This spark.serializer is a kludge to get around serialization issues. Will be fixed in future ticket
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Needed to serialize the successful mapped records
    implicit val dplaMapDataEncoder = org.apache.spark.sql.Encoders.kryo[DplaMapData]
    // Need to map the mapping results
    implicit val dplaMapEncoder = org.apache.spark.sql.Encoders.kryo[DplaMap]

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    // Load the harvested record dataframe
    val records: Dataset[DplaMapData] = spark
      .read
      .format("com.databricks.spark.avro")
      .load(data)
      .as[DplaMapData]

    val jsonData = records.map(r => model.jsonlRecord(r)).map(_.replaceAll("\n", " "))

    jsonData.limit(1).rdd.repartition(1).saveAsTextFile("/Users/scott/i3_pa_mapping/pa_json.mapped.jsonl")
  }
}
