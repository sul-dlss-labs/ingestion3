package dpla.ingestion3.utils

import java.io.File
import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.IOUtils
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarEntry
import org.apache.tools.tar.TarInputStream

import scala.annotation.tailrec

// loosely based on tar-to-seq from https://stuartsierra.com/2008/04/24/a-million-little-files

class Archive2Avro {

  val schema: Schema = new Schema.Parser().parse(
    """
      |{
      |    "namespace": "la.dp.avro",
      |     "type": "record",
      |     "name": "tar",
      |     "fields": [
      |         {  "name": "path", "type": "string" },
      |         {  "name": "data",  "type": "string" }
      |     ]
      |}
    """.stripMargin
  )

  def getAvroWriter(outputFile: File): DataFileWriter[GenericRecord] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.setCodec(CodecFactory.deflateCodec(1))
    dataFileWriter.create(schema, outputFile)
  }

  def getTarInputStream(inputFile: File): Option[TarInputStream] = {
    val fileStream = new FileInputStream(inputFile)

    inputFile.getName match {

      case name if name.endsWith("gz") || name.endsWith("tgz") =>
        Some(new TarInputStream(new GZIPInputStream(fileStream)))

      case name if name.endsWith("bz2") || name.endsWith("tbz2") =>
        //skip the "BZ" header added by bzip2.
        fileStream.skip(2)
        Some(new TarInputStream(new CBZip2InputStream(fileStream)))

      case _ => None
    }
  }

  def convert(
               input: TarInputStream,
               writer: DataFileWriter[GenericRecord]
             ): Unit = {

    def handle(entry: TarEntry) = entry match {
      case _ if entry.isDirectory => Unit //ignore directories
      case fileEntry =>
        val path = fileEntry.getName
        val data = new String(IOUtils.toByteArray(input, fileEntry.getSize), "utf-8")
        val genericRecord = new GenericData.Record(schema)
        genericRecord.put("path", path)
        genericRecord.put("data", data)
        writer.append(genericRecord)
    }

    @tailrec
    def iter(): Unit = {
      input.getNextEntry match {
        case null => Unit
        case entry => handle(entry); iter()
      }
    }

    iter()
  }
}

object Archive2Avro extends App {

  def exitWithHelp() {
    println("Args: <tarfile> <output>")
    println()
    println("<tarfile> may be GZIP or BZIP2 compressed, must have a")
    println("recognizable extension .tar, .tar.gz, .tgz, .tar.bz2, or .tbz2.")
    System.exit(1)
  }

  val archive2Avro = new Archive2Avro

  if (args.length != 2) exitWithHelp()
  val inputFile = new File(args(0))
  val tarInputStream = archive2Avro.getTarInputStream(inputFile)
  if (tarInputStream.isEmpty) exitWithHelp()

  val outputFile = new File(args(1))
  val dataFileWriter = archive2Avro.getAvroWriter(outputFile)

  archive2Avro.convert(tarInputStream.get, dataFileWriter)

  dataFileWriter.close()
}
