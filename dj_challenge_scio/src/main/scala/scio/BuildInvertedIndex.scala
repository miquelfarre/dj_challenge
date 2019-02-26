package scio

import java.io.InputStream
import java.nio.channels.Channels
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileSystems
import scala.collection.JavaConverters._
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import scala.collection.SortedSet

object BuildInvertedIndex {

  val FILE_INDEX_REGEX_PATTERN = "([0-9]+)".r

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val datasetPath = args("input")
    val dictionaryPath = args("dictionary")
    val outputPath = args("output")

    val uris = FileSystems
      .`match`(datasetPath)
      .metadata()
      .asScala
      .map(_.resourceId().toString)

    val fileNameAndLines = sc.parallelize(uris)
      .flatMap { uri =>
        val rsrc = FileSystems.matchSingleFileSpec(uri).resourceId()
        val in = Channels.newInputStream(FileSystems.open(rsrc))
        toUTF8Source(in) //multiple encodings in dataset files, all to UTF-8
          .getLines()
          .filter(_.nonEmpty)
          .map((FILE_INDEX_REGEX_PATTERN.findFirstIn(uri).get, _))
      }

    invertedIndex(fileNameAndLines, sc.textFile(dictionaryPath))
      .saveAsTextFile(outputPath, 1)

    val result = sc.close().waitUntilFinish()
    //result.allCounters...
  }

  private def toUTF8Source(inputStream: InputStream): scala.io.BufferedSource = {
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    scala.io.Source.fromInputStream(inputStream)(decoder)
  }

  def invertedIndex(fileNameAndLines: SCollection[(String, String)],
                    dictionaryFile: SCollection[String]): SCollection[String] = {

    val fileNameAndWords = fileNameAndLines.flatMap {
      case (fileName, line) =>
        line.split("\\W+").map(w => (w.toLowerCase, fileName))
    }

    val dictionary = dictionaryFile.map {
      case (line) => line.split(",") match {
        case Array(word, wordId) => (word, wordId)
      }
    }

    fileNameAndWords.join(dictionary).map {
      case (_, (docId, wordId)) => (wordId, docId)
    }
      .aggregateByKey(SortedSet[String]())(_ + _, _ ++ _)
      .map(x => x._1 + "," + "(" + x._2.mkString(",") + ")")
  }
}
