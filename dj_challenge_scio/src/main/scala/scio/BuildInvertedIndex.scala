package scio

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileSystems

import scala.collection.JavaConverters._
import scala.collection.SortedSet

object BuildInvertedIndex {

  val FILE_INDEX_REGEX_PATTERN = "([0-9]+)".r

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val datasetPath = args("input")
    val dictionaryPath = args("dictionary")
    val outputPath = args("output")

    val NUM_SHARDS = 1

    val dataset = readDataset(sc, datasetPath)
    val dictionary = readDictionary(sc, dictionaryPath)
    val invertedIndex = buildInvertedIndex(dataset, dictionary)
    write(outputPath, NUM_SHARDS, invertedIndex)
    val result = sc.close().waitUntilFinish()
    //result.allCounters...
  }

  private def readDataset(sc: ScioContext, datasetPath: String) = {
    val uris = FileSystems
      .`match`(datasetPath)
      .metadata()
      .asScala
      .map(_.resourceId().toString)

    sc.parallelize(uris)
      .flatMap { uri =>
        val rsrc = FileSystems.matchSingleFileSpec(uri).resourceId()
        val in = Channels.newInputStream(FileSystems.open(rsrc))
        SourceReader.toUTF8Source(in) //multiple encodings in dataset files, all to UTF-8
          .getLines()
          .filter(_.nonEmpty)
          .map((FILE_INDEX_REGEX_PATTERN.findFirstIn(uri).get, _))
      }
  }

  private def readDictionary(sc: ScioContext, dictionaryPath: String) = {
    sc.textFile(dictionaryPath).map {
      case (line) => line.split(",") match {
        case Array(word, wordId) => (word, wordId)
      }
    }
  }

  def buildInvertedIndex(fileNameAndLines: SCollection[(String, String)],
                            dictionaryFile: SCollection[(String, String)]): SCollection[String] = {

    val fileNameAndWords = fileNameAndLines.flatMap {
      case (fileName, line) =>
        line.split("\\W+").map(w => (w.toLowerCase, fileName))
    }

    fileNameAndWords.join(dictionaryFile).map {
      case (_, (docId, wordId)) => (wordId, docId)
    }
      .aggregateByKey(SortedSet[String]())(_ + _, _ ++ _)
      .map(x => x._1 + "," + "(" + x._2.mkString(",") + ")")
  }

  private def write(outputPath: String, NUM_SHARDS: Int, invertedIndex: SCollection[String]) = {
    invertedIndex
      .saveAsTextFile(outputPath, NUM_SHARDS)
  }
}
