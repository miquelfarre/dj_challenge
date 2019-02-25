package sparkchallenge

import scala.collection.mutable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BuildInvertedIndex {

  val FILE_INDEX_REGEX_PATTERN = "([0-9]+)".r

  def main(args: Array[String]): Unit = {

    val datasetPath = args(0)
    val dictionaryPath = args(1)
    val invertedIndexPath = args(2)

    val conf = new SparkConf().setAppName("Build Inverted Index").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //read dictionary
    val dictionaryWordWordId = sc.textFile(dictionaryPath).map {
      case (line) => line.split(",") match {
        case Array(word, wordId) => (word, wordId)
      }
    }

    //all files in dataset with path
    val files = FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(datasetPath))

    //all filename and word pairs in dataset
    val fileNameAndWords = files.map(filename => {
      (FILE_INDEX_REGEX_PATTERN.findFirstIn(filename.getPath.toString).get,
        sc.textFile(filename.getPath.toString).map(_.toLowerCase).flatMap {
          case (line) => line.split("\\W+").filter(_.nonEmpty)
        })
    })

    transform(fileNameAndWords, dictionaryWordWordId, invertedIndexPath)
    sc.stop
  }


  def transform(fileNameAndWords: Seq[(String, RDD[String])],
                dictionary: RDD[(String, String)],
                invertedIndexPath: String): Unit = {

    val wordDocIdPairs = fileNameAndWords
      .map {
        case (doc, words) => words
        .flatMap(_.split("\\W+").filter(_.nonEmpty))
        .map(word => (word.toLowerCase, doc))
      }
      .reduce(_ ++ _)

    //aggregateByKey parameters
    val initialSet = mutable.SortedSet.empty[Int]
    val addToSet = (s: mutable.SortedSet[Int], v: Int) => s += v
    val mergePartitionSets = (p1: mutable.SortedSet[Int], p2: mutable.SortedSet[Int]) => p1 ++= p2

    //join words with dictionary, map output to Ints to sort
    wordDocIdPairs.join(dictionary).map {
      case (_, (docId, wordId)) => (wordId.toInt, docId.toInt)
    }
      .aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      .sortByKey(ascending = true)
      .map(x => x._1 + "," + "(" + x._2.mkString(",") + ")")
      //only one output file
      .repartition(1)
      .saveAsTextFile(invertedIndexPath)
  }
}