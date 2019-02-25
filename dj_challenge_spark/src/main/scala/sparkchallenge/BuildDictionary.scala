package sparkchallenge

import org.apache.spark.{SparkConf, SparkContext}

object BuildDictionary{

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("Build Dictionary").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.textFile(inputPath)
      .map(_.toLowerCase)
      .flatMap{
        line => line.split("\\W+").filter(_.nonEmpty)
      }
      //remove duplicate words
      .distinct
      //assign unique id to each word (>1 partition, otherwise zipWithIndex)
      .zipWithUniqueId
      //file format (word, id)
      .map(x => x._1 + "," + x._2)
      //only one output file, 1 partitions
      .repartition(1)
      .saveAsTextFile(outputPath)

    sc.stop()

  }
}
