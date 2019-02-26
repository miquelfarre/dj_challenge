import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import sparkchallenge.BuildInvertedIndex

class InvertedIndexTest extends FunSuite {

  test("BuildInvertedIndex.invertedIndex") {
    val conf = new SparkConf().setAppName("Build Dictionary").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = Array("tremp", "talarn", "salas", "tremp")
    val words2 = Array("tremp", "pobla", "guardia")
    val pairWordWordIdSeq = Array(
      ("tremp", "1"),
      ("talarn", "2"),
      ("pobla", "3"),
      ("guardia", "4"),
      ("salas", "5")
    )

    val result: RDD[String] = sc.parallelize(Array("1,(1,2)","2,(1)", "3,(2)", "4,(2)", "5,(1)"))
    val dictionary: RDD[(String, String)] = sc.parallelize(pairWordWordIdSeq)
    val fileNameAndWords: Seq[(String, RDD[String])] =
      Seq(("1", sc.parallelize(words)), ("2", sc.parallelize(words2)))

    val invertedIndexRDD = BuildInvertedIndex.buildInvertedIndex(fileNameAndWords, dictionary)
    assert(invertedIndexRDD.count === result.count)
    assert(invertedIndexRDD.first() === "(1,2)")

    sc.stop

  }
}
