import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import scio.BuildInvertedIndex

class InvertedIndexTest extends PipelineSpec {

  it should "check inverted index" in {
    runWithContext { sc =>
        val pairWordWordIdSeq = Seq(
          ("tremp","1"),
          ("talarn","2"),
          ("pobla","3"),
          ("guardia","4"),
          ("salas","5")
        )
        val dictionary: SCollection[(String, String)] = sc.parallelize(pairWordWordIdSeq)
        val fileNameAndLines: SCollection[(String, String)] =
          sc.parallelize(
            Seq(("1", "tremp talarn salas tremp"), ("2", "tremp pobla guardia")))
        val invertedIndexSCollection = BuildInvertedIndex.buildInvertedIndex(fileNameAndLines, dictionary)
        val expected = Seq("4,(2)", "3,(2)", "1,(1,2)", "5,(1)", "2,(1)")
        invertedIndexSCollection should containInAnyOrder(expected)
      }
  }
}
