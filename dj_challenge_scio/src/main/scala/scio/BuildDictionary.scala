package scio

import java.math.BigInteger
import java.security.MessageDigest
import com.spotify.scio.ContextAndArgs

object BuildDictionary {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val datasetPath = args("input")
    val outputPath = args("output")

    sc.textFile(datasetPath).map(_.trim)
      .flatMap(_.split("\\W+").filter(_.nonEmpty).map(_.toLowerCase))
      .distinct
      .map(t => t + "," + md5HashString(t))
      .saveAsTextFile(outputPath)

    val result = sc.close().waitUntilFinish()
    //result.allCounters...
  }

  def md5HashString(s: String): String = {
    val digest = MessageDigest.getInstance("MD5").digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    bigInt.toString(16)
  }

}
