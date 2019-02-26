package scio

import java.io.InputStream
import java.nio.charset.{Charset, CodingErrorAction}

object SourceReader {

  def toUTF8Source(inputStream: InputStream): scala.io.BufferedSource = {
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    scala.io.Source.fromInputStream(inputStream)(decoder)
  }
}