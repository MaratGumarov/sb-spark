import java.io.FileNotFoundException
import java.util.Properties

import scala.io.Source

object ConfigurationReader {
  def conf: Properties = {
    val properties: Properties = new Properties()
    val url = getClass.getResource("lab03.properties")

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
      properties
    }
    else {
      throw new FileNotFoundException("Properties file cannot be loaded")
    }
  }
}
