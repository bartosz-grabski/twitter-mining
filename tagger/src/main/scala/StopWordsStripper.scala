/**
 * Created by bartosz on 5/25/14.
 */

import com.mongodb.casbah.Imports._
import scala.io.Source._

class StopWordsStripper(filename:String) {

	val stopWordsContent = fromInputStream(getClass.getResourceAsStream(filename)).mkString

    private def extractStopWords(content: String) = {
        content.split(",");
    }

    def strip(content: String) : Array[String] = {
    	
    	val stopWords = extractStopWords(stopWordsContent)

    	val splitted = content.split("[\\p{Punct}\\s]+");
    	val filtered = splitted.filter { el: String =>
    		!stopWords.contains(el) && !(el.length == 1)
    	}

    	filtered;
    }


}
