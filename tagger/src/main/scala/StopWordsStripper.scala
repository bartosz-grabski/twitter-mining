/**
 * Created by bartosz on 5/25/14.
 */

import com.mongodb.casbah.Imports._
import scala.io.Source._

class StopWordsStripper {

    /**
     * Returns content of file
     * @param filename
     * @return
     */
    private def openFile(filename: String) = {
        val lines = fromInputStream(getClass.getResourceAsStream(filename)).mkString
        lines
    }

    private def extractStopWords(content: String) = {
        content.split(",");
    }

    def strip(content: String, stopWordsFile: String) : Array[String] = {
    	
    	val stopWordsContent = openFile(stopWordsFile)
    	val stopWords = extractStopWords(stopWordsContent)

    	val splitted = content.split("[\\p{Punct}\\s]+");
    	val filtered = splitted.filter { el: String =>
    		!stopWords.contains(el) && !(el.length == 1)
    	}

    	filtered;
    }


}
