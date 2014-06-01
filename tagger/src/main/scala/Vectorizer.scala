class Vectorizer {
	
	private var words = Map("asd" -> 2,"bsd" -> 1,"csd" -> 0)

	//Creates vector. Last element of created vector is the label value
	//(tag,classification)
	
	def createVectorForContent(content : Array[String], label : Int) = {
		val vector = new Array[Int](words.size + 1)
		content.foreach { t =>
			if (words.contains(t)) {
				vector(words(t)) = 1	
			}
		}
		vector(vector.length-1) = label;
		vector
	}

}