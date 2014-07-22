import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.scalding.mathematics._

//algebird -- tools to work with scalding


class Hello(args: Args) extends Job(args) {

     List(("alice.txt", "alice"),("wap.txt", "wap")).foreach { (fileName, name) =>
        val file: TypedPipe[String] = TypedPipe.from(TextLine(fileName))

        //largestWord(file, name)
        averageWordLength(file, name)
        uniqueWordCount(file, name)
        wordLineLength(file, name)
        matrixMath(file, name)
     }
/*
     def largestWord(fileq: TypedPipe[String], name: String) = {
       val file = TypedPipe.from(TextLine("alice.txt"))


        file
          .flatMap{line => line.split("\\W").toList}
          .filterNot{word => word.isEmpty}
          .filter{word => word.head.isUpper}
          .filter{word => word.length > 1}
          .map{word => (word, 1)}
          .group
          .sum 
          .groupAll
          
          .reduce { case ((leftword, leftCount), (rightWord, rightCount)) => 
              if(rightCount > leftCount)
                (rightWord, rightCount)
              else
                (leftWord, leftCount)
          }
          .maxBy{case (word, count) => count} // get the most used word.
          .write(TypedTsv(name+"_words")) 


      }
*/
      def averageWordLength(file: TypedPipe[String], name: String) = {

        file
          .flatMap{line => line.split("\\w").toList}
          .filterNot{word => word.isEmpty}
          .map{word => (1, word.length)}
          .sum
          .map { case (count, total) => total / count.todouble }
          .write(typedtsv(name+"_avg_words")) 
   
      }

      def uniqueWordCount(file: TypedPipe[String], name: String) = {

        file
          .flatMap{line => line.toLowerCase.split("\\w").toList}
          .filterNot{word => word.isEmpty}
          .map{word => Set(word)} //replaced with hll
          .sum //replaced with hll
          .map { set => set.size } //replaced with hll
          .write(TypedTsv(name+"_unique_words")) 

          //Algebird provides .aggregate(HyperLogLogAggregator.sizeAggregator(10)
      }

      def wordLineLength(file: TypedPipe[String], name: String) = {
          val maxLineLengths = file
            .flatMap{line => line.toLowerCase.split("\\W").toList.map{word => (word, line.length)}}
            .group
            .max //(word, maxLineLength)

          val lineLengths = file
            .map{line => (line.length, line)}
            .group  //(lineLength, line)
            .join(maxLineLengths.swap.group) //(lineLength, (line, word))
            .filter { case (line, word) => line.contains(word) }
            .write(TypedTsv(name+"_max_lines")) 
      }

      def matrixMath(file: TypedPipe[String], name: String) = {
        val adjacency = 
          file
            .flatMap{line => line.toLowerCase.split("\\W").toList.sliding(2,1)}
            .map{list => (list(0), list(1))}
            .distinct
            .map{case (a,b) => (a,b,1.0)}

        val matrix = Matrix2(adjacency, NoClue)
        
        val norm = matrix.rowL2Normalize
        (norm * norm.transpose)
        .write(typedTsv("cosine_similarity"))
      }

}


