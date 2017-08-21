/*
Page rank
Scala & Spark
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
import scala.io.Source

object pagerank {

def main (args: Array[String]) {
  
  val conf = new SparkConf().setAppName("pagerank")
  
	val sc= new SparkContext(conf)
	var input = sc.textFile(filepath)
    //extract key value pairs
    // strip parenthesis
    val pages = input.map{ l =>
 	val pair = l.stripPrefix("(").stripSuffix(")").split(",", 2) 
		(pair(0), pair(1)) 

 	}
 	// strip brackets 
    val links  =  pages.map{ r =>
    (r._1, r._2.split("\t").map(t=>t.stripPrefix("[[").stripSuffix("]]")))} 
    
  	// initialize ranks to 1 
    var ranks = links.map{r => (r._1, 1.0) }// load rdd of page title,rank pairs - initially 1
    
    // page rank loop
    val ITER = 20
    for ( i <- 20 to ITER ){

    	val contribs = links.join(ranks).flatMap {
		case (title, (links, rank)) => links.map(dest => (dest, rank / links.size))
	}
		ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85  * _ )  
    }
    // Sort pages by scores descending order
    
    ranks.sortBy(_._2)

    ranks.map(r => r._1 + "\t" + r._2).saveAsTextFile("PageRanks")
	ranks.take(40).foreach(println)
    
}// end main
} // end object
