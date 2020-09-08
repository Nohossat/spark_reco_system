package recommendation

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class DataAnalysis(){
  def doEDA(collection: String): Unit = {
    // print values
    val mongo = new Mongo()
    val moviesRdd = mongo.getCollection(collection)
    moviesRdd.show()
    //check for dtypes
    // check missing values or nan values
    // check for feature correlation
    // tf-idf + truncated svd string columns
    // pass it to model
  }

  def featureTransformation() : Unit = {
    println("test")
  }
}
