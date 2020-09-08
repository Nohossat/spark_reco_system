package recommendation

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, sum, explode}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class DataAnalysis(){
  def doEDA(collection: String): Unit = {
    val mongo = new Mongo()
    val moviesRdd = mongo.getCollection(collection)
    val spark = SparkSession
                .builder
                .appName("Movies EDA")
                .getOrCreate()

    // print values
    moviesRdd.show()

    //check for dtypes
    moviesRdd.printSchema()

    // check missing values or nan values

    // first explode the description column
    val moviesExploded = moviesRdd
            .withColumn("overview", col("description.overview"))
            .withColumn("tag_line", col("description.tag_line"))
            .withColumn("adult", col("description.adult"))
            .withColumn("original_language", col("description.original_language"))
            .withColumn("release_date", col("description.release_date"))
            .withColumn("vote_count", col("description.vote_count"))
            .withColumn("vote_average", col("description.vote_average"))
            .drop("description")
    
    moviesExploded.show()

    moviesExploded.createOrReplaceTempView("movies")
    spark.sql("""
      SELECT * FROM movies
    """).show()

    // no null values
    spark.sql("""
      SELECT COUNT(genres), 
             COUNT(overview), 
            COUNT(adult), 
            COUNT(original_language),
            COUNT(release_date),
            COUNT(vote_count) as vote_count,
            COUNT(vote_average) as vote_average FROM movies
    """).show()

    // check for empty strings
    // TBD

    // what to do with Genres list => one hot

    // parse release_date

    // concat title, overview and tagline values so we can do a tf-idf
    // word2vec string columns
    // pass it to model
  }

  def featureTransformation() : Unit = {
    println("test")
  }
}
