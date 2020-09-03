import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable
import scala.io.Source

class HSpark {

  def parseInput(line: Row): Row = {
    var fields = line.toString().filter(!"[".contains(_)).split("\\s")
    return Row(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]) {
    // Hide end warnings/infos at the end of build. (local)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Initialize Session
    val spark = SparkSession.builder().appName("Movie Recommendations").master("local[*]").getOrCreate()

    // Import list of movies and put their titles and ids in an hashmap
    val filename = "hdfs:///user/maria_dev/ml-100k/u.item"
    var movieNames = mutable.HashMap[Int, String]()

    // This is for local
    /*for (line <- Source.fromFile(filename)(codec = "ISO-8859-1").getLines) {
      movieNames(line.split("""\|""")(0).toInt) = line.split("""\|""")(1)
    }*/

    // For Spark
    for (line <- spark.sparkContext.textFile(filename).toLocalIterator.toArray) {
      movieNames(line.split("""\|""")(0).toInt) = line.split("""\|""")(1)
    }

    // Import data file as a RDD
    val lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd
    // Formatting rows as [userID, movieID, ratings]
    val ratingsRDD = lines.map(row => parseInput(row))

    // Define the next dataframe structure
    val struct = StructType(StructField("userID", IntegerType, true) :: StructField("movieID", IntegerType, false) :: StructField("rating", FloatType, false) :: Nil)

    // Create a dataframe from the rdd
    val ratings = spark.createDataFrame(ratingsRDD, struct).cache()

    // Create a RDD[Rating] from RDD[Row]
    val ratingForAls = ratingsRDD.map(_.toString().filter(!"[]".contains(_)).split(",") match {
      case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toFloat)
    })

    // Create a model by training the RDD[Rating]
    val rank = 10
    val numIterations = 10
    var model = ALS.train(ratingForAls, rank, numIterations, 0.01)

    // Display the ratings for the user for whom we want to recommend movies
    println("\nRatings for user ID 0:")
    var userRatings = ratings.filter("userID = 237").orderBy("rating")
    for (rating <- userRatings.collect()) {
      println(movieNames(rating(1).toString.toInt), rating(2))
    }

    // Filter to avoid recommending some obscure movie
    val ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    val popularMovies = ratingCounts.select("movieID")
    .withColumn("userID", lit(237))
    .rdd
    .map(row => (row(0).toString.toInt, row(1).toString.toInt))

    // convert to RDD
    val popularMoviesRDD = spark.sparkContext.parallelize(popularMovies.collect())

    // Predicting the ratings for other movies for the user we chose
    val recommendations = model.predict(popularMoviesRDD)

    // Taking the best of those ratings
    val topRecommendations = recommendations.collect().sortBy(x => x.rating)(Ordering.Double.reverse).take(7)

    // Printing the top recommendations
    println("\nBest 7 Recommendations:")
    for (recommendation <- topRecommendations) {
      println(movieNames(recommendation.user))
    }

    spark.close()
    }
}