package recommendation

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.log4j.{Logger,Level}
import org.bson.Document
import org.apache.spark.mllib.recommendation.Rating
import scala.collection.mutable

class Mongo {

  def connectToMongoDb() : SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sp = SparkSession.builder()
    .master("local[8]")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb+srv://m001-student:m001-mongodb-basics@nodecluster-dfgwa.mongodb.net/simplon?readPreference=primaryPreferred")
    .config("spark.mongodb.output.uri", "mongodb+srv://m001-student:m001-mongodb-basics@nodecluster-dfgwa.mongodb.net/simplon")
    .config("spark.executor.heartbeatInterval", "10s")
    .getOrCreate()

    sp.sparkContext.setLogLevel("ERROR")

    return sp
  }

  val spark = connectToMongoDb()
  import spark.implicits._
  val moviesRdd = getCollection("movies")
  val usersRdd = getCollection("users")

  def getCollection(collection : String) : Dataset[Row] = {
    var readConfig = ReadConfig(Map(
    "uri" -> "mongodb+srv://m001-student:m001-mongodb-basics@nodecluster-dfgwa.mongodb.net/", 
    "database" -> "simplon", 
    "collection" -> collection))

    val rdd = MongoSpark.load(spark, readConfig)
    return rdd
  }

  def getDataAnalysis(): Unit = {
    val spark = connectToMongoDb()
    
    // EDA for users
    println("Analysis for Movielens users")

    // count
    println(s"the number of users is ${usersRdd.count}")
    usersRdd.printSchema()
    usersRdd.show(30)

    println("Movies Struct")
    val ratingsByUser = usersRdd.select(col("_id"), col("movies"))
            .withColumn("movies", explode(usersRdd("movies")))
            .withColumn("movieId", col("movies.movieId"))
            .withColumn("rating", col("movies.rating"))
            .drop(col("movies"))

    ratingsByUser.show()
    ratingsByUser.printSchema()

    println("Gender repartition")
    usersRdd
    .groupBy("gender")
    .agg(count("*"))
    .show()

    println("Age repartition")
    usersRdd
    .groupBy("age")
    .agg(count("*"))
    .sort(asc("age"))
    .show(100)

    println("Occupation repartition")
    usersRdd
    .groupBy("occupation")
    .agg(count("*"))
    .sort(asc("occupation"))
    .show()

    // EDA for movies collection

    println(s"the number of movies is ${moviesRdd.count}")
    moviesRdd.printSchema()
    moviesRdd.show(30)

    // unwind and get genres
    println("Movies genres")
    var moviesRddUnwind = moviesRdd.select(split(col("genres"), "[|]").as("genre"), col("title"))
                          .drop("genres")

    moviesRddUnwind = moviesRddUnwind.withColumn("genre", explode(moviesRddUnwind("genre"))) 
    moviesRddUnwind.show()
    
    // get genres only
    println("Main genres")
    moviesRddUnwind
    .groupBy("genre")
    .agg(count("*"))
    .sort(asc("genre"))
    .show()
  }

  def getUserMovieRatingsIds(): Dataset[Row]= {
    val ratingsByUser = usersRdd
            .select(col("_id"), col("movies"))
            .withColumn("movies", explode(usersRdd("movies")))
            .withColumn("movieId", col("movies.movieId"))
            .withColumn("rating", col("movies.rating"))
            .drop(col("movies"))

    return ratingsByUser
  }

  def getUserMovieRatingsIdsAls(): RDD[Rating]= {
    val ratings = getUserMovieRatingsIds()
    val ratingForAls : RDD[Rating] = ratings.rdd.map({
      case Row(user:Int, item:Int, rate:Int) => Rating(user.toInt, item.toInt, rate.toFloat)
    })

    return ratingForAls
  }

  def getUserPreferences(userId : Int): Unit = {
    println(s"User preferences $userId")
    val ratingsByUser = getUserMovieRatingsIds()
                        .filter(col("_id") === userId)
                        .orderBy(col("rating").asc)
                        .limit(20)
    
    for (rating <- ratingsByUser.collect()) {
      getMovieName(rating(1).toString.toInt)
    }
  }

  def getMovieName(movieId : Int) : Unit = {
    val movie = moviesRdd
    .select(col("title"))
    .filter(col("_id") === movieId)
    .as[String]
    .collect()
    .foreach(println)
  } 
  // see if we can have genre also 
}