package recommendation

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main extends App {
    val mongo = new Mongo()
    val algo = new Algo()
    val spark = mongo.connectToMongoDb()
    val ratings = mongo.getUserMovieRatingsIds()
    val ratingsAls = mongo.getUserMovieRatingsIdsAls()
    val userId = 200

    // Room for improvement 
    // put mongo credentials inside an application.conf

    // Recommendation system
    mongo.getUserPreferences(userId)
    val recommendations = algo.getPredictions(spark, ratings, ratingsAls, userId)

    // Printing the top recommendations
    println("\nBest 7 Recommendations:")
    for (recommendation <- recommendations) {
        println(mongo.getMovieName(recommendation.user))
    }
} 