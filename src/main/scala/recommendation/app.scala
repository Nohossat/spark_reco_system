package recommendation

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{udf, col}

object Main extends App {
    val mongo = new Mongo()
    val spark = mongo.connectToMongoDb()
    val userId = 200

    def getCollaborativeFiltering(userId : Int) : Unit = {
        // print user preferences
        // mongo.getUserPreferences(userId)

        // Collaborative filtering
        val cf = new CollaborativeFiltering()
        val ratings = mongo.getUserMovieRatingsIds()
        val ratingsAls = mongo.getUserMovieRatingsIdsAls()
        val recommendations = cf.getPredictions(spark, ratings, ratingsAls, userId)

        // Printing the top recommendations
        println("\nBest 7 Recommendations:")
        for (recommendation <- recommendations) {
            println(mongo.getMovieName(recommendation.user))
        }
    }

    val dataAnalysis = new DataAnalysis()
    dataAnalysis.doEDA("movies_full")
} 