package recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._

class Algo {
    def trainModel(ratings: RDD[Rating], rank : Int, numIter : Int): MatrixFactorizationModel = {
        // val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
        
        var model = ALS.train(ratings, rank, numIter, 0.01)

        /*
        val predictions = model.predict(userProducts)

        val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rating")
            .setPredictionCol("prediction")
        
        val rmse = evaluator.evaluate(predictions)
        println(s"RMSE : $rmse") */

        return model
    }

    
    def getPredictions(spark : SparkSession, ratings: Dataset[Row], ratingsAls: RDD[Rating], userId: Int): Array[Rating] = {
        // train model
        val userProducts = ratings.select(ratings("_id"), ratings("movieId"))
        val model = trainModel(ratingsAls, 10, 10)

        // we only want to recommend among popular movies
        val ratingCounts = ratings.groupBy("movieID")
                                .count()
                                .filter("count > 100")

        // prepare RDD with user id and list of popular movies to recommend
        val popularMovies = ratingCounts
                            .select("movieId")
                            .withColumn("userID", lit(userId))
                            .rdd
                            .map(row => (row(0).toString.toInt, row(1).toString.toInt))
        val popularMoviesRDD = spark.sparkContext.parallelize(popularMovies.collect())

        // Predicting the ratings for other movies for the user we chose
        val recommendations = model.predict(popularMoviesRDD)

        // Taking the best of those ratings
        val topRecommendations = recommendations
                                .collect()
                                .sortBy(x => x.rating)(Ordering.Double.reverse)
                                .take(7)

        return topRecommendations
    } 
}