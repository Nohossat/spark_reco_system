package recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import java.nio.file.{Paths, Files}

class Algo {
    def trainModel(spark : SparkSession, ratings: RDD[Rating], rank : Int, numIter : Int): MatrixFactorizationModel = {
        
        val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
        
        // load existing model if exists
        var model : MatrixFactorizationModel = null

        if (Files.exists(Paths.get("als_model"))) {
            model = MatrixFactorizationModel.load(spark.sparkContext, "als_model")
        } else {
            model = ALS.train(training, rank, numIter, 0.01)
            model.save(spark.sparkContext, "als_model")
            val rmse = getMetrics(training, test, model)
            println("Mean Squared Error = " + rmse)
        }

        return model
    }

    def getMetrics(training : RDD[Rating], test : RDD[Rating], model: MatrixFactorizationModel) : Double = {
        val testUserProducts = test
                                .map(rating => (rating.user, rating.product))

        val predictions = model.predict(testUserProducts)
                            .map { case Rating(user, product, rate) => ((user, product), rate) }

        val ratesAndPreds = test
                            .map { case Rating(user, product, rate) => ((user, product), rate)}
                            .join(predictions)

        val RMSE = math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
            val err = (r1 - r2)
            err * err
            }.mean()) // current : 0.88
        return RMSE
    }

    def getPredictions(spark : SparkSession, ratings: Dataset[Row], ratingsAls: RDD[Rating], userId: Int): Array[Rating] = {
        // train model
        val userProducts = ratings.select(ratings("_id"), ratings("movieId"))
        val model = trainModel(spark, ratingsAls, 10, 10)

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