package recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import java.nio.file.{Paths, Files}
import scala.collection.mutable.ArrayBuffer

class Algo {
    var bestRMSE : Double = 0
    var bestParams : ArrayBuffer[Double] = ArrayBuffer[Double]()
    var bestModel : MatrixFactorizationModel = null

    def trainModel(spark : SparkSession, ratings: RDD[Rating], rank : Int, numIter : Int): MatrixFactorizationModel = {
        
        val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
        var model : MatrixFactorizationModel = null

        // load existing model if exists
        if (Files.exists(Paths.get("als_model"))) {
            model = MatrixFactorizationModel.load(spark.sparkContext, "als_model")
        } else {
            // params grid search
            val params = Map(
                "lambda" -> Array(0.001, 0.01, 0.1),
                "numIter" -> Array(4.0, 10.0, 15.0),
                "rank" -> Array(5.0, 7.0, 10.0)
            )
            
            var idx : Int = params.size - 1

            println("start grid search")
            model = doGridSearch(training, params, idx, ArrayBuffer())
            println("done with grid search")
            model.save(spark.sparkContext, "als_model")

            // metrics
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
            }.mean())
        return RMSE
    }

    def doGridSearch(ratings: RDD[Rating], grid_search_params : Map[String, Array[Double]], idx : Int, currentParams : ArrayBuffer[Double]): MatrixFactorizationModel = {
        val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
        val params = grid_search_params.keys.toList // get grid search paramaters names
        var model : MatrixFactorizationModel = null


        if (idx == 0) {
            for (param_value <- grid_search_params(params(idx))) {
                val updated_params : ArrayBuffer[Double] = currentParams.clone
                updated_params += param_value

                //  launch grid_search
                println(updated_params.mkString(" "))
                val ArrayBuffer(rank, numIter, lambda) = updated_params
                model = ALS.train(training, rank.toInt, numIter.toInt, lambda)
                val rmse = getMetrics(training, test, model)
                println(rmse)

                // update best model + config if better
                if (bestRMSE < rmse) {
                    bestRMSE = rmse
                    bestParams = updated_params
                    bestModel = model
                }
            }
        }

        if (idx > 0) {
            for (param_value <- grid_search_params(params(idx))) {

                val updated_params : ArrayBuffer[Double] = currentParams.clone
                updated_params += param_value
                println(updated_params.mkString(" "))

                val param_idx : Int = idx - 1
                doGridSearch(ratings, grid_search_params, param_idx, updated_params)
            }
        }

        return model
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