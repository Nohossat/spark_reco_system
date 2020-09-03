package recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

class Algo {
    def trainModel(ratings: RDD[Rating], rank : Int, numIter : Int): MatrixFactorizationModel = {
        var model = ALS.train(ratings, rank, numIter, 0.01) // what is the last value ?

        return model
    }

    /*
    def getPredictions(userId: Int): Unit = {
        // Predicting the ratings for other movies for the user we chose
        val recommendations = model.predict(popularMoviesRDD)

        // Taking the best of those ratings
        val topRecommendations = recommendations
                                .collect()
                                .sortBy(x => x.rating)(Ordering.Double.reverse)
                                .take(7)
        
        // Printing the top recommendations
        println("\nBest 7 Recommendations:")
        for (recommendation <- topRecommendations) {
            println(movieNames(recommendation.user))
        }
    } 
    */
}