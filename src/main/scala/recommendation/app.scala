package recommendation

object Main extends App {
    val mongo = new Mongo()
    val algo = new Algo()

    val ratings = mongo.getUserMovieRatingsIds()
    // val model = algo.trainModel(ratings, 10, 10)
    
    mongo.getUserPreferences(20)
} 