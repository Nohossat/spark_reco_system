package recommendation

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Kmeans {
    def trainModel(data: Dataset[Row], spark : SparkSession) : Unit = {
        val Array(training, test) = data.randomSplit(Array(0.8, 0.2))

        // Trains a k-means model.
        val kmeans = new KMeans().setK(2).setSeed(1L)
        val model = kmeans.fit(training)

        // Make predictions
        val predictions = model.transform(test)

        // Evaluate clustering by computing Silhouette score
        val evaluator = new ClusteringEvaluator()

        val silhouette = evaluator.evaluate(predictions)
        println(s"Silhouette with squared euclidean distance = $silhouette")

        // Shows the result.
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)
    }
}
