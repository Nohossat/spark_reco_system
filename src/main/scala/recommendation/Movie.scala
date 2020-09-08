package recommendation

import com.typesafe.config.ConfigFactory
import scala.io.Source
import org.apache.spark.sql.functions.udf
import org.bson.Document
import ujson._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import shapeless.tag
import org.apache.commons.collections.map.LinkedMap
import java.net.{URL, HttpURLConnection}

class Movie {
  val api_key = ConfigFactory.load().getString("moviedb.key.value")

  case class MovieInfo(overview: Option[String] = None, tag_line: Option[String] = None, adult: Boolean, original_language: String, release_date: String, vote_count: Integer, vote_average: Double)

  @throws(classOf[java.io.IOException])
  def getMovieDescription(movieId : Int, 
                         connectTimeout: Int = 5000, 
                         readTimeout: Int = 5000, 
                         requestMethod: String = "GET") : MovieInfo = {
    // connect to API
    val url = s"https://api.themoviedb.org/3/movie/$movieId?api_key=$api_key"
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)

    val inputStream = connection.getInputStream

    val response = Source.fromInputStream(inputStream).mkString

    if (inputStream != null) inputStream.close
    // end connect to API

    val data = ujson.read(response)

    val makeMovieInfo = (data: ujson.Value) => MovieInfo(Some(data("overview").toString), 
                                                        Some(data("tagline").toString), 
                                                        data("adult").toString.toBoolean, 
                                                        data("original_language").toString, 
                                                        data("release_date").toString,
                                                        data("vote_count").toString.toInt, 
                                                        data("vote_average").toString.toDouble)

    return makeMovieInfo(data)
  }
}
