import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object movieRating5 {

  case class MovieRatings(movieName: String, rating: Double)

  case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("movieRating5")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val movies_critics = Seq(
      MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
      MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
    val ratings = movies_critics.toDF

    val explodeRatings = ratings.withColumn("movieRatings", explode($"movieRatings"))
      .select($"name", $"movieRatings.movieName", $"movieRatings.rating")

    val soln = explodeRatings.groupBy("name")
      .pivot("movieName")
      .agg(first("rating"))

    soln.show()

    spark.close()

  }

}
