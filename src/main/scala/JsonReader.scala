import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser._
import io.circe.syntax._
import WineMag._

case class WineMag(
  id: Option[Int],
  country: Option[String],
  points: Option[Int],
  title: Option[String],
  variety: Option[String],
  winery: Option[String]
                  )

object WineMag {
  implicit val decoder: Decoder[WineMag] = deriveDecoder[WineMag]
}

object JsonReader {


  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("USAGE: <winemag_file.json>")
      sys.exit(1)
    }

    val spark = SparkSession.builder().appName("Json reader").getOrCreate()

    val res: RDD[String] = spark.sparkContext.textFile(args(0))

    res.map(decode[WineMag](_))
      .collect
      .foreach(_.map(println).left.map(e => println(s"ERROR: $e")))
  }

}
