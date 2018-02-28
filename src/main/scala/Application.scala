import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {
  val two = 2
  val three = 3
  val four = 4
  val five = 5
  val six = 6
  val conf = new SparkConf()
  Logger.getLogger("org").setLevel(Level.OFF)

  conf.setAppName("football data")
  conf.setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  val filepath = "src/main/resources/data.csv"
  //loading .csv file
  val footballDataDF = spark.read.option("header", "true").option("inferschema", "true").csv(filepath)
  val footballData = footballDataDF.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").toDF
  val object1 = new Operation
  object1.findingHomeTeam(footballData, spark).show
  object1.topTenTeam(footballData, spark).show
}
