import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{callUDF, input_file_name}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import schema_config.{ConfigReader, DataframeSchema}
import spray.json._


object Main {


  def main(args: Array[String]): Unit = {

    //Allouer de la mémoire
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Pi").set("spark.driver.host", "localhost")
    conf.set("spark.testing.memory", "2147480000")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    //****************************************************

    Logger.getLogger("org").setLevel(Level.OFF)
    //val sparkSession = SparkSession.builder().master("local").getOrCreate()
    //sparkSession.read.csv("path/source", "path/file1", "path/file2").coalesce(1)

    val session = SparkSession.builder().master("local").getOrCreate()

    val schema = StructType(Seq(
      StructField("amount", IntegerType),
      StructField("base_currency", StringType),
      StructField("currency", StringType),
      StructField("exchange_rate", DoubleType)
    ))

    val df = session.read.option("delimiter",",").schema(schema).csv("*.csv")
    df.printSchema()
    println(df.schema)
    df.show


    session.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
    val df2 = df.withColumn("date", callUDF("get_file_name", input_file_name()))
    df2.show(40,false)




    val dfWithRightColumnNames = df2.toDF()
    dfWithRightColumnNames.write.partitionBy("date").mode(SaveMode.Overwrite).csv("output")
    dfWithRightColumnNames.show()





    val confreader= ConfigReader.readConfig("Config/Config.json")
    val configuration= confreader.parseJson.convertTo[JsonConfig]

    println(configuration.csvOptions)

    val dfSchema= DataframeSchema.buildDataframeSchema(schema_config.fields)
    val data = DataFrameReader.readCsv("*.csv", schema_config.CsvOptions, dfSchema)
    data.show



  }
}