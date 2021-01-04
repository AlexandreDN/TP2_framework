import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataFrameReader {
  def readCsv(path: String, options: CsvOptions, schema: StructType)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .option("delimiter", options.delimiter)
      .option("header",options.header)
      .option("interSchema", options.interSchema)
      .schema(schema)
      .csv(path)
  }

}
