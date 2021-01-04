import org.apache.spark.sql.execution.datasources.csv.CSVOptions
import org.apache.spark.sql.types.IntegerType
import org.spark_project.jetty.util.Fields
import spray.json.{DefaultJsonProtocol, JsonFormat}

case class JsonConfig(name: String, CSVOptions: CSVOptions, fields: Seq[Fields])
case class CsvOptions(header: String, interSchema: String, delimiter: String)
case class Field(date: String, fillWithDaysAgo: Int)

object JsonConfigProtocol extends DefaultJsonProtocol{

  implicit val csvOptions: JsonFormat[CsvOptions] = lazyFormat(jsonFormat3(CsvOptions))
  implicit val field: JsonFormat[Field] = lazyFormat(jsonFormat2(Field))
  implicit val jsonConfig: JsonFormat[JsonConfig] = lazyFormat(jsonFormat3(JsonConfig))

}