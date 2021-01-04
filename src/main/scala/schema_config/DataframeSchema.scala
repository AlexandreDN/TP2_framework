package schema_config


import org.apache.avro.Schema.Field
import org.apache.spark.sql.types._
import org.apache.spark
import org.apache.spark.sql.types._


object DataframeSchema {

  def buildDataframeSchema(fields: Seq[Field]): StructType = {
    val structFieldsList=fields.map ( field =>
    {StructField(field.name, mapPrimitiveTypesToSparkTypes(field.fillWithDaysAgo))
    })

    StructType(structFieldsList)

  }

  def mapPrimitiveTypesToSparkTypes(fillWithDaysAgo: Int): DataType = {
    fillWithDaysAgo match{
      case "String" => StringType
      case "Double" => DoubleType
      case "Long" => LongType
      case "Integer" => IntegerType
      case _ => StringType
    }

  }


}
