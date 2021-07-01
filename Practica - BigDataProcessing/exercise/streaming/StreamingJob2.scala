package io.keepcoding.spark.exercise.streaming
import org.apache.spark.sql.functions.{col, dayofmonth, from_json, hour, lit, month, sum, window, year}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamingJob2 extends StreamingJob {
  override val spark: SparkSession =
  SparkSession
    .builder()
    .master("local[*]")
    .appName("Streaming Job")
    .getOrCreate()

  import spark.implicits._

  val devicesSchema = StructType(Seq(
    StructField("timestamp", LongType, nullable = false),
    StructField("id", StringType, nullable = false),
    StructField("antenna_id", StringType, nullable = false),
    StructField("bytes", LongType, nullable = false),
    StructField("app", StringType, nullable = false)
  ))


  override def readFromKafka(kafkaServer: String, topic: String): DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(from_json($"value".cast(StringType), devicesSchema).as("json"))    // para poder usar el $ se necesita importar implicits
      .select("json.*")
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read                //solo read porque los datos son estáticos
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF
      .join(metadataDF,
        antennaDF("id") === metadataDF("id")
      ).drop(metadataDF("id")) //se quita una de las columnas id
  }

   def computeDevicesCountByCoordinates(dataFrame: DataFrame, aggColumn: String): DataFrame = {
    dataFrame
    .select($"timestamp".cast(TimestampType).as("timestamp"), col(aggColumn), $"bytes")
    .withWatermark("timestamp", "1 minute")
    .groupBy(col(aggColumn), window($"timestamp", "2 minutes"))
    .agg(sum($"bytes").as("total_bytes"))
    .select(
      $"window.start".cast(TimestampType).as("timestamp"),
      col(aggColumn).as("id"),
      $"total_bytes".as("value"),
      lit(s"${aggColumn}_total_bytes").as("type")
    )
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch{ (df: DataFrame, id: Long) =>  //ya es un dataframe normal (no hay jdbc en streaming)
        df
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()  //punto final del programa?
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year", year($"timestamp".cast(TimestampType)))
      .withColumn("month", month($"timestamp".cast(TimestampType)))
      .withColumn("day", dayofmonth($"timestamp".cast(TimestampType)))
      .withColumn("hour", hour($"timestamp".cast(TimestampType)))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")  // por si se cae
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit  = {

    val metadataDF = readAntennaMetadata("jdbc:postgresql://35.199.112.5:5432/postgres","user_metadata","postgres","keepcoding")

    val antennaStreamDF = parserJsonData(readFromKafka("34.95.140.1:9092","devices"))

    val antenna_byte_sum = computeDevicesCountByCoordinates(enrichAntennaWithMetadata(antennaStreamDF, metadataDF), "antenna_id")
    val usuario_byte_sum = computeDevicesCountByCoordinates(enrichAntennaWithMetadata (antennaStreamDF, metadataDF), "id")
    val app_byte_sum = computeDevicesCountByCoordinates(enrichAntennaWithMetadata(antennaStreamDF, metadataDF), "app")

    val writeToStorageFut = writeToStorage(antennaStreamDF, "/tmp1/practica")


     val writeToJdbcFut_1 = writeToJdbc(antenna_byte_sum,
       "jdbc:postgresql://35.199.112.5:5432/postgres", "bytes", "postgres", "keepcoding"
     )
    val writeToJdbcFut_2 = writeToJdbc(usuario_byte_sum,
      "jdbc:postgresql://35.199.112.5:5432/postgres", "bytes", "postgres", "keepcoding"
    )
    val writeToJdbcFut_3 = writeToJdbc(app_byte_sum,
      "jdbc:postgresql://35.199.112.5:5432/postgres", "bytes", "postgres", "keepcoding"
    )

    val f = Future.sequence(Seq(writeToJdbcFut_1, writeToJdbcFut_2, writeToJdbcFut_3, writeToStorageFut))
    Await.result(f, Duration.Inf)

  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???
}
