package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamingJobImpl2 extends StreamingJob {

  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()


  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val schema = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json($"value".cast(StringType), schema).as("json"))
      .select("json.*")
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
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
      ).drop(metadataDF("id"))
  }

  override def computeBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select("timestamp","antenna_id", "bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"antenna_id", window($"timestamp", "2 minutes") )
      .agg(sum($"bytes").as("total_bytes"))
      .select($"window.start".cast(TimestampType).as("timestamp"),
        $"antenna_id".as("id"),
        $"total_bytes".as("value"),
        lit(s"Antenna_total_bytes").as("type")
      )
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch{(df: DataFrame, id : Long) =>
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
      }.start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val metadataDF = readAntennaMetadata("jdbc:postgresql://34.70.9.147:5432/postgres","user_metadata","postgres","keepcoding")


    val antennaStreamDF = parserJsonData(readFromKafka("35.198.25.17:9092","devices"))


    val writeToStorageFut = writeToStorage(antennaStreamDF, "/tmp/practica/cold")

    val writeToJdbcFut = writeToJdbc(computeBytesByAntenna(enrichAntennaWithMetadata(antennaStreamDF, metadataDF)),"jdbc:postgresql://34.70.9.147:5432/postgres","bytes","postgres","keepcoding" )


    val f = Future.sequence(Seq(writeToStorageFut, writeToJdbcFut))
    Await.result(f, Duration.Inf)

  }
}
