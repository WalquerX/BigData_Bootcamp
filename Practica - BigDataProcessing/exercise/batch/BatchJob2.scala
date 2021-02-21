package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, lit, max, min, sum, when, window}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BatchJobImpl extends BatchJob{
  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
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
      .select("antenna_id", "bytes")
      .groupBy( $"antenna_id", $"bytes")
      .agg(sum($"bytes").as("total_bytes"))
      .select($"antenna_id".as("id"),
        $"total_bytes".as("value"),
        lit(s"Antenna_total_bytes").as("type")
      )
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .save(s"$storageRootPath/historical")
  }

  //def main(args: Array[String]): Unit = run(args)


  def main(args: Array[String]): Unit = {
    val argsTime = "2021-02-20T18:00:00Z"


    val storageDF = readFromStorage("/tmp/practica/cold/data", OffsetDateTime.parse(argsTime))
    val metadataDF = readAntennaMetadata("jdbc:postgresql://34.70.9.147:5432/postgres","user_metadata","postgres", "keepcoding")
    val enrichDF = enrichAntennaWithMetadata(storageDF, metadataDF)
    val aggbyAntenna = computeBytesByAntenna(enrichDF)

    writeToJdbc(aggbyAntenna, "jdbc:postgresql://34.70.9.147:5432/postgres","bytes_hourly","postgres", "keepcoding")

    writeToStorage(storageDF, "/tmp/practica/")
  }
}
