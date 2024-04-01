package com.nga.hlibliubchenko

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import fs2.data.csv._
import fs2.io.file.{Files, Path}

import scala.collection.mutable

case class SensorData(sensorId: String, humidity: Option[Int])
object SensorData {
  implicit val csvDecoder: CsvRowDecoder[SensorData, String] = row =>
    for {
      sensorId <- row.as[String]("sensor-id")
      humidity <- row.as[String]("humidity")
    } yield SensorData(sensorId, humidity.toIntOption)
}

class ProcessingStatsAcc(
    var processedFilesCount: Int = 0,
    var processedMeasurements: Int = 0,
    var failedMeasurement: Int = 0
) {
  def incProcessedFilesCount(): ProcessingStatsAcc = { processedFilesCount += 1; this }
  def incMeasurements(success: Boolean): ProcessingStatsAcc = {
    processedMeasurements += 1; if (!success) failedMeasurement += 1; this
  }
}

class SensorStats(var min: Int = -1, var max: Int = -1, var sum: Int = -1, var count: Long = -1) {
  def update(humidity: Int): Unit =
    if (min == -1) {
      min = humidity
      max = humidity
      sum = humidity
      count = 1
    } else {
      min = math.min(min, humidity)
      max = math.max(max, humidity)
      sum += humidity
      count += 1
    }

  lazy val avg: Long = if (count > 0L) sum / count else -1
}

class SensorStatsAcc(stats: mutable.Map[String, SensorStats] = mutable.Map.empty) {
  def update(record: SensorData): SensorStatsAcc = {
    val acc = stats.getOrElseUpdate(record.sensorId, new SensorStats())
    record.humidity.foreach(acc.update)
    this
  }

  def sort: Seq[(String, SensorStats)] = stats.toSeq.sortBy(_._2.avg)(Ordering[Long].reverse)
}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = compute(args)
    .flatMap(IO.println)
    .as(ExitCode.Success)

  private[hlibliubchenko] def compute(args: List[String]): IO[String] = {
    val dir = args.headOption match {
      case Some(value) => value
      case None        => throw new IllegalArgumentException("A path to the input data directory is not provided")
    }

    // This mutable state could be replaced by multiple calls to the `.zipWithIndex' method (at the file level, at the
    // line level, and after parsing the csv line). After that we can use all these indices to calculate all statistic.
    // But I believe that current mutable approach is better from performance and code readability point of view.
    val acc: ProcessingStatsAcc = new ProcessingStatsAcc()

    val sensorsData = readDir(dir, acc)
      .flatMap(readFile(_, acc))

    calculateStats(sensorsData)
      .map(printStats(acc, _))
  }

  private[hlibliubchenko] def readDir(dir: String, acc: ProcessingStatsAcc): Stream[IO, Path] = Files[IO]
    .list(Path(dir), "*.csv")
    .evalTap(_ => IO(acc.incProcessedFilesCount()))

  private[hlibliubchenko] def readFile(file: Path, acc: ProcessingStatsAcc): Stream[IO, SensorData] = Files[IO]
    .readUtf8(file)
    .through(decodeUsingHeaders[SensorData]())
    .evalTap(r => IO(acc.incMeasurements(r.humidity.isDefined)))

  private[hlibliubchenko] def calculateStats(sensorsData: Stream[IO, SensorData]): IO[Seq[(String, SensorStats)]] =
    sensorsData.compile
      .fold(new SensorStatsAcc()) { case (acc, record) => acc.update(record) }
      .map(_.sort)

  private[hlibliubchenko] def printStats(processingStats: ProcessingStatsAcc, sensorStats: Seq[(String, SensorStats)]) =
    s"""Num of processed files: ${processingStats.processedFilesCount}
       |Num of processed measurements: ${processingStats.processedMeasurements}
       |Num of failed measurements: ${processingStats.failedMeasurement}
       |
       |Sensors with highest avg humidity:
       |
       |sensor-id,min,avg,max
       |${sensorStats.map { case (id, stats) => printSensorStats(id, stats) }.mkString("\n")}
       |""".stripMargin

  private def printSensorStats(sensorId: String, stats: SensorStats): String =
    (sensorId +: Seq(stats.min, stats.avg, stats.max).map(v => if (v == -1) "NaN" else v.toString)).mkString(",")
}
