package com.nga.hlibliubchenko

import cats.effect.testing.scalatest.AsyncIOSpec
import com.nga.hlibliubchenko.MainSuite.basePath
import fs2.io.file.Path
import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable

class MainSuite extends AsyncWordSpec with AsyncIOSpec with Matchers {
  "E2E" should {
    "complain if input argument isn't provided" in {
      intercept[IllegalArgumentException] {
        Main.compute(List.empty)
      }.getMessage shouldBe "A path to the input data directory is not provided"
    }

    "print an empty output if no csv files present in the input directory" in {
      Main.compute(List(s"$basePath/emptyDir")).asserting { res =>
        res shouldBe """Num of processed files: 0
                       |Num of processed measurements: 0
                       |Num of failed measurements: 0
                       |
                       |Sensors with highest avg humidity:
                       |
                       |sensor-id,min,avg,max
                       |
                       |""".stripMargin
      }
    }

    "print an empty output if all csv files in the input directory are empty " in {
      Main.compute(List(s"$basePath/emptyFiles")).asserting {
        _ shouldBe """Num of processed files: 2
                     |Num of processed measurements: 0
                     |Num of failed measurements: 0
                     |
                     |Sensors with highest avg humidity:
                     |
                     |sensor-id,min,avg,max
                     |
                     |""".stripMargin
      }
    }

    "generate correct result for the valid input data" in {
      Main.compute(List(s"$basePath/validInput")).asserting {
        _ shouldBe """Num of processed files: 2
                     |Num of processed measurements: 7
                     |Num of failed measurements: 2
                     |
                     |Sensors with highest avg humidity:
                     |
                     |sensor-id,min,avg,max
                     |s2,78,82,88
                     |s1,10,54,98
                     |s3,NaN,NaN,NaN
                     |""".stripMargin
      }
    }
  }

  "readDir" should {
    def read(dirName: String, acc: ProcessingStatsAcc) = Main
      .readDir(s"src/test/resources/$dirName", acc)
      .map(_.toString)
      .compile
      .toList

    "find all csv file" in {
      val acc = new ProcessingStatsAcc()
      read("validInput", acc).asserting { res =>
        res should contain theSameElementsAs List(1, 2).map(i => s"$basePath/validInput/leader-$i.csv")
        acc.processedFilesCount shouldBe 2
      }
    }

    "skip non csv file" in {
      val acc = new ProcessingStatsAcc()
      read("mixedFormats", acc).asserting { res =>
        res should contain theSameElementsAs List(1, 2).map(i => s"$basePath/mixedFormats/leader-$i.csv")
        acc.processedFilesCount shouldBe 2
      }
    }

    "do nothing if files not found" in {
      val acc = new ProcessingStatsAcc()
      read("emptyDir", acc).asserting { res =>
        res shouldBe res.empty
        acc.processedFilesCount shouldBe 0
      }
    }
  }

  "readFile" should {
    "read all lines" in {
      val acc = new ProcessingStatsAcc()
      Main.readFile(Path(s"$basePath/validInput/leader-1.csv"), acc).compile.toList.asserting { res =>
        res should contain theSameElementsAs List(
          SensorData("s1", Some(10)),
          SensorData("s2", Some(88)),
          SensorData("s1", None)
        )
      }
    }

    "convert lines with non defined humidity correctly" in {
      val acc = new ProcessingStatsAcc()
      Main.readFile(Path(s"$basePath/invalidInput/leader-1.csv"), acc).compile.toList.asserting { res =>
        res should contain theSameElementsAs List(
          SensorData("s1", None),
          SensorData("s2", None),
          SensorData("s1", None)
        )
      }
    }
  }

  "calculateStats" should {
    "aggregate stats correctly for the multiple occurrence of the same sensor" in {
      Main
        .calculateStats(
          fs2.Stream(
            SensorData("s1", Some(10)),
            SensorData("s2", Some(20)),
            SensorData("s3", Some(30)),
            SensorData("s1", Some(100)),
            SensorData("s2", Some(200)),
            SensorData("s1", Some(1000)),
            SensorData("s1", None),
            SensorData("s2", None),
            SensorData("s3", None)
          )
        )
        .asserting { res =>
          res.map(_._1) shouldBe Seq("s1", "s2", "s3")
          val v1 :: v2 :: v3 :: Nil = res.map(_._2)
          v1 should equal(new SensorStats(10, 1000, 1110, 3))
          v2 should equal(new SensorStats(20, 200, 220, 2))
          v3 should equal(new SensorStats(30, 30, 30, 1))
        }
    }

    "have expected values for the sensors that contains only NaN values" in {
      Main
        .calculateStats(
          fs2.Stream(
            SensorData("s1", None),
            SensorData("s2", None),
            SensorData("s3", None)
          )
        )
        .asserting { res =>
          val empty                 = new SensorStats(-1, -1, -1, -1)
          val v1 :: v2 :: v3 :: Nil = res.map(_._2)
          v1 should equal(empty)
          v2 should equal(empty)
          v3 should equal(empty)
        }
    }
  }

  "printStats" should {
    "generate output in the expected format" in {
      val stats = Seq("s1" -> new SensorStats(1, 2, 3, 4), "s2" -> new SensorStats(-1, -1, -1, -1))
      val expected = """Num of processed files: 10
                       |Num of processed measurements: 100
                       |Num of failed measurements: 20
                       |
                       |Sensors with highest avg humidity:
                       |
                       |sensor-id,min,avg,max
                       |s1,1,0,2
                       |s2,NaN,NaN,NaN
                       |""".stripMargin
      Main.printStats(new ProcessingStatsAcc(10, 100, 20), stats) shouldBe expected
    }
  }

  "ProcessingStatsAcc" should {
    implicit val eq: Equality[ProcessingStatsAcc] = (a, b) =>
      b match {
        case s: ProcessingStatsAcc => s.processedFilesCount == a.processedFilesCount &&
          s.processedMeasurements == a.processedMeasurements &&
          s.failedMeasurement == a.failedMeasurement
        case _ => false
      }

    "increment processed files correctly" in {
      new ProcessingStatsAcc().incProcessedFilesCount() should equal(new ProcessingStatsAcc(1, 0, 0))
    }

    "increment measurements in case of success correctly" in {
      new ProcessingStatsAcc().incMeasurements(true) should equal(new ProcessingStatsAcc(0, 1, 0))
    }

    "increment measurements in case of failure correctly" in {
      new ProcessingStatsAcc().incMeasurements(false) should equal(new ProcessingStatsAcc(0, 1, 1))
    }
  }

  "SensorStats.update" should {
    "work as expected" in {
      val stats = new SensorStats()

      stats.update(2)
      stats should equal(new SensorStats(2, 2, 2, 1))

      stats.update(3)
      stats should equal(new SensorStats(2, 3, 5, 2))

      stats.update(1)
      stats should equal(new SensorStats(1, 3, 6, 3))
    }
  }

  "SensorStatsAcc.sort" should {
    "work as expected" in {
      val (s1, s2, s3) = (
        "s1" -> new SensorStats(1, 1, 1, 1),
        "s2" -> new SensorStats(2, 2, 2, 1),
        "s3" -> new SensorStats(-1, -1, -1, -1)
      )
      new SensorStatsAcc(mutable.Map(s1, s2, s3)).sort should equal(Seq(s2, s1, s3))
    }
  }

  implicit val eq: Equality[SensorStats] = (a, b) =>
    b match {
      case sd: SensorStats => sd.max == a.max && sd.min == a.min && sd.sum == a.sum && sd.count == a.count
      case _               => false
    }
}
object MainSuite {
  private val basePath = "src/test/resources"
}
