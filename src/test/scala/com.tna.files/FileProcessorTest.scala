package com.tna.files

import java.nio.file.{Paths, StandardOpenOption}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import cats.effect.{Blocker, IO}
import fs2.{Stream, io, text}
import org.scalatest.{Assertion, Succeeded}

class FileProcessorTest extends AnyWordSpec with Matchers {

  val processor = new FileProcessor()

  "updateRowValues" should {

    "throw java.nio.file.NoSuchFileException when the file does not exist" in {
      val f = new FileProcessor()
      an [java.nio.file.NoSuchFileException] should be thrownBy f.updateRowValues(
        "file_does_not_exist.csv",
        "origin",
        "Londom",
        "London"
      )
    }

    "throw java.lang.IndexOutOfBoundsException when the column does not exist" in {
      val f = new FileProcessor()
      an [java.lang.IndexOutOfBoundsException] should be thrownBy f.updateRowValues(
        "test.csv",
        "missingColumn",
        "Londom",
        "London"
      )
    }

    "update the column with the new value" in {
      val f = new FileProcessor()
      val csvFileName = "test.csv"
      val columnPosition = 1
      val oldValue = "Londom"
      val newValue = "London"
      f.updateRowValues(
        csvFileName,
        "origin",
        oldValue,
        newValue
      )

      val projectDirPath = Paths.get("").toAbsolutePath()
      val filePath = projectDirPath.resolve(s"src/main/resources/$csvFileName")

      def assertColumnChanged(lineNumber: Int, line: String) = {
        val columnValues = line.split(",").toVector
          columnValues(columnPosition).trim == newValue
      }

      val program =  Stream.resource(Blocker[IO]).flatMap { blocker =>
        implicit val cs = IO.contextShift(blocker.blockingContext)
        io.file.readAll[IO](filePath, blocker, 4096)
          .through(text.utf8Decode)
          .through(text.lines)
          .filter(s => !s.trim.isEmpty)
          .mapAccumulate(0)( (lineNumber, line) => (lineNumber, assertColumnChanged(lineNumber, line)))
          .map(_._2)
      }

      val result = program.compile.toList.unsafeRunSync()

      result(3) shouldBe true
    }

  }

}
