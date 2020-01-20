package com.tna.files

import java.nio.file.{Paths, StandardOpenOption}
import cats.effect.{Blocker, IO}
import fs2.{Stream, io, text}

class FileProcessor() {

  private def createProgram(csvFileName: String,
                      column: String,
                      oldValue: String,
                      newValue: String) = {
    def replaceValue(line: String, changedColumnPosition: Int): String = {
      val columnValues = line.split(",").toVector
      val maybeUpdatedLine = if (columnValues(changedColumnPosition).trim == oldValue) {
        columnValues.updated(changedColumnPosition, newValue)
      } else {
        columnValues
      }
      maybeUpdatedLine.mkString(",")
    }
    val projectDirPath = Paths.get("").toAbsolutePath()
    val filePath = projectDirPath.resolve(s"src/main/resources/$csvFileName")
    val program =  Stream.resource(Blocker[IO]).flatMap { blocker =>
      implicit val cs = IO.contextShift(blocker.blockingContext)
      for {
        changedColumnPosition <- io.file.readAll[IO](filePath, blocker, 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .head
         .map( columns => columns.split(",").toVector.indexWhere(_.trim == column))

      } yield io.file.readAll[IO](filePath, blocker, 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .filter(s => !s.trim.isEmpty)
        .map(line => replaceValue(line, changedColumnPosition))
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(
          io.file.writeAll(
            filePath,
            blocker,
            List(StandardOpenOption.CREATE)
          )
        )
    }
    program.flatten
  }

  def updateRowValues(csvFileName: String,
                             column: String,
                             oldValue: String,
                             newValue: String) = {
    val program = createProgram(csvFileName, column, oldValue, newValue)
    program.compile.drain.unsafeRunSync()
  }
}


