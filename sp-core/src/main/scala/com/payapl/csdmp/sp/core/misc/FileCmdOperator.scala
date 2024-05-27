package com.payapl.csdmp.sp.core.misc

import com.payapl.csdmp.sp.core.Misc
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import org.apache.spark.sql.SparkSession

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, TimeoutException}
import scala.sys.process
import scala.sys.process.{Process, ProcessLogger}

case class FileCmdOperator(name: String, command: String) extends Misc {

  override def process(keys: List[String])(implicit sparkSession: SparkSession): Unit = {
    val commands: Array[String] = command.split(";").map(_.trim).filter(_.nonEmpty)
    for (cmd <- commands) {
      logInfo(s"Start to execute cmd: $cmd")

      val allowedCommandPrefixes: immutable.Seq[String] = List("gsutil", "hdfs", "hadoop", "ls", "cp", "copy", "mv", "rename", "cat", "chmod")
      if (!allowedCommandPrefixes.exists(cmd.toLowerCase.startsWith)) {
        throw new InvalidArgumentException(s"Unsupported command: $cmd, Allowed command prefixes are : ${allowedCommandPrefixes.mkString(", ")}")
      }
      val commandSeq: Seq[String] = cmd.split("\\s+").map(_.trim).toSeq
      val forbiddenForcedDeleteFlags = List("rm", "-rm", "rmdir", "-rmdir", "-f", "-force", "-rmr", "-rf")
      if (forbiddenForcedDeleteFlags.exists(flag => commandSeq.contains(flag))) {
        throw new InvalidArgumentException(s"Command $cmd contains forbidden force delete flag,Force delete is not allowed, Delete flag are ${forbiddenForcedDeleteFlags.mkString(", ")}")
      }

      logInfo(s"Command $cmd will be executed")
      val fileCommand: process.ProcessBuilder = Process(cmd)
      val stdout = new StringBuilder()
      val stderr = new StringBuilder()

      val exitCode: Int = -1
      val futureProcess: Future[Int] = Future {
        fileCommand.run(ProcessLogger(stdout append _, stderr append _)).exitValue()
      }
      try
        Await.result(futureProcess, 60.seconds)
      catch {
        case timeout: TimeoutException =>
          logError(s"Command $cmd is executed time out")
          throw new RuntimeException(s"Command execution timed out!")
        case other: Exception => {
          throw new RuntimeException(s"Command execution failed with error: ${other.getMessage}")
        }
      }
      if (exitCode == 0) {
        logInfo(s"Command is executed successfully and output $stdout")
      } else {
        logError(s"Command execution failed with error and error: $stderr")
        throw new RuntimeException(s"Command execution failed with error and error :$stderr")
      }
    }
  }
}