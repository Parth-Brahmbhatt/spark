/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.io.ObjectOutputStream
import java.text.NumberFormat
import java.util.{Date, Map, HashMap}

import com.netflix.dse.mds.data.DataField
import com.netflix.utils.ConfigurationUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

import com.netflix.dse.mds.PartitionMetrics
import com.netflix.dse.storage.output.{StorageHelper, DeltaOutputCommitter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator, Utilities}
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.hive.common.FileUtils

import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.{Logging, SerializableWritable, SparkHadoopWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableJobConf

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on [[SparkHadoopWriter]].
 */
private[hive] class SparkHiveWriterContainer(
    jobConf: JobConf,
    fileSinkConf: FileSinkDesc,
    franklinTblName: String,
    partColNames: Array[String])
  extends Logging
  with SparkHadoopMapRedUtil
  with Serializable {

  import SparkHiveWriterContainer._

  private val defaultPartName = jobConf.get(
    ConfVars.DEFAULTPARTITIONNAME.varname, ConfVars.DEFAULTPARTITIONNAME.defaultStrVal)

  private val now = new Date()
  private val tableDesc: TableDesc = fileSinkConf.getTableInfo
  // Add table properties from storage handler to jobConf, so any custom storage
  // handler settings can be set to jobConf
  if (tableDesc != null) {
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
  }
  private val conf = new SerializableJobConf(jobConf)
  private val finalLocation: String = fileSinkConf.getDirName
  private val partitionMetadata: Map[String, PartitionMetrics] = new HashMap()

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null
  private val batchidPattern: Regex = "(.*)/batchid=[\\d]+$".r

  @transient private var writers: mutable.HashMap[String, FileSinkOperator.RecordWriter] = _
  @transient private lazy val testing = sys.props.contains("spark.testing")
  @transient private lazy val committer = if (testing) {
    conf.value.getOutputCommitter
  } else {
    new DeltaOutputCommitter(
      franklinTblName,
      getUniqueId(),
      getFinalLocation(),
      true,
      null)
  }

  @transient private lazy val jobContext = newJobContext(conf.value, jID.value)
  @transient private lazy val taskContext = newTaskAttemptContext(conf.value, taID.value)
  @transient private lazy val outputFormat =
    conf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

  def driverSideSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    committer.setupJob(jobContext)
  }

  def executorSideSetup(jobId: Int, splitId: Int, attemptId: Int) {
    setIDs(jobId, splitId, attemptId)
    setConfParams()
    committer.setupTask(taskContext)
    initWriters()
  }

  private def getUniqueId(): String = {
    return conf.value.get("uuid")
  }

  private def getBatchId(): String = {
    return conf.value.get("batchid")
  }

  private def getFinalLocation(): String = {
    finalLocation match {
      case batchidPattern(rootPath) =>
        // If there is already a batchid subdir in finalLocation, remove it
        // since a new batchid subdir will be inserted by dynamicPartPath.
        // This is needed for non-partitioned tables.
        rootPath
      case _ =>
        // If not, use finalLocation as is.
        finalLocation
    }
  }

  private def getMetricClasses(): String = {
    return conf.value.get("dse.storage.partition.metrics")
  }

  private def getFieldMetricClasses(): String = {
    return conf.value.get("dse.storage.field.metrics")
  }

  private def getOutputName: String = {
    val numberFormat = NumberFormat.getInstance()
    numberFormat.setMinimumIntegerDigits(5)
    numberFormat.setGroupingUsed(false)
    val extension = Utilities.getFileExtension(conf.value, fileSinkConf.getCompressed, outputFormat)
    "part-" + numberFormat.format(splitID) + extension
  }

  def getLocalFileWriter(row: InternalRow, schema: StructType):
      FileSinkOperator.RecordWriter = {
    def convertToHiveRawString(col: String, value: Any): String = {
      val raw = String.valueOf(value)
      schema(col).dataType match {
        case DateType => DateTimeUtils.dateToString(raw.toInt)
        case _: DecimalType => BigDecimal(raw).toString()
        case _ => raw
      }
    }

    val dynamicPartPath =
      if (partColNames != null) {
        val nonDynamicPartLen = row.numFields - partColNames.length
        partColNames.zipWithIndex.map { case (colName, i) =>
          val rawVal = row.get(nonDynamicPartLen + i, schema(colName).dataType)
          val string = if (rawVal == null) null else convertToHiveRawString(colName, rawVal)
          val colString =
            if (string == null || string.isEmpty) {
              defaultPartName
            } else {
              FileUtils.escapePathName(string, defaultPartName)
            }
          s"/$colName=$colString"
        }.mkString + (if (testing) "" else s"/batchid=${getBatchId()}")
      } else {
        if (testing) "" else s"/batchid=${getBatchId()}"
      }

    val columnDataFields = schema.iterator
      .map(x => new DataField(x.name, x.dataType.simpleString)).toList
    val dynamicPartPathWithNoStartingSlash = dynamicPartPath.stripPrefix("/")
    // TODO: Add dummy partition metrics. DeltaOutputCommitter reads the metadata file and
    // determines which partitions in franklin should get updated. So even if we don't have
    // partition metrics yet, we add dummy one for now.
    if (!partitionMetadata.containsKey(dynamicPartPathWithNoStartingSlash)) {
      logInfo(s"Add ${dynamicPartPathWithNoStartingSlash} to partition metadata map")
      val pm: PartitionMetrics = new PartitionMetrics(
        getMetricClasses(), getFieldMetricClasses(), new HivePartitionMetricHelper())
      pm.setSchema(columnDataFields.asJava)
      pm.initialize(ConfigurationUtils.newConfiguration(conf.value.iterator()))
      partitionMetadata.put(dynamicPartPathWithNoStartingSlash, pm)
    }
    partitionMetadata.get(dynamicPartPathWithNoStartingSlash).update(new HiveDataTuple(row))

    def newWriter(): FileSinkOperator.RecordWriter = {
      val newFileSinkDesc = new FileSinkDesc(
        fileSinkConf.getDirName + dynamicPartPath,
        fileSinkConf.getTableInfo,
        fileSinkConf.getCompressed)
      newFileSinkDesc.setCompressCodec(fileSinkConf.getCompressCodec)
      newFileSinkDesc.setCompressType(fileSinkConf.getCompressType)

      val path = if (testing) {
        FileOutputFormat.getTaskOutputPath(
          conf.value,
          (dynamicPartPath + "/" + getOutputName).stripPrefix("/"))
      } else {
        val sh: StorageHelper = new StorageHelper(taskContext, getUniqueId(), getFinalLocation())
        if(partColNames != null) {
          val partColNamesSet: java.util.Set[String] = partColNames.toSet.asJava
          sh.setPartitionKeys(partColNamesSet)
        }
        val base: String = sh.getBaseTaskAttemptTempLocation
        assert(base != null, "Undefined job output-path")
        val workPath = new Path(base, dynamicPartPathWithNoStartingSlash)
        new Path(workPath, getOutputName)
      }

      HiveFileFormatUtils.getHiveRecordWriter(
        conf.value,
        fileSinkConf.getTableInfo,
        conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
        newFileSinkDesc,
        path,
        Reporter.NULL)
    }

    writers.getOrElseUpdate(dynamicPartPath, newWriter())
  }

  def close(): Unit = {
    writers.values.foreach(_.close(false))
    commit()
  }

  def commitJob() {
    // This is a hack to avoid writing _SUCCESS mark file. In lower versions of Hadoop (e.g. 1.0.4),
    // semantics of FileSystem.globStatus() is different from higher versions (e.g. 2.4.1) and will
    // include _SUCCESS file when glob'ing for dynamic partition data files.
    //
    // Better solution is to add a step similar to what Hive FileSinkOperator.jobCloseOp does:
    // calling something like Utilities.mvFileToFinalPath to cleanup the output directory and then
    // load it with loadDynamicPartitions/loadPartition/loadTable.
    val oldMarker = conf.value.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    committer.commitJob(jobContext)
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, oldMarker)
  }

  private def initWriters() {
    // NOTE: This method is executed at the executor side.
    // Actual writers are created for each dynamic partition on the fly.
    writers = mutable.HashMap.empty[String, FileSinkOperator.RecordWriter]
  }

  private def commit() {
    if (!testing) {
      // In DseStorage, metadata is written by PartitionedRecordWriter. But since in Spark.
      // we don't use it, we instead write metadata in commit task.
      val sh: StorageHelper = new StorageHelper(taskContext, getUniqueId(), getFinalLocation())
      if (partColNames != null) {
        val partColNamesSet: java.util.Set[String] = partColNames.toSet.asJava
        sh.setPartitionKeys(partColNamesSet)
      }
      val base: String = sh.getBaseTaskAttemptTempLocation
      val file: Path = new Path(base, "_metadata")
      val fs: FileSystem = file.getFileSystem(conf.value)
      val os: ObjectOutputStream = new ObjectOutputStream(fs.create(file, true))
      os.writeObject(partitionMetadata)
      os.close
    }

    SparkHadoopMapRedUtil.commitTask(committer, taskContext, jobID, splitID)
  }

  private def setIDs(jobId: Int, splitId: Int, attemptId: Int) {
    jobID = jobId
    splitID = splitId
    attemptID = attemptId

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobId))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString)
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString)
    conf.value.set("mapred.task.id", taID.value.toString)
    conf.value.setBoolean("mapred.task.is.map", true)
    conf.value.setInt("mapred.task.partition", splitID)
  }
}

private[hive] object SparkHiveWriterContainer {
  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }
}
