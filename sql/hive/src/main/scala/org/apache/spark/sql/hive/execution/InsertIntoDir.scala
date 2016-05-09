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

package org.apache.spark.sql.hive.execution

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, TextInputFormat}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.util.SerializableJobConf


case class InsertIntoDir(
    path: String,
    isLocal: Boolean,
    overwrite: Boolean,
    child: SparkPlan) extends SaveAsHiveFile {

  @transient private val sessionState = sqlContext.sessionState.asInstanceOf[HiveSessionState]
  def output: Seq[Attribute] = Seq.empty

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val hadoopConf = sessionState.newHadoopConf()

    val properties = new Properties()
    properties.put(serdeConstants.SERIALIZATION_LIB, classOf[LazySimpleSerDe].getName)

    val Array(cols, types) = child.output.foldLeft(Array("", ""))((r, a) => {
      r(0) = r(0) + a.name + ","
      r(1) = r(1) + a.dataType.typeName + ":"
      r
    })

    properties.put("columns", cols.dropRight(1))
    properties.put("columns.types", types.dropRight(1))

    val isCompressed =
      sessionState.conf.getConfString("hive.exec.compress.output", "false").toBoolean

    val tableDesc = new TableDesc(
      classOf[TextInputFormat],
      classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]],
      properties
    )

    val fileSinkConf = new FileSinkDesc(path, tableDesc, isCompressed)

    val jobConf = new JobConf(hadoopConf)
    val jobConfSer = new SerializableJobConf(jobConf)

    val writerContainer = new SparkHiveWriterContainer(
        jobConf,
        fileSinkConf,
        child.output)

    writerContainer.driverSideSetup()

    @transient val outputClass = writerContainer.newSerializer(tableDesc).getSerializedClass
    saveAsHiveFile(child.execute(), outputClass, fileSinkConf, jobConfSer, writerContainer,
      isCompressed)

    val outputPath = FileOutputFormat.getOutputPath(jobConf)

    if(isLocal) {
      outputPath.getFileSystem(hadoopConf).copyToLocalFile(true, outputPath, new Path(path))
      log.info(s"Results copied to local dir ${path}")
    } else {
      log.info(s"Results copied to path ${outputPath}")
    }

    Seq.empty[InternalRow]
  }

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult.asInstanceOf[Seq[InternalRow]], 1)
  }
}
