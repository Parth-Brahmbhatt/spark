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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapred.JobConf

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{RunnableCommand, SparkPlan}
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableJobConf

/**
  * :: DeveloperApi ::
  */
@DeveloperApi
case class WriteToDirectory(
                             path: String,
                             child: SparkPlan,
                             isLocal: Boolean,
                             desc: TableDesc) extends RunnableCommand with SaveAsHiveFile {

  override def output: Seq[Attribute] = child.output

  def run(sqlContext: SQLContext): Seq[Row] = {
    @transient val hiveContext = sqlContext.asInstanceOf[HiveContext]
    @transient lazy val context = new Context(hiveContext.hiveconf)
    @transient lazy val outputClass = newSerializer(desc).getSerializedClass
    val jobConf = new JobConf(hiveContext.hiveconf)
    val jobConfSer = new SerializableJobConf(jobConf)
    val targetPath = new Path(path)

    val writeToPath = if (isLocal) {
      val localFileSystem = FileSystem.getLocal(jobConf)
      val localPath = localFileSystem.makeQualified(targetPath)
      // remove old dir
      if (localFileSystem.exists(localPath)) {
        localFileSystem.delete(localPath, true)
      }
      localPath
    } else {
      val qualifiedPath = FileUtils.makeQualified(targetPath, hiveContext.hiveconf)
      val dfs = qualifiedPath.getFileSystem(jobConf)
      if (dfs.exists(qualifiedPath)) {
        dfs.delete(qualifiedPath, true)
      } else {
        dfs.mkdirs(qualifiedPath.getParent)
      }
      qualifiedPath
    }

    val fileSinkConf = new FileSinkDesc(writeToPath.toString, desc, false)
    val isCompressed = hiveContext.hiveconf.getBoolean(
      ConfVars.COMPRESSRESULT.varname, ConfVars.COMPRESSRESULT.defaultBoolVal)

    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      hiveContext.hiveconf.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hiveContext.hiveconf.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(hiveContext.hiveconf.get("mapred.output.compression.type"))
    }

    val writerContainer =
      new SparkHiveWriterContainer(jobConf, fileSinkConf, desc.getTableName, Array[String]())

    saveAsHiveFile(
      hiveContext.sparkContext,
      child.execute(),
      StructType.fromAttributes(output),
      child.output.map(_.dataType).toArray,
      outputClass,
      fileSinkConf,
      jobConfSer,
      writerContainer)

    Seq.empty[Row]
  }

}