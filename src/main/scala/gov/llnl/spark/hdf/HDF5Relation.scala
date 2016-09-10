/*
 * Copyright (c) 2016, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory
 *
 * Written by Joshua Asplund <asplund1@llnl.gov>
 * LLNL-CODE-699384
 *
 * All rights reserved.
 *
 * This file is part of spark-hdf5.
 * For details, see https://github.com/LLNL/spark-hdf5
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.llnl.spark.hdf

import java.net.URI

import gov.llnl.spark.hdf.ScanExecutor.{BoundedScan, UnboundedScan}
import gov.llnl.spark.hdf.reader.HDF5Schema.{Dataset}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

class HDF5Relation( val paths: Array[String]
                    , val dataset: String
                    , val fileExtension: Array[String]
                    , val chunkSize: Int)
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  val fileSystem = FileSystem.get(hadoopConfiguration)

  lazy val files: Array[URI] = {
    val roots = paths.map{ path =>
      fileSystem.getFileStatus(new Path(path)) }.toSeq

    val leaves = roots.flatMap{
      case status if status.isFile => Set(status)
      case directory if directory.isDirectory =>
        val it = fileSystem.listFiles(directory.getPath, true)
        var children: Set[FileStatus] = Set()
        while (it.hasNext) {
          children += it.next()
        }
        children
    }

    leaves.filter(status => status.isFile)
      .map(_.getPath)
      .filter(path => fileExtension.contains(FilenameUtils.getExtension(path.toString)))
      .map(org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(_).toUri)
      .toArray
  }

  private lazy val datasets: Array[Dataset[_]] = files.flatMap {
    file => new ScanExecutor(file.toString).openReader(_.getObject(dataset))
  }.collect { case y: Dataset[_] => y }

  private lazy val hdf5Schema: Dataset[_] = datasets match {
    case Array(head: Dataset[_], _*) => head
    case _ => throw new java.io.FileNotFoundException("No files")
  }

  override def schema: StructType = SchemaConverter.convertSchema(hdf5Schema)

  override def buildScan(): RDD[Row] = {
    val scans = datasets.map{ UnboundedScan(_, chunkSize) }
    val splitScans = scans.flatMap{
      case UnboundedScan(ds, size) if ds.size > size =>
        (0L until Math.ceil(ds.size.toFloat / size).toLong).map(x => BoundedScan(ds, size, x))
      case x: UnboundedScan => Seq(x)
    }
    sqlContext.sparkContext.parallelize(splitScans).flatMap{ item =>
      new ScanExecutor(item.dataset.file).execQuery(item)
    }
  }

}
