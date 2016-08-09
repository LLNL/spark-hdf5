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

import java.io.File
import java.net.URI

import gov.llnl.spark.hdf.Reader.{BoundedScan, ScanItem, UnboundedScan}
import gov.llnl.spark.hdf.reader.{DatasetReader, HDF5Reader}
import gov.llnl.spark.hdf.reader.HDF5Schema.Dataset
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.language.existentials

class HDF5Relation( val paths: Array[String]
                  , val dataset: String
                  , val fileExtension: Array[String]
                  , val chunkSize: Int)
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  val fileSystem = FileSystem.get(hadoopConfiguration)

  val files: Array[URI] = {
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

  override def schema: StructType = SchemaConverter.convertSchema(hdf5Schema)

  private lazy val hdf5Schema: Dataset[_] = files match {
    case Array(head, _*) =>
      new Reader(head)
      .openReader(_.getObject(dataset)) match {
        case Some(x: Dataset[_]) => x
        case _ => throw new java.io.FileNotFoundException("Not a dataset")
      }
    case Array() =>
      throw new java.io.FileNotFoundException("No files")
  }

  override def buildScan(): RDD[Row] = {
    val scans = files.map( x => (x, chunkSize))
    val datasetRDD = sqlContext.sparkContext.parallelize(scans.map{
      case (file, size) => (file
                          , size
                          , new Reader(file).openReader(reader => reader.getObject(dataset)))
    })
    val scanRDD = datasetRDD.map{
      case (file, size, Some(ds: Dataset[_])) => UnboundedScan(file, ds, size)
      case x => x
    }
    val splitScanRDD = scanRDD.flatMap{
      case UnboundedScan(file, ds, size) if ds.size > size =>
        (0L until Math.ceil(ds.size.toFloat / size).toLong).map(x => BoundedScan(file, ds, size, x))
      case x: UnboundedScan => Seq(x)
    }
    splitScanRDD.flatMap{ item =>
      new Reader(item.path).execQuery(item)
    }
  }

}

object Reader {
  sealed trait ScanItem {
    val path: URI
    val dataset: Dataset[_]
    val chunkSize: Int
  }
  case class UnboundedScan(path: URI, dataset: Dataset[_], chunkSize: Int) extends ScanItem
  case class BoundedScan(path: URI
                         , dataset: Dataset[_]
                         , chunkSize: Int
                         , chunkNumber: Long = 0) extends ScanItem
}

class Reader(filePath: URI) extends Serializable {

  def execQuery[T](scanItem: ScanItem): Seq[Row] = scanItem match {
    case UnboundedScan(path, dataset, _) =>
      val dataReader = newDatasetReader(dataset)(_.readDataset())
      dataReader.zipWithIndex.map { case (x, index) => Row(index.toLong, x) }
    case BoundedScan(path, dataset, size, number) =>
      val dataReader = newDatasetReader(dataset)(_.readDataset(size, number))
      dataReader.zipWithIndex.map { case (x, index) => Row((size * number) + index.toLong, x) }
  }

  def openReader[T](fun: HDF5Reader => T): T = {
    val file = new File(filePath.toString)
    val reader = new HDF5Reader(file)
    val result = fun(reader)
    reader.close()
    result
  }

  def newDatasetReader[S, T](node: Dataset[T])(fun: DatasetReader[T] => S): S = {
    openReader(reader => reader.getDataset(node)(fun))
  }

}
