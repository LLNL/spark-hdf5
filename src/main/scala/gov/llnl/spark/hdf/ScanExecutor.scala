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

import gov.llnl.spark.hdf.ScanExecutor.{BoundedScan, ScanItem, UnboundedScan}
import gov.llnl.spark.hdf.reader.{DatasetReader, HDF5Reader}
import gov.llnl.spark.hdf.reader.HDF5Schema.Dataset
import org.apache.spark.sql.Row

import scala.language.existentials

object ScanExecutor {
  sealed trait ScanItem {
    val dataset: Dataset[_]
    val chunkSize: Int
  }
  case class UnboundedScan(dataset: Dataset[_], chunkSize: Int) extends ScanItem
  case class BoundedScan(dataset: Dataset[_]
                       , chunkSize: Int
                       , chunkNumber: Long = 0) extends ScanItem
}

class ScanExecutor(filePath: String) extends Serializable {

  def execQuery[T](scanItem: ScanItem): Seq[Row] = scanItem match {
    case UnboundedScan(dataset, _) =>
      val dataReader = newDatasetReader(dataset)(_.readDataset())
      dataReader.zipWithIndex.map { case (x, index) => Row(index.toLong, x) }
    case BoundedScan(dataset, size, number) =>
      val dataReader = newDatasetReader(dataset)(_.readDataset(size, number))
      dataReader.zipWithIndex.map { case (x, index) => Row((size * number) + index.toLong, x) }
  }

  def openReader[T](fun: HDF5Reader => T): T = {
    val file = new File(filePath)
    val reader = new HDF5Reader(file)
    val result = fun(reader)
    reader.close()
    result
  }

  def newDatasetReader[S, T](node: Dataset[T])(fun: DatasetReader[T] => S): S = {
    openReader(reader => reader.getDataset(node)(fun))
  }

}
