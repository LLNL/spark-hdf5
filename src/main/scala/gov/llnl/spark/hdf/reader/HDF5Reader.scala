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
package gov.llnl.spark.hdf.reader

import java.io.{Closeable, File}

import ch.systemsx.cisd.hdf5.{HDF5DataClass, HDF5DataTypeInformation, HDF5FactoryProvider, IHDF5Reader}
import gov.llnl.spark.hdf.reader.HDF5Schema._

import scala.collection.JavaConverters._

class HDF5Reader(val input: File) extends Closeable with Serializable {
  lazy val path: String = input.getPath

  val reader = HDF5FactoryProvider.get().openForReading(input)

  def getSchema: HDF5Node = listMembers()

  private lazy val objects = getSchema.flatten().map {
    case node@Dataset(_, name, _, _, _) => (name, node)
    case node@Group(_, name, _) => (name, node)
    case node@GenericNode(_, _) => (node.path, node)
  }.toMap

  def getObject(path: String): Option[HDF5Node] = objects.get(path)

  override def close(): Unit = reader.close()

  def isDataset(path: String): Boolean = !reader.isGroup(path)

  def getDataset[S, T](dataset: Dataset[T])(fun: DatasetReader[T] => S): S =
    fun(new DatasetReader[T](reader, dataset))

  def listMembers(name: String = "/"): HDF5Node = {
    reader.isGroup(name) match {
      case true =>
        val children = reader.getGroupMembers(name).asScala
        name match {
          case "/" => Group(path, name, children.map { x => listMembers("/" + x) })
          case _ => Group(path, name, children.map { x => listMembers(name + "/" + x) })
        }
      case false =>
        val info = reader.getDataSetInformation(name)
        val hdfType = infoToType(name, info.getTypeInformation)
        Dataset(path, name, hdfType, info.getDimensions, info.getNumberOfElements)
    }
  }

  def infoToType(name: String, info: HDF5DataTypeInformation): HDF5Type[_] = {
    (info.getDataClass, info.isSigned, info.getElementSize) match {
      case (HDF5DataClass.INTEGER, true, 1) => HDF5Schema.Int8(path, name)
      case (HDF5DataClass.INTEGER, false, 1) => HDF5Schema.UInt8(path, name)
      case (HDF5DataClass.INTEGER, true, 2) => HDF5Schema.Int16(path, name)
      case (HDF5DataClass.INTEGER, false, 2) => HDF5Schema.UInt16(path, name)
      case (HDF5DataClass.INTEGER, true, 4) => HDF5Schema.Int32(path, name)
      case (HDF5DataClass.INTEGER, false, 4) => HDF5Schema.UInt32(path, name)
      case (HDF5DataClass.INTEGER, true, 8) => HDF5Schema.Int64(path, name)
      case (HDF5DataClass.FLOAT, true, 4) => HDF5Schema.Float32(path, name)
      case (HDF5DataClass.FLOAT, true, 8) => HDF5Schema.Float64(path, name)
      case (HDF5DataClass.STRING, signed, size) => HDF5Schema.FLString(path, name)
      case _ => throw new NotImplementedError("Type not supported")
    }
  }

}
