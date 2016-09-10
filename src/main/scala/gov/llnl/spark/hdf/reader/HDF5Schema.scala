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

import ch.systemsx.cisd.hdf5._

object HDF5Schema {

  // TODO: Needs reference, time, unsigned, compound, enumeration
  //          case COMPOUND
  sealed trait HDF5Type[T] {
    def readArray(reader: IHDF5Reader): Array[T]
    def readArrayBlock(reader: IHDF5Reader
                       , blockSize: Int
                       , blockNumber: Long): Array[T]
  }

  case class Int8(file: String, name: String) extends HDF5Type[Byte] {
    override def readArray(reader: IHDF5Reader): Array[Byte] =
      reader.int8.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Byte] =
      reader.int8.readArrayBlock(name, blockSize, blockNumber)
  }

  case class UInt8(file: String, name: String) extends HDF5Type[Short] {
    override def readArray(reader: IHDF5Reader): Array[Short] =
      reader.uint8.readArray(name).map(UnsignedIntUtils.toUint8)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Short] =
      reader.uint8.readArrayBlock(name, blockSize, blockNumber).map(UnsignedIntUtils.toUint8)
  }

  case class Int16(file: String, name: String) extends HDF5Type[Short] {
    override def readArray(reader: IHDF5Reader): Array[Short] =
      reader.int16.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Short] =
      reader.int16.readArrayBlock(name, blockSize, blockNumber)
  }

  case class UInt16(file: String, name: String) extends HDF5Type[Int] {
    override def readArray(reader: IHDF5Reader): Array[Int] =
      reader.uint16.readArray(name).map(UnsignedIntUtils.toUint16)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Int] =
      reader.uint16.readArrayBlock(name, blockSize, blockNumber).map(UnsignedIntUtils.toUint16)
  }

  case class Int32(file: String, name: String) extends HDF5Type[Int] {
    override def readArray(reader: IHDF5Reader): Array[Int] =
      reader.int32.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Int] =
      reader.int32.readArrayBlock(name, blockSize, blockNumber)
  }

  case class UInt32(file: String, name: String) extends HDF5Type[Long] {
    override def readArray(reader: IHDF5Reader): Array[Long] =
      reader.uint32.readArray(name).map(UnsignedIntUtils.toUint32)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Long] =
      reader.uint32.readArrayBlock(name, blockSize, blockNumber).map(UnsignedIntUtils.toUint32)
  }

  case class Int64(file: String, name: String) extends HDF5Type[Long] {
    override def readArray(reader: IHDF5Reader): Array[Long] =
      reader.int64.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Long] =
      reader.int64.readArrayBlock(name, blockSize, blockNumber)
  }

  case class Float32(file: String, name: String) extends HDF5Type[Float] {
    override def readArray(reader: IHDF5Reader): Array[Float] =
      reader.float32.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Float] =
      reader.float32.readArrayBlock(name, blockSize, blockNumber)
  }

  case class Float64(file: String, name: String) extends HDF5Type[Double] {
    override def readArray(reader: IHDF5Reader): Array[Double] =
      reader.float64.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[Double] =
      reader.float64.readArrayBlock(name, blockSize, blockNumber)
  }

  case class FLString(file: String, name: String) extends HDF5Type[String] {
    override def readArray(reader: IHDF5Reader): Array[String] =
      reader.string.readArray(name)

    override def readArrayBlock(reader: IHDF5Reader
                                , blockSize: Int
                                , blockNumber: Long): Array[String] =
      reader.string.readArrayBlock(name, blockSize, blockNumber)
  }

  sealed trait HDF5Node {
    val file: String
    val path: String

    def flatten(): Seq[HDF5Node]
  }

  case class Dataset[T](  file: String
                        , path: String
                        , contains: HDF5Type[T]
                        , dimension: Array[Long]
                        , size: Long) extends HDF5Node with Serializable {
    def flatten(): Seq[HDF5Node] = Seq(this)
  }

  case class Group(file: String, path: String, children: Seq[HDF5Node]) extends HDF5Node {
    def flatten(): Seq[HDF5Node] = this +: children.flatMap(x => x.flatten())
  }

  case class GenericNode(file: String, path: String) extends HDF5Node {
    def flatten(): Seq[HDF5Node] = Seq(this)
  }

}
