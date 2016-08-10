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

import ch.systemsx.cisd.hdf5.IHDF5Reader
import gov.llnl.spark.hdf.reader.HDF5Schema.Dataset

class DatasetReader[T](val reader: IHDF5Reader, val node: Dataset[T]) extends Serializable {

  def readDataset(): Array[T] =
    node.contains.readArray(reader, node.path)

  def readDataset(blockSize: Int, blockNumber: Long): Array[T] =
    node.contains.readArrayBlock(reader, node.path, blockSize, blockNumber)

}
