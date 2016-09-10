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

import gov.llnl.spark.hdf.reader.HDF5Schema._
import org.apache.spark.sql.types._

object SchemaConverter {

  def convertSchema(dataset: Dataset[_]): StructType = {
    val columns = dataset.dimension.indices.map {
      index => "index" + index
    }.map {
      name => StructField(name, LongType, nullable = false)
    }
    StructType(columns :+ StructField("value", extractTypes(dataset.contains)))
  }

  def extractTypes(datatype: HDF5Type[_]): DataType = datatype match {
    case Int8(_, _) => ByteType
    case UInt8(_, _) => ShortType
    case Int16(_, _) => ShortType
    case UInt16(_, _) => IntegerType
    case Int32(_, _) => IntegerType
    case UInt32(_, _) => LongType
    case Int64(_, _) => LongType
    case Float32(_, _) => FloatType
    case Float64(_, _) => DoubleType
    case FLString(_, _) => StringType
  }

}
