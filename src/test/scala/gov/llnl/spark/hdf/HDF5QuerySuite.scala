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

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class HDF5QuerySuite extends FunTestSuite {

  val h5file = getClass.getResource("test1.h5").toString
  val h5dir = FilenameUtils.getFullPathNoEndSeparator(getClass.getResource("test1.h5").getPath)

  val multiDataset = "/multi"
  val int8test = "/datatypes/int8"
  val int16test = "/datatypes/int16"
  val int32test = "/datatypes/int32"
  val int64test = "/datatypes/int64"
  val uint8test = "/datatypes/uint8"
  val uint16test = "/datatypes/uint16"
  val uint32test = "/datatypes/uint32"
  val float32test = "/datatypes/float32"
  val float64test = "/datatypes/float64"

  test("Reading multiple files") {
    val df = sqlContext.read.hdf5(h5dir, multiDataset)
    val expected = (0 until 30).map{ x => Row(x % 10, x) }

    checkEqual(df, expected)
  }

  test("Read files in chunks") {
    val evenchunkeddf = sqlContext.read
          .option("chunk size", 5.toString)
          .hdf5(h5file, int8test)

    val oddchunkeddf = sqlContext.read
          .option("chunk size", 3.toString)
          .hdf5(h5file, int8test)

    val expected = Row(0L, Byte.MinValue) +:
      (1L until 9L).map { x => Row(x, (x - 2).toByte) } :+
      Row(9L, Byte.MaxValue)

    checkEqual(evenchunkeddf, expected)
    checkEqual(oddchunkeddf, expected)
  }

  // Signed integer tests

  test("Reading int8") {
    val df = sqlContext.read.hdf5(h5file, int8test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", ByteType)))
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Byte.MinValue) +:
        (1L until 9L).map { x => Row(x, (x - 2).toByte) } :+
        Row(9L, Byte.MaxValue)

    checkEqual(df, expected)
  }

  test("Reading int16") {
    val df = sqlContext.read.hdf5(h5file, int16test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", ShortType)))
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Short.MinValue) +:
        (1L until 9L).map { x => Row(x, (x - 2).toShort) } :+
        Row(9L, Short.MaxValue)

    checkEqual(df, expected)
  }

  test("Reading int32") {
    val df = sqlContext.read.hdf5(h5file, int32test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", IntegerType)))
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Int.MinValue) +:
        (1L until 9L).map { x => Row(x, (x - 2).toInt) } :+
        Row(9L, Int.MaxValue)

    checkEqual(df, expected)
  }

  test("Reading int64") {
    val df = sqlContext.read.hdf5(h5file, int64test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", LongType)))
    assert(df.schema === expectedSchema)

    val expected = Row(0L, Long.MinValue) +:
        (1L until 9L).map { x => Row(x, x - 2) } :+
        Row(9L, Long.MaxValue)

    checkEqual(df, expected)
  }

  // Unsigned integer tests

  test("Reading uint8") {
    val df = sqlContext.read.hdf5(h5file, uint8test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", ShortType)))
    assert(df.schema === expectedSchema)

    val expected = (0L until 9L).map { x => Row(x, x.toShort) } :+ Row(9L, 255)

    checkEqual(df, expected)
  }

  test("Reading uint16") {
    val df = sqlContext.read.hdf5(h5file, uint16test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", IntegerType)))
    assert(df.schema === expectedSchema)

    val expected = (0L until 9L).map { x => Row(x, x.toInt) } :+ Row(9L, 65535)

    checkEqual(df, expected)
  }

  test("Reading uint32") {
    val df = sqlContext.read.hdf5(h5file, uint32test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", LongType)))
    assert(df.schema === expectedSchema)

    val expected = (0L until 9L).map { x => Row(x, x) } :+ Row(9L, 4294967295L)

    checkEqual(df, expected)
  }

  // Float tests

  test("Reading float32") {
    val df = sqlContext.read.hdf5(h5file, float32test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", FloatType)))
    assert(df.schema === expectedSchema)

    val expected = (0 until 10).map(x => x % 2 match {
      case 0 => Row(x, (0.2 * x).toFloat)
      case 1 => Row(x, (-0.2 * x).toFloat)
    })

    checkEqual(df, expected)
  }

  test("Reading float64") {
    val df = sqlContext.read.hdf5(h5file, float64test)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", DoubleType)))
    assert(df.schema === expectedSchema)

    val expected = (0 until 10).map(x => x % 2 match {
      case 0 => Row(x, (2 * x).toDouble / 10)
      case 1 => Row(x, (-2 * x).toDouble / 10)
    })

    checkEqual(df, expected)
  }

  test("Reading fixed length strings") {
    val dataset = "/datatypes/string"
    val alpha = "abcdefghijklmnopqrstuvwxyz"
    val df = sqlContext.read.hdf5(h5file, dataset)

    val expectedSchema = StructType(Seq(
      StructField("index0", LongType, nullable = false),
      StructField("value", StringType)))
    assert(df.schema === expectedSchema)

    val expected = (0 until 10).map{x => Row(x, alpha.substring(0, 0 + x))}

    checkEqual(df, expected)
  }
}
