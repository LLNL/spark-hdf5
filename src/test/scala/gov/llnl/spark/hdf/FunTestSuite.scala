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

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/*
 * Base abstract class for all unit tests in Spark for handling common functionality.
 */
abstract class FunTestSuite extends FunSuite with BeforeAndAfterAll {

  private val sparkConf = new SparkConf()

  protected var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "HDF5Suite", sparkConf))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  def checkEqual(df: DataFrame, expected: Seq[Row]): Unit = {
    assert(df.collect.toSet === expected.toSet)
  }

}
