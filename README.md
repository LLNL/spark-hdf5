# Spark-HDF5

[![Build Status](https://travis-ci.org/LLNL/spark-hdf5.svg?branch=master)](https://travis-ci.org/LLNL/spark-hdf5)

## Progress
Currently the plugin can read numeric arrays (signed and unsigned) from HDF5 files.

The following numeric types are supported:
  * Int8
  * UInt8
  * Int16
  * UInt16
  * Int32
  * Int64
  * Float32
  * Float64

## Testing
You can start a spark repl by running the following command:
```
sbt console
```
This will fetch all of the dependencies, including setting up a local Spark instance, and start a Spark repl with the plugin loaded.
The path is specified by separating the file path from the dataset path with a colon.

You can test out the code interactively:
```
import spark.hdf._
val df = sqlContext.read.option("dataset", "dataset")
                        .hdf5("src/test/resources/test.h5")
df.show
```
The plugin also includes a test suite which can be run through SBT
```
sbt test
```

## Roadmap
  * Use the [hdf-obj package](https://www.hdfgroup.org/products/java/hdf-object/) rather than the [sis-jhdf5 wrapper](https://wiki-bsse.ethz.ch/pages/viewpage.action?pageId=26609113)
  * Support for multi-dimensional arrays
  * Support for non-numerical types (String, Boolean, Structs)
  * Support for compound datasets
  * Additional testing
  * [Partition discovery][2] (data inference based on location)

[1]: https://github.com/paulp/sbt-extras
[2]: http://spark.apache.org/docs/latest/sql-programming-guide.html#partition-discovery

## Release
This code was developed at the Lawrence Livermore National Lab (LLNL) and is available under the [Apache 2.0 license](LICENSE) (`LLNL-CODE-699384`)
