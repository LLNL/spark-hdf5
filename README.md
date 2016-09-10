# Spark-HDF5 [![Build Status](https://travis-ci.org/LLNL/spark-hdf5.svg?branch=master)](https://travis-ci.org/LLNL/spark-hdf5)

## Progress
The plugin can read single-dimensional arrays from HDF5 files.

The following types are supported:
  * Int8
  * UInt8
  * Int16
  * UInt16
  * Int32
  * Int64
  * Float32
  * Float64
  * Fixed length strings

## Setup
If you are using the sbt-spark-package, the easiest way to use the package is by requiring it from the [spark packages website](https://spark-packages.org/package/LLNL/spark-hdf5):
```
spDependencies += "LLNL/spark-hdf5:0.0.4"
```
Otherwise, download the latest release jar and include it on your classpath. 

## Usage
```scala
import gov.llnl.spark.hdf._

val df = sqlContext.read.hdf5("path/to/file.h5", "/dataset")
df.show
```

You can start a spark repl with the console target:
```
sbt console
```
This will fetch all of the dependencies, set up a local Spark instance, and start a Spark repl with the plugin loaded.

## Options
The following options can be set:

Key          | Default | Description
-------------|---------|------------
`extension`  | `h5`    | The file extension of data
`chunk size` | `10000` | The maximum number of elements to be read in a single scan

## Testing
The plugin includes a test suite which can be run through SBT
```
sbt test
```

## Roadmap
  * Use the [hdf-obj package](https://www.hdfgroup.org/products/java/hdf-object/) rather than the [sis-jhdf5 wrapper](https://wiki-bsse.ethz.ch/pages/viewpage.action?pageId=26609113)
  * Support for multi-dimensional arrays
  * Support for compound datasets
  * Additional testing
  * [Partition discovery][2] (data inference based on location)

[1]: https://github.com/paulp/sbt-extras
[2]: http://spark.apache.org/docs/latest/sql-programming-guide.html#partition-discovery

## Release
This code was developed at the Lawrence Livermore National Lab (LLNL) and is available under the [Apache 2.0 license](LICENSE) (`LLNL-CODE-699384`)
