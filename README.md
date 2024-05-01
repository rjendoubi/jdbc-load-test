# jdbc-load-test

## Purpose

For measuring time taken for loading progressive orders of magnitude of
data via JDBC.

Primarily intended for Impala, but could be extended to accommodate
different databases, for side-by-side comparisons.

## Usage

Look at the `.env.example` file. Save as `.env` and fill in values for
your environment.

## Contribution

Patches welcome. There are `TODO` comments in the codebase, in addition
to the [`TODO.md`](TODO.md) file itself.

## References

* [How to use JDBC source to write and read data in PySpark (StackOverflow)](
    https://stackoverflow.com/questions/30983982/how-to-use-jdbc-source-to-write-and-read-data-in-pyspark)
* [Efficient way to do back inserts with JDBC (StackOverflow)](https://stackoverflow.com/questions/3784197/efficient-way-to-do-batch-inserts-with-jdbc)
