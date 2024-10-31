# TODO

* [ ] Prevent or add coordinatation/cancelation when attempting to close the DB during a log compaction.
* [ ] Produce .index files during merge and use them to Open DB.
* [ ] Allow DB to be opened in read-only mode.
* [ ] What to do if a file Sync() errs?
* [ ] Cleanup/improve errors (messages, data, which to export, etc.). Create an InvalidConfigError type?
* [ ] Fill out the README and documentation.
* [ ] Much more extensive testing.
* [ ] Much more extensive benchmarking and profiling.
* [ ] Emit better, more structrued messages and emit more of them (e.g. when segment rotation starts and ends).
* [ ] Profile to determine if usage of bufio.Reader is necessary or if io.CopyBuffer (4096 buf?) would be better than io.Copy when writing merged segment files.
* [ ] Provide a CLI which wraps this library.
* [ ] Provide a server exposing this library over the Redis serialization protocol (RESP) (as a separate package in separate repo if 3rd party package is used). Call it `bitcaskresp` and use package `github.com/tidwall/redcon`.
