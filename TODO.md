# TODO

* [ ] Produce .index files during merge and use them to Open DB.
* [ ] Don't let event handling impact DB operation.
* [ ] Emit better, more structrued messages and emit more of them (e.g. when segment rotation starts and ends).
* [ ] Allow DB to be opened in read-only mode.
* [ ] Allow cancelation of log compaction? Replace Close() with Shudown(context.Context)?
* [ ] Implement `CompactOnOpen` config options, or some means of running a faster, but more memory intensive, log compaction?
* [ ] What to do if a file Sync() errs?
* [ ] Cleanup/improve errors (messages, data, which to export, etc.). Create an InvalidConfigError type?
* [ ] Fill out the README and documentation.
* [ ] Much more extensive testing.
* [ ] Much more extensive benchmarking and profiling.
* [ ] Profile to determine if usage of bufio.Reader is necessary or if io.CopyBuffer (4096 buf?) would be better than io.Copy when writing merged segment files.
* [ ] Provide a CLI which wraps this library.
* [ ] Provide a server exposing this library over the Redis serialization protocol (RESP) (as a separate package in separate repo if 3rd party package is used). Call it `bitcaskresp` and use package `github.com/tidwall/redcon`.
* [ ] Rename "got" to "have" in tests
* [ ] Run errcheck and staticcheck on the codebase
