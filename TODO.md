# TODO

* [ ] Produce .index files during merge and use them to Open DB.
* [ ] Support batching writes and/or add configuration options to automatically Sync() after every n seconds and/or n records written.
* [ ] Shutdown DB entirely and return error if a file Sync() errs.
* [ ] Allow cancelation of log compaction. Replace Close() with Shudown(context.Context).
* [ ] Implement a "read committed" isolation level model where reads don't block writes?
* [ ] Implement `CompactOnOpen` config options, or some means of running a faster, but more memory intensive, log compaction?
* [ ] Emit better, more structrued event messages and emit more of them (e.g. when segment rotation starts and ends), while also not letting handling of the events impact DB operation.
* [ ] Allow DB to be opened in read-only mode.
* [ ] Cleanup/improve errors (messages, data, which to export, etc.). Create an InvalidConfigError type?
* [ ] Fill out the README and documentation. Document fault models like TigerBeetle does.
* [ ] Much more extensive testing.
* [ ] Much more extensive benchmarking and profiling.
* [ ] Profile to determine if usage of bufio.Reader is necessary or if io.CopyBuffer (4096 buf?) would be better than io.Copy when writing merged segment files.
* [ ] Provide a CLI which wraps this library.
* [ ] Provide a server exposing this library over the Redis serialization protocol (RESP) (as a separate package in separate repo if 3rd party package is used). Call it `bitcaskresp` and use package `github.com/tidwall/redcon`.
* [ ] Rename "got" to "have" in tests
* [ ] Run errcheck and staticcheck on the codebase
