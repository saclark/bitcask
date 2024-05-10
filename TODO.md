# TODO

* [ ] Produce .index files during merge use them to Open DB.
* [ ] Allow DB to be opened in read-only mode.
* [ ] Consider performing merge in a way that blocks less (e.g. don't iterate kvIndex, parse keys from data files).
* [ ] switchover() before returning ErrPartialWrite, but how to handle if switchover() fails also?
* [ ] Cleanup/improve errors (messages, data, which to export, etc.).
    * [ ] Create an InvalidConfigError type?
* [ ] What to do if a file Sync() errs?
* [ ] What to do with invalid or truncated records encountered during Open?
* [ ] Validate crc32 checksum on Get() (write crc32 at end of record).
* [ ] Fill out the README and documentation.
* [ ] Much more extensive testing.
* [ ] Much more extensive benchmarking and profiling.
* [ ] Add expiry feature. Replace Timestamp with Expiry?
* [ ] crc the whole record? Not just value?
* [ ] Profile to determine if usage of bufio.Reader is necessary or if io.CopyBuffer (4096 buf?) would be better than io.Copy when writing merged data files.
* [ ] Provide a CLI which wraps this library.
* [ ] Provide a server exposing this library over the Redis serialization protocol (RESP) (as a separate package in separate repo if 3rd party package is used). Call it `bitcaskresp` and use package `github.com/tidwall/redcon`.