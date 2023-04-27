# s3, a fast command line client for AWS S3

## Tour

You can:
* `s3 cat` lets you view files on S3 without downloading them, using the [bat pager](https://github.com/sharkdp/bat) which supports syntax highlighting. Adding the `-p/--pretty-print` switch will pretty print JSON files
* `s3 ls` list files; `-r/--recursive` will generate list contents recursively and `-h/--human` will print sizes in human readable format (e.g., 234 Mb)
* `s3 rm paths...` will delete files from S3
* `s3 touch path` will create an empty file on S3
* `s3 cp source destination` will copy a file; source and destination can be local paths or S3 paths


## Installation

If you don't have rust installed, you can install it with [rustup](https://rustup.rs/).

Once rust is installed, you can run
```
cargo install --locked --git https://github.com/msalib/s3
```

to install s3 and then run
```
s3 --help
```
to see a lit of commands.
