# Mount a Bolt database as a FUSE filesystem

[Bolt](https://github.com/boltdb/bolt) is a key-value store that also supports nested buckets. This makes it look a
little bit like a file system tree.

`boltmnt` exposes a Bolt database as a FUSE file system.

mount:

```sh
$ mkdir -p mnt/bucket/sub
$ boltmnt test.bolt mnt &
$ echo Hello, world > mnt/bucket/sub/greeting
$ mkdir -p mnt/default/
$ echo bingoo > mnt/default/name
$ cat  mnt/default/name
$ cat mnt/bucket/sub/greeting
Hello, world
$ ls -lhR mnt
total 0
drwxr-xr-x  1 root  wheel     0B Jul 26 07:07 bucket
drwxr-xr-x  1 root  wheel     0B Jul 26 07:07 default

mnt//bucket:
total 0
drwxr-xr-x  1 root  wheel     0B Jul 26 07:07 sub

mnt//bucket/sub:
total 8
-rw-r--r--  1 root  wheel    13B Jul 26 07:07 greeting

mnt//default:
total 8
-rw-r--r--  1 root  wheel     7B Jul 26 07:07 name
```

unmount:

- Linux
    ```sh
    $ fusermount -u mnt
    ```
- OS X
    ```sh
    $ umount mnt
    [1]  + 1253 done       boltmnt test.bolt mnt
    ```

## Encoding keys to file names

As Bolt keys can contain arbitrary bytes, but file names cannot, the keys are encoded.

First, we define *safe* as:

- ASCII letters and numbers
- the characters ".", "," "-", "_" (period/dot, comma, dash, underscore)

A name consisting completely of *safe* characters, and not starting with a dot, is unaltered. Everything else is
hex-encoded. Hex encoding looks like `@xx[xx..]` where `xx` are lower case hex digits.

Additionally, any *safe* prefixes (not starting with a dot) and suffixes longer than than a noise threshold remain
unaltered. They are separated from the hex encoded middle part by a semicolon, as in
`[PREFIX:]MIDDLE[:SUFFIX]`.

For example:

A Bolt key packing two little-endian `uint16` values 42 and 10000 and the string
"test" is encoded as filename `@002a2710:test`.
