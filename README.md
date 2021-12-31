# build-bench

A few bash scripts performing a real-world JVM benchmark. The benchmark utilizes major maven and gradle projects from github.

## Prerequisites
- an installed java jdk 11 version
- an installed maven 3.8.x version

The benchmark can be run by issuing the following command:
```
./run-bench.sh
```

The benchmark creates a *reports* directory containing the results.

# ramdisk

A RAM disk can help git operations and downloads on slow disks, such as USB drives.
The benchmark results are not affected after the third (of 6) iteration of the benchmark.

## OSX
```
diskutil erasevolume HFS+ 'ramdisk' $(hdiutil attach -nobrowse -nomount ram://4194304)
```

## Linux

fstab:
```
tmpfs  /mnt/ramdisk  tmpfs  rw,size=25%  0   0
```

or:
```
mkdir -p /mnt/ramdisk
mount -t tmpfs -o rw,size=25%
```

## Windows

There exist several tools to create a ramdisk. If using *Hiren's Boot CD* the X drive is a ramdisk.
