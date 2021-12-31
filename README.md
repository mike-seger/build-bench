# build-bench

## Prerequisites
- an installed java 11 version
- an installed maven 3.8.x version

A few bash scripts performing a real-world JVM benchmark. The benchmark utilizes major maven and gradle projects from github.

The benchmark can be run by issuing the following command:
```
./run-bench.sh
```

The benchmark creates a **reports** directory containing the results.

# ramdisk
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
