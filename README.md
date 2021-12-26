# build-bench

A few bash scripts performing a real-world JVM benchmark. The benchmark utilizes a set of snapshots from major maven and gradle projects on github.

The benchmark can be run with caches enabled or disabled.

Multiple iterations of the benchmark can be run in a pre-defined order calling:
```
./multirun.sh
```

The projects included in the benchmark are defined at the end of the main benchmark script: ```run.sh```

The logs from the benchmark are recorded in the directory: *reports*

Calling ```./stats.sh```  
extracts the timitng statistics from all log files in *reports*.

# ramdisk
## OSX
```
diskutil erasevolume HFS+ 'ramdisk' $(hdiutil attach -nobrowse -nomount ram://4194304)
```

## Linux
```
tmpfs  /mnt/ramdisk  tmpfs  rw,size=25%  0   0
```
