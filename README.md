# build-bench

A JVM benchmark utilizing a selection of major maven and gradle projects hosted on github. 

Each project can be built with caches enabled or disabled.

Multiple iterations of the benchmark can be run in a pre-defined order calling:
```
multirun.sh
```

The projects included in the benchmark is defined at the end of: ```run.sh```

The benchmark logs are recored in the directory: *reports*

The script: ```stats.sh```  
extracts the timitng statistics from all log files in *reports*.


