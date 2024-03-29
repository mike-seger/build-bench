# build-bench

A few bash scripts performing a real-world JVM benchmark. 
The benchmark measure build times of common maven and gradle projects from github.

## Prerequisites
- an installed java jdk version 11

### Installing prerequisites
If installing sdkman is an option for you, here's how:
- https://gist.github.com/mike-seger/330a9ce984027253f1e9840da6b68351#install-java-and-maven

## Running
The benchmark can be run by issuing the following command:
```
./run-bench.sh
```

The benchmark creates a *reports* directory containing the results.

## Compare results
Using this project: https://github.com/mike-seger/bb-reports  
any results can be compared by simply copying the *reports* directory over to the above project and following the instructions.


# RAM Disk

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
mount -t tmpfs -o rw,size=25% tmpfs /mnt/ramdisk
```

## Windows

There exist several tools to create a ramdisk. If using *Hiren's Boot CD* the X drive is
already a ramdisk.

# Run using docker
```
openssl s_client -showcerts -connect repo1.maven.org:443 < /dev/null > certs.txt
openssl x509 -in certs.txt -out certs.der -outform DER
keytool -importcert -file certs.der -keystore ./cacerts

docker run --rm -it -v $(pwd):/home/docker openjdk:11-jdk bash
```
