romba
=====

### Installation on Linux/Mac OS X

* Install [Go](http://golang.org/doc/install):

Edit your _~/.profile_ file adding the following lines:

```
export GOPATH=$HOME/go
export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH
```

Reload your profile file:

```
. ~/.profile
```

* Install Mercurial:

```
sudo apt-get install mercurial
```

* Install [zlib](http://www.zlib.net/), download latest and unpack in Downloads folder, then do in the unpacked directory:

```
./configure
make
sudo make install
```

* Install Git and G++:

```
sudo apt-get install git
sudo apt-get install g++
```

* Install leveldb:

```
sudo apt-get install libleveldb-dev
```

* Install ROMba:

```
go install github.com/uwedeportivo/romba/cmds/rombaserver@latest
go install github.com/uwedeportivo/romba/cmds/romba@latest
```

* Set up romba directory:

```
mkdir ~/romba
cd ~/romba
cp -r ~/go/pkg/mod/github.com/uwedeportivo/romba@-<LATEST>/cmds/rombaserver/web .
cp ~/go/pkg/mod/github.com/uwedeportivo/romba@-<LATEST>/cmds/rombaserver/romba.ini .
mkdir db logs dats tmp depot
```

Copy dat files into __~/romba/dats__.

* Starting ROMba server:

```
cd ~/romba
rombaserver
```

You should see something like this in the terminal:
```
I0125 19:46:24.488784 22463 db.go:120] Loading DB
I0125 19:46:24.496356 22463 kv.go:117] Loading Generation File
I0125 19:46:24.496401 22463 kv.go:124] Loading Dats DB
I0125 19:46:25.000354 22463 kv.go:131] Loading CRC DB
I0125 19:46:25.542857 22463 kv.go:138] Loading MD5 DB
I0125 19:46:26.891565 22463 kv.go:145] Loading SHA1 DB
I0125 19:46:28.447981 22463 kv.go:152] Loading CRC -> SHA1 DB
I0125 19:46:28.452836 22463 kv.go:159] Loading MD5 -> SHA1 DB
I0125 19:46:30.384619 22463 db.go:127] Done Loading DB in 5s
I0125 19:46:30.385023 22463 depot.go:112] Initializing Depot with the following roots
I0125 19:46:30.385067 22463 depot.go:116] root = /Users/uwe/romba/depot, maxSize = 537GB, size = 0B
starting romba server at localhost:4200/romba.html
```

Visit [ROMba web shell](http://localhost:4200/romba.html)

![romba web shell](https://github.com/uwedeportivo/romba/raw/master/docs/rombaweb.png "romba web")
