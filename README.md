romba
=====

### Installation on Ubuntu

#### Install Go

* Go to [Golang](https://code.google.com/p/go/downloads/list), download the latest 64bit version tarball and then in terminal go to the Downloads folder, and execute:

```
sudo tar -C /usr/local -xzf go1.2.linux-amd64.tar.gz
```

* Add the following lines to your .profile or .bashrc file (gedit ~/.bashrc):

```
export GOPATH=$HOME/go
export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH
```

Re-source these in your terminal session afterwards (easiest way is to close and open a new terminal).

* Create the following directories:

```
mkdir ~/go
cd ~/go
mkdir src
mkdir bin
mkdir pkg
```

* Test your Go installation

```
go version
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

* Install [leveldb](https://code.google.com/p/leveldb/downloads/list),download latest and unpack in Downloads folder, then do in the unpacked directory:

```
make
sudo cp --preserve=links libleveldb.* /usr/local/lib
sudo cp -R include/leveldb /usr/local/include
sudo ldconfig
```

* Install ROMba:

```
go get github.com/uwedeportivo/romba/cmds/rombaserver
go get github.com/uwedeportivo/romba/cmds/romba
```

* Set up romba directory:

```
mkdir ~/romba
cd ~/romba
cp -r ~/go/src/github.com/uwedeportivo/romba/cmds/rombaserver/web .
cp ~/go/src/github.com/uwedeportivo/romba/cmds/rombaserver/romba.ini .
mkdir db
mkdir logs
mkdir dats
mkdir tmp
mkdir depot
```

Copy dat files into __~/romba/dats__.

* Starting ROMba server:

```
cd ~/romba
rombaserver
```

Visit [ROMba web shell](http://localhost:4200/romba.html)

