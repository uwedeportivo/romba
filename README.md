# ROMba

[![Build Status](https://travis-ci.com/uwedeportivo/romba.svg?branch=master)](https://travis-ci.com/uwedeportivo/romba)

ROMba is a command-line tool (and WEB shell) for the management, and collection of ROMs and DAT files within the Linux and Mac OS X environments.
While its core functionality is similar to tools like [ROMVault](http://www.romvault.com/) and [CLRMamePRO](http://mamedev.emulab.it/clrmamepro/), ROMba takes a unique approach by storing ROMs in a de-duplicated way, and allowing you to "build" any set you need on demand.

## Installation

* [Linux Mac](INSTALLATION.md)

## Docker

```
docker run --rm --publish 4204:4204 --volume /xxx_PATH_TO_ROMBA_DIR_xxx:/var/romba uwedeportivo/romba:v1xx
```

## [Usage](USAGE.md)

## License

ROMba is under the [Go license](http://golang.org/LICENSE).
