## Model

In ROMba, every single file (.rom, .bin, iso, etc) within a "game" is stored in a repository called a depot, the depot can span multiple drives and paths, and can be limited in size. This allows the user to split their depot over (many) different drives; ROMba will handle the split over the paths that are made available to ROMba automatically.
 
Within the depot, files are stored in a folder hierarchy based on the calculated SHA1 hash value of the file.
 
The folder structure for an archived file is as follows:
 
/user defined depot path/3f/1a/7c/cc/3f1a7ccc0f92fc567203814e00458727f4307fed.gz
 
The first 4 folders are based on the 8 first bytes of the SHA1 value of the file stored - This is done to both limit the amount of files in 1 folder,
as well as allowing for very fast lookup of the file in case of build-like file operations.
 
Files are stored in the gzip format, a good medium between speed and compression. Additionally, the gzip header contains the uncompressed original file SIZE, CRC32, MD5, and SHA1 that was calculated during the initial archiving of the file.
 
Next, DAT files are stored in a folder structure of choice. During build operations, this folder structure is exactly replicated in the location where rebuilt sets will be put.
If DAT folder A/ contains 1 dat in A/B/ and 2 dats in A/B/C/, there will be 1 set in Rebuild folder X/B/ and 2 sets in folder X/B/C/ after a rebuild.
 
Since DAT files effectively are nothing more than just a repository of SHA1 (CRC32, MD5) values and names combined together make up a set, it is possible to store 1000s of DAT files without increasing the need to store more ROMs than the net difference of what is in the depot already. ROMba can deal with DATs that have CRC32, MD5 or SHA1 information contained.
ROMBa does not require individual "size" entries in the DATfile in order to still operate correctly, it will however use the size attribute in cases where a DAT only contains CRC entries to avoid (as far as possible) CRC collisions.
 
All ROMba operations run via separate threads (workers), the number of workers can be manually adjusted to best fit the environment ROMba is operated in.
 
## Commands
 
Full ROMba command syntax
ROMba <*****OBI*****> <*****OBI*****>
 
archive      Adds ROM files from the specified directories to the ROM archive.
build        For each specified DAT file it creates the torrentzip files.
dbstats      Prints db stats.
diffdat      Creates a DAT file with those entries that are in -new DAT.
dir2dat      Creates a DAT file for the specified input directory and saves it to the -out filename.
fixdat       For each specified DAT file it creates a fix DAT.
lookup       For each specified hash it looks up any available information.
memstats     Prints memory stats.
miss         For each specified DAT file it creates a miss file and a have file.
progress     Shows progress of the currently running command.
purge-backup Moves DAT index entries for orphaned DATs.
purge-delete Deletes DAT index entries for orphaned DATs.
refresh-dats Refreshes the DAT index from the files in the DAT master directory tree.
shutdown     Gracefully shuts down server.
 
Use "Romba help <command>" for more information about a command.


## BUILD command
 
help build
Usage: Romba build -out <outputdir> <list of DAT files or folders with DAT files>
 
For each specified DAT file it creates the torrentzip files in the specified
output dir. The files will be placed in the specified location using a folder
structure according to the original DAT master directory tree structure.
 
Options:
  -out="": output dir
 
So for example:
 
build -out newroms '/fullpath/TOSEC/Altos Computer Systems/ACS-8000/Applications/Altos Computer Systems ACS-8000 - Applications (TOSEC-v2010-02-10_CM).dat'
 
 
 
 
build -out 'mnt/roms' 'TOSEC/Altos Computer Systems/ACS-8000/Applications/Altos Computer Systems ACS-8000 - Applications (TOSEC-v2010-02-10_CM).dat'
would generate:
 
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/Altos Diagnostic (19xx)(Altos Computer Systems)(Serial No. ADX-2596)[DMA][DD].zip
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/Altos Diagnostic (19xx)(Altos Computer Systems)(Serial No. ADX-2596)[non DMA][SD].zip
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/CP-M v2.21 (19xx)(Digital Research)(Serial No. 25-1489)[DMA][DD].zip
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/CP-M v2.21 (19xx)(Digital Research)(Serial No. 25-1489)[non DMA][SD].zip
 
(these 4 are in the DAT)
 
 
Now let's say that the last one is MISSING in your depot (you don't have it)
it would build:
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/Altos Diagnostic (19xx)(Altos Computer Systems)(Serial No. ADX-2596)[DMA][DD].zip
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/Altos Diagnostic (19xx)(Altos Computer Systems)(Serial No. ADX-2596)[non DMA][SD].zip
/mnt/roms/TOSEC/Altos Computer Systems/ACS-8000/Applications/CP-M v2.21 (19xx)(Digital Research)(Serial No. 25-1489)[DMA][DD].zip
 
and in:
/mnt/roms/
it would put a file:
fix_Altos Computer Systems ACS-8000 - Applications (TOSEC-v2010-02-10_CM).dat
 
containing:
clrmamepro (
    name "fix_Altos Computer Systems ACS-8000 - Applications"
)
game (
    name "CP-M v2.21 (19xx)(Digital Research)(Serial No. 25-1489)[non DMA][SD]"
    description "CP-M v2.21 (19xx)(Digital Research)(Serial No. 25-1489)[non DMA][SD]"
    rom ( name "CP-M v2.21 (19xx)(Digital Research)(Serial No. 25-1489)[non DMA][SD].td0" size 165124 crc aa086940 md5 4c8b605f9a8bd75dcc9d287ad2979f72 )
) 
 
## Unsupported ROMba functionality
(1) ROMba does not use HEADER files, nor will it ever. Just like with ROMVault sets, e.g. "No-Intro Nintendo Famicom Disk System" will be built
    fine as it will simply match with those ROMs that DO have the headers.
(2) ROMba only supports dir2dat style DAT files for MAME/MESS CHD's - This is as per design.


## ADDITIONS / Appendices
 
What is a DAT file?
A DAT file contains all information required (sizes and hash values of ROMs that belong to a set, for example MAME) to complete a full set as originally intended by the distributor, from any stage of completion and organization.
 
What is TorrentZip?
The idea behind TorrentZip is to use standard values when creating zips to create bit-identical files over multiple systems, for cross-platform uniformity.
As well as allowing consistent hash-checking results, it also minimizes piece loss when downloading/resuming files over the bittorrent protocol, or anything else that is block based.
 
What is a ROM?
ROM stands for (R)ead (O)nly (M)emory.
A ROM by its original definition was a set of programmed hardware chips that were Read, Once, into Memory.
They were unable to be written to, hence the "Read Only".
For the context of ROM collecting, all later media like floppy disk images, CD and DVD images, and flash memory are also considered ROMs.
 
What is a CHD?
CHD stands for (C)ompressed (H)unks of (D)ata.
A CHD file is a compressed image of the hard drive or laserdisc that is used in an arcade cabinet.
Hard drives are used in arcade games because of the huge storage available on them (gigabytes!),
as opposed to hardware ROMs which although much faster, are much smaller in capacity due to cost.
 
What is dir2dat?
It is simply a way to scan a directory of all of its files, hash them, and create a dat of them to distribute.
Loading a dir2dat DAT file will basically ensure that you have an exact mirror (directory structure) of what DAT
you have loaded in, and what fixes you will require to complete the collection.
 
What is unicode?
Unicode is an international standard for storing and displaying symbols, characters, and lexigraphs from all languages
in a strictly defined encoding. This avoids a common issue of software failing to correctly interpret 龍, ®, £, and
other regionally distinguished symbols (a.k.a. "special characters") when they are parsed with a character encoding
incompatible with their intended. As an example, the Western character Ã is seen as ﾃ in Japanese Shift_JIS encoding.
For more information on what unicode is: http://www.unicode.org/standard/WhatIsUnicode.html
 
What is ZIP64 compliance?
Normal .ZIP files are 32bit, and have a hard limit of 4GB (4,294,967,295 bytes) per file.
64 bit ZIP overcomes this limitation, allowing 4GB+ files to be used.
Reference for 32bit vs 64bit ZIP: http://www.artpol-software.com/ziparchive/KB/0610051629.aspx
 
 
 
-----------------
 
***BELOW NEEDS a NEW home still, either in text like above, or DELETE*** se
(5) ROMba can deal with external ZIP hashing - This is useful for sets where the original image had zips that need to preserved.
(6) ROMba outputs TorrentZIP "ZIP64" compliant files (Multi gigabyte files are natively handled)
(7) ROMba handles unicode files
(8) ROMba can store your ROMs on any (set of) paths you specify - Tell ROMba where to put ROMs, How much space it is allowed to occupy, and where to place them.
(9) ROMba can build diff-DATs - For example you require a MAME 0.148-0.152 ROM update set? ROMba can make the DATfile, the ROM set or both!

