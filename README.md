# mtFileCopy

This script uses threads to run multiple rsync copies of the source folder to the target folder.
The source and target names must match.

## Issues:

You can use rsync directly, but I find that it has a few issues:
* performance:  I find rsync very slow in the copying.  It does not appear to efficiently manage threads so the drives and the system bandwidth is not fully utilized;
* slow startup:  It seems that rsync needs to build the entire tree before it starts to copy.  If we use checksums to confirm the files, it can take days to build a list when the volumes are in the Terabytes;
* inability to stop and start:  Given that it takes days for my arrays to back up (between 32 and 64 TB), some drives will go offline momentarily - especially when the drives are running at their limits and begin to heat up.  By itself, rsync does not have a persistent queue so any failure means restarting.

## Solution:

This script solves the above issues by:
* performance:  Runs multiple instances of rsync - each with only one file to handle so the parallel processing is managed outside of rsync;
* slow startup:  By running separate rsync instances, each only needs to checksum one file.  For large files, it will still take a while to checksum the source and target, but not the hours or days that rsync would take for doing a checksum on all files before starting the copy;
* inability to stop and start:  This script uses a queue file.  If the queue file is empty or missing, the script traverses the paths from the source and buils a new queue file which will be updated as the files are copied.

## Usage:

```
$ mtFileCopy.py
usage: mtFileCopy.py [-h] --source srcPath --target trgPath [--threads threads] [--execute] [--move] [--fast] [--log loglevel] [--tmpdir TMPDIR] [--create] [--queue QUEUE]
```

* -h : prints the help information
* --source <srcPath>  : required - the top-level folder from which to copy files.
* --target <trgPath>  : required - the top-level folder to which to copy files.
* --threads <threads> : optional - the number of threads to use.  The default is 1.
* --execute           : optional - if not specified, rsync will be run as a dry-run and the copy will not be done.  Use --execute to actuallly do the copy or move.
* --move              : optional - if specified, rsync is told to remove the source file when the copy is done.
* --fast              : optional - by default rsync is run with --checksum.  If --fast is specified, --checksum will not be passed to rsync making it run faster but only use dates and file sizes to determine if the file should be copied.
* --log <logleve>     : optional - sets the level of logging.  The default is "INFO".  Other values include "WARN", "DEBUG", etc.
* --tmpdir <tmpdir>   : optional - specify where to write the log files.  The default is the current folder.
* --create            : optional - create the target folder if it is missing.  Use this option if mtFileCopy fails with the error TARGET_PATH_DOES_NOT_EXIST.
* --queue <queue>     : optional - the path to the queue.  Specifies the location of the queue file to which a list of files will be written. If interrupted, the script can be restarted with this queue file which will contain a list of the files not yet processed.  The default is mtFileCopy.queue.


## Example:

In this example, mtFileCopy.py is used to copy the VideoScratch array to a backup location.  Note that the backup location must have the same name as the top-level folder of the source.

```
$ mtFileCopy.py --execute --source /Volumes/VideoScratch --target /Volumes/BackupVideoScratch/VideoScratch  --execute --threads 5 --tmpdir /Volumes/VideoScratch/mtFileCopy --queue /Volumes/VideoScratch/mtFileCopy/mtFileCopy.queue
```
