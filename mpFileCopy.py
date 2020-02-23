#!/usr/bin/env python3.8

"""
Given a source path and a destination path where the last element of the path matches,
find the leaf nodes of the source folder tree.
"""

import os.path
import logging
import argparse
import sys
import subprocess
from datetime import datetime
from enum import Enum, auto
import multiprocessing
from multiprocessing import Pool
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import timeit

class ERR(Enum):
    OK = 0
    SOURCE_PATH_DOES_NOT_EXIST = auto()
    TARGET_PATH_DOES_NOT_EXIST = auto()
    SOURCE_TARGET_MISMATCH = auto()


class LeafFinder:
    """Multi-threaded leaf-node finder"""
    m_threads = 1
    m_sourcePath = ""
    m_targetPath = ""
    m_logLevel   = logging.INFO
    m_dry = True
    m_move = False
    m_fast = False
    m_create = False

    m_ignoreFiles = [ ".DocumentRevisions-V100",
                      ".Spotlight-V100",
                      ".TemporaryItems",
                      ".Trashes",
                      ".fseventsd",
                      ".DS_Store"
                    ]

    def __init__( self, src, trg, thrds, dry, move, fast, level, create ):
        self.m_threads = int(thrds)
        self.m_sourcePath = src
        self.m_targetPath = trg
        self.m_nodes = []
        self.m_dry = dry
        self.m_move = move
        self.m_fast = fast
        self.m_logLevel = level
        self.m_create = create

    def confirmFolderExists( self, path ) -> bool:
        """ return true if the path is a folder that exists """
        logger = logging.getLogger()
        if not os.path.exists( path ):
            logger.error( f"Folder ({path}) does not exist." )
            return False
        if not os.path.isdir( path ):
            logger.error( f"Folder ({path}) is not a folder." )
            return False
        return True

    def leafNodesMatch( self,  src, trg ) -> bool:
        return ( os.path.basename( os.path.realpath(src) ) == os.path.basename( os.path.realpath(trg) ) )

    def findAllLeafNodes( self, currentPath ):
        logger = logging.getLogger()
        # if the current is in the ignore  list, skip it
        if os.path.basename( currentPath ) in self.m_ignoreFiles:
            logger.warning( f'Ignoring { os.path.join( self.m_sourcePath, currentPath ) }')
            return
        # if the path is a folder, iterate of the contents can call this function recursively
        fullCurrent = os.path.join( self.m_sourcePath, currentPath )
        if os.path.isdir( fullCurrent ):
            for child in os.listdir( fullCurrent ):
                self.findAllLeafNodes( os.path.join( currentPath, child ) )
            return
        logger.debug( f"Adding leaf node: {currentPath}" )
        self.m_nodes.append( currentPath )
        return

    def rsyncFile( self, filePath ):
        logger = logging.getLogger()
        src = os.path.join( self.m_sourcePath, filePath )
        trg = os.path.dirname( os.path.join( self.m_targetPath, filePath ) )
        logger.debug( f"Copying src={src}\n\ttrg={trg}" )
        # create target folder if it does not exist
        if not os.path.isdir( trg ):
            os.makedirs( trg, mode=0o775, exist_ok=True )
        cmdpre = 'rsync   '
        # add dry-run if specified
        if self.m_dry:
            cmdpre += ' --dry-run '
        # add move if specified
        if self.m_move:
            cmdpre += ' --remove-source-files '
        # if not fast, use checksum
        if not self.m_fast:
            cmdpre += ' --checksum '
        #
        # --progress
        #
        cmdpre += ' -v -v  --perms --links --times --itemize-changes --stats'
        cmdpre += ' --backup --suffix=.backup  --exclude=.DS_Store --exclude=.Trashes --exclude=.Trash'
        cmdpre += ' --exclude=._.Trashes --exclude=.localized --exclude=.DocumentRevisions-*'
        cmdpre += ' --exclude=.Spotlight* --exclude=.fseventsd --exclude=.apdisk'
        cmdpre += ' --exclude=.com.apple.timemachine.donotpresent --exclude=.fcplock --exclude=.fcpuser'
        cmdpre += ' --exclude=.fseventsd --exclude=.cache --exclude=._.TemporaryItems --exclude=._.apdisk'
        cmdpre += ' --exclude=.TemporaryItems'
        #
        cmdfilt0='--filter=dir-merge /.rsync.include'
        cmdfilt1='--filter=dir-merge /.rsync.exclude'
        cmd = cmdpre + ' ' + cmdfilt0 + ' ' + cmdfilt1 + ' "' + src + '" "' + trg + '"'
        filesize = os.path.getsize( src )
        starttime = timeit.default_timer()
        logging.info( f'================== { datetime.now() }, size={filesize/1024000000.0:.3f}G, file={src} ========== {cmd}' )
        print( f'================== { datetime.now() }, size={filesize/1024000000.0:.3f}G, file={src} ========== {cmd}' )
        cmdArr = cmdpre.split()
        cmdArr += [ cmdfilt0, cmdfilt1, src, trg ]
        res = subprocess.run( cmdArr, capture_output=True, text=False )
        if( res.returncode != 0 ):
            logging.error( res )
            for line in res.stderr.decode("latin-1").split('\n'):
                print( line )
                logging.error( line )
        for line in res.stdout.decode("latin-1").split('\n'):
            print( line )
            logging.info( line )
        endtime = timeit.default_timer()
        delta = endtime - starttime
        logging.info( f'================== endTime={ datetime.now() }, duration={delta:.0}s, size={filesize/1024000000.0:.3f}G, file={src}' )
        print( f'================== endTime={ datetime.now() }, duration={delta:.0}s, size={filesize/1024000000.0:.3f}G, file={src}' )
        return( f'{os.getpid()},\t{datetime.now()},\t{src}' )

    def worker_init(self):
        pidName = f'mpFileCopy.{ os.getpid() }.log'
        logging.basicConfig( filename=pidName, level=self.m_logLevel )
        logger = logging.getLogger()
        logger.setLevel( self.m_logLevel )
        logging.info( f'================== { datetime.now() }')

    def mpRsyncCopy( self ):
        ''' create m_threads processes of files and do rsyncCopy on each. '''
        print( f'Threads == {self.m_threads}' )
        pool = multiprocessing.Pool( self.m_threads, self.worker_init )
        for result in pool.map( self.rsyncFile, self.m_nodes ):
            logging.info( f'Process completed: {result}' )
        pool.close()
        pool.join()

    def run( self ) -> int:
        """Verify that the source and target exist and are the same folder name, then find the leaf nodes of the source."""
        logger = logging.getLogger()
        logger.info( f" ========================================  start: {datetime.now()}" )
        if not self.leafNodesMatch( self.m_sourcePath, self.m_targetPath ):
            logger.error( f"Source and Target must have the same starting point." )
            logger.info( f"Source = {self.m_sourcePath}" )
            logger.info( f"Target = {self.m_targetPath}" )
            logger.info( f" ========================================    end: {datetime.now()}" )
            return ERR.SOURCE_TARGET_MISMATCH
        if not self.confirmFolderExists( self.m_sourcePath ):
            logger.error( f"Source path does not exist: {self.m_sourcePath}" )
            logger.info( f" ========================================    end: {datetime.now()}" )
            return ERR.SOURCE_PATH_DOES_NOT_EXIST
        if not self.confirmFolderExists( self.m_targetPath ):
            logger.error( f"Target path does not exist: {self.m_targetPath}" )
            # if the user specified --create, create the directory hierarchy
            if self.m_create:
                logger.info( f"Creating target path: {self.m_targetPath}" )
                os.makedirs( self.m_targetPath, mode=0o755, exist_ok=True )
            else:
                logger.info( f" ========================================    end: {datetime.now()}" )
                return ERR.TARGET_PATH_DOES_NOT_EXIST
        logger.info( f"Source = {self.m_sourcePath}" )
        logger.info( f"Target = {self.m_targetPath}" )
        logger.info( f"Threads = {self.m_threads}" )
        self.findAllLeafNodes( "" )
        logger.info( f" ======================================== listed: {datetime.now()}" )
        self.mpRsyncCopy()
        logger.info( f" ========================================    end: {datetime.now()}" )
        return ERR.OK



def logger_init( logLevel ):
    logFilePath = 'mpFileCopy.control.log'
    # add rotating file handler
    rfh = RotatingFileHandler( filename=logFilePath, mode='a', maxBytes=10240000, backupCount=9 )
    # if the log file already exists and is not empty, do a rollover
    if( os.path.exists( logFilePath ) and os.path.getsize( logFilePath ) ):
        rfh.doRollover()
    rfh.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s - %(process)s - %(message)s"))
    rfh.setLevel( logLevel )
    logger = logging.getLogger()
    logger.setLevel( logLevel )
    logger.addHandler( rfh )
    # add stdout handler
    sh = logging.StreamHandler( sys.stdout )
    sh.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s - %(process)s - %(message)s"))
    sh.setLevel( logging.INFO )
    logger.addHandler( sh )
    logging.info( f' ==================== { datetime.now() }')


if __name__ == '__main__':
    parser = argparse.ArgumentParser( description='Copy files from the source folder to the target path on separate processes.' )
    parser.add_argument( "--source", "-s", metavar="srcPath", help="Source Path", required=True )
    parser.add_argument( "--target", "-t", metavar="trgPath", help="Target Path", required=True )
    parser.add_argument( "--threads", "-n", metavar="threads", help="Number of threads", default=1 )
    parser.add_argument( "--execute", "-x", help="Perform the copy/move rather than a dry run.", action='store_true', default=False, required=False )
    parser.add_argument( "--move", "-m", help="Move files", action='store_true', default=False, required=False )
    parser.add_argument( "--fast", "-f", help="Fast compare - no checksum", action='store_true', default=False, required=False )
    parser.add_argument( "--log", "-l", dest="loglevel", metavar="loglevel", help="Log leval as WARN, INFO, DEBUG, etc", default="INFO" )
    parser.add_argument( "--create", "-c", help="Create target folder if missing", action='store_true', default=False, required=False )
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper(), None )
    #
    logger_init( numeric_level )
    if not isinstance( numeric_level, int ):
        raise ValueError( f"Invalid log level: {args.loglevel}" )
    logging.basicConfig( filename="mpFileCopy.log", level=numeric_level )
    leafFinder = LeafFinder( args.source, args.target, args.threads, not args.execute, args.move, args.fast, numeric_level, args.create )
    sys.exit( leafFinder.run() )

