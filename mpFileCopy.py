#!/usr/bin/env python3.7

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
from logging.handlers import QueueHandler, QueueListener

class ERR(Enum):
    SOURCE_DOES_NOT_EXIST = auto()
    TARGET_DOES_NOT_EXIST = auto()
    SOURCE_TARGET_MISMATCH = auto()


class LeafFinder:
    """Multi-threaded leaf-node finder"""
    m_threads = 1
    m_sourcePath = ""
    m_targetPath = ""
    m_logLevel   = logging.INFO

    def __init__( self, src, trg, thrds, level ):
        self.m_threads = int(thrds)
        self.m_sourcePath = src
        self.m_targetPath = trg
        self.m_nodes = []
        self.m_logLevel = level

    def confirmFolderExists( self, path ) -> bool:
        """ return true if the path is a folder that exists """
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
        src = os.path.join( self.m_sourcePath, filePath )
        trg = os.path.dirname( os.path.join( self.m_targetPath, filePath ) )
        logger.debug( f"Copying src={src}\n\ttrg={trg}" )
        # create target folder if it does not exist
        if not os.path.isdir( trg ):
            os.makedirs( trg, mode=0o775, exist_ok=True )
        # -e ssh -T -c arcfour -o Compression=no
        #  --progress
        cmdpre = 'rsync   -v -v --progress  --perms --links --times --itemize-changes --stats --backup --suffix=.backup  --exclude=.DS_Store --exclude=.Trashes --exclude=.Trash --exclude=._.Trashes --exclude=.localized --exclude=.DocumentRevisions-* --exclude=.Spotlight* --exclude=.fseventsd --exclude=.apdisk --exclude=.com.apple.timemachine.donotpresent --exclude=.fcplock --exclude=.fcpuser --exclude=.fseventsd --exclude=.cache --exclude=._.TemporaryItems --exclude=._.apdisk --exclude=.TemporaryItems'
        cmdfilt0='--filter=dir-merge /.rsync.include'
        cmdfilt1='--filter=dir-merge /.rsync.exclude'
        cmd = cmdpre + ' ' + cmdfilt0 + ' ' + cmdfilt1 + ' "' + src + '" "' + trg + '"'
        logging.info( f' ========== {cmd}' )
        print( f' ========== {cmd}' )
        cmdArr = cmdpre.split()
        cmdArr += [ cmdfilt0, cmdfilt1, src, trg ]
        #res = subprocess.run( cmdArr, capture_output=True, text=True )
        res = subprocess.run( cmdArr, capture_output=True, text=False )
        if( res.returncode != 0 ):
            logging.error( res )
            for line in res.stderr.decode("latin-1").split('\n'):
                print( line )
                logging.error( line )
        for line in res.stdout.decode("latin-1").split('\n'):
            print( line )
            logging.info( line )


    def mpRsyncCopy( self ):
        ''' create m_threads processes of files and do rsyncCopy on each. '''
        with Pool( processes=self.m_threads ) as pool:
            pool.map( self.rsyncFile, self.m_nodes )

    def run( self ) -> int:
        """Verify that the source and target exist and are the same folder name, then find the leaf nodes of the source."""
        logger.info( f" ========================================  start: {datetime.now()}" )
        if not self.confirmFolderExists( self.m_sourcePath ):
            logger.error( f"Source path does not exist: {self.m_sourcePath}" )
            logger.info( f" ========================================    end: {datetime.now()}" )
            return ERR.SOURCE_DOES_NOT_EXIST
        if not self.confirmFolderExists( self.m_targetPath ):
            logger.error( f"Target path does not exist: {self.m_targetPath}" )
            logger.info( f" ========================================    end: {datetime.now()}" )
            return ERR.TARGET_DOES_NOT_EXIST
        if not self.leafNodesMatch( self.m_sourcePath, self.m_targetPath ):
            logger.error( f"Source and Target must have the same starting point." )
            logger.info( f"Source = {self.m_sourcePath}" )
            logger.info( f"Target = {self.m_targetPath}" )
            logger.info( f" ========================================    end: {datetime.now()}" )
            return ERR.SOURCE_TARGET_MISMATCH
        logger.info( f"Source = {self.m_sourcePath}" )
        logger.info( f"Target = {self.m_targetPath}" )
        logger.info( f"Threads = {self.m_threads}" )
        self.findAllLeafNodes( "" )
        logger.info( f" ======================================== listed: {datetime.now()}" )
        self.mpRsyncCopy()
        logger.info( f" ========================================    end: {datetime.now()}" )


if __name__ == '__main__':
    logger = logging.getLogger("mpFileCopy.logger")
    parser = argparse.ArgumentParser( description='Copy files from the source folder to the target path on separate processes.' )
    parser.add_argument( "--source", "-s", metavar="srcPath", help="Source Path", required=True )
    parser.add_argument( "--target", "-t", metavar="trgPath", help="Target Path", required=True )
    parser.add_argument( "--threads", "-n", metavar="threads", help="Number of threads", default=1 )
    parser.add_argument( "--log", "-l", dest="loglevel", metavar="loglevel", help="Log leval as WARN, INFO, DEBUG, etc", default="INFO" )
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper(), None )
    if not isinstance( numeric_level, int ):
        raise ValueError( f"Invalid log level: {args.loglevel}" )
    logging.basicConfig( filename="mpFileCopy.log", level=numeric_level )
    leafFinder = LeafFinder(  args.source, args.target, args.threads, numeric_level )
    sys.exit( leafFinder.run() )

