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
#import multiprocessing
#from multiprocessing import Pool
import threading
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import timeit

class ERR(Enum):
    OK = 0
    SOURCE_PATH_DOES_NOT_EXIST = auto()
    TARGET_PATH_DOES_NOT_EXIST = auto()
    SOURCE_TARGET_MISMATCH = auto()
    PERMISSION_ERROR = auto()


class LeafFinder:
    """Multi-threaded leaf-node finder"""
    m_threads = 1
    m_sourcePath = ""
    m_targetPath = ""
    m_nodes = []
    m_logLevel   = logging.INFO
    m_dry = True
    m_move = False
    m_fast = False
    m_create = False
    m_tmpdir = "."
    m_queueFileName = ""

    m_ignoreFiles = [ ".DocumentRevisions-V100",
                      ".Spotlight-V100",
                      ".TemporaryItems",
                      ".Trashes",
                      ".fseventsd",
                      ".DS_Store",
                      ".apdisk"
                    ]

    def __init__( self, args ):
        self.m_threads = int( args.threads )
        self.m_sourcePath = args.source
        self.m_targetPath = args.target
        self.m_nodes = []
        self.m_dry = not args.execute
        self.m_move = args.move
        self.m_fast = args.fast
        self.m_logLevel = getattr(logging, args.loglevel.upper(), None )
        self.m_create = args.create
        self.m_tmpdir = args.tmpdir
        self.m_queueFileName = args.queue

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
        # add the leaf node.
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
        logger.info( f'================== { datetime.now() }, size = {filesize/1024000000.0:.03f} G, file = {src} ========== {cmd}' )
        print( f'================== { datetime.now() }, size = {filesize/1024000000.0:.03f} G, file = {src} ========== {cmd}' )
        cmdArr = cmdpre.split()
        cmdArr += [ cmdfilt0, cmdfilt1, src, trg ]
        res = subprocess.run( cmdArr, capture_output=True, text=False )
        if( res.returncode != 0 ):
            logger.error( res )
            for line in res.stderr.decode("latin-1").split('\n'):
                print( line )
                logger.error( line )
        for line in res.stdout.decode("latin-1").split('\n'):
            print( line )
            logger.info( line )
        endtime = timeit.default_timer()
        delta = endtime - starttime
        if( delta > 0.0 ):
            rateMBps = filesize / 1024000.0 / delta
        else:
            rateMBps = 0.0
        logger.info( f'================== endTime = { datetime.now() }, rate = {rateMBps:.03f} MBps, duration = {delta:.03f} s, size = {filesize/1024000000.0:.03f} G, file = {src}' )
        print( f'================== endTime = { datetime.now() }, rate = {rateMBps:.03f} MBps, duration = {delta:.03f} s, size = {filesize/1024000000.0:.03f} G, file = {src}' )
        #return( f'{os.getpid()},\t{datetime.now()},\t{src}' )
        return( ( os.getpid(), filePath, datetime.now() ) )

    def worker_init(self) -> None:
        pidName = os.path.join( self.m_tmpdir, f'mpFileCopy.{ os.getpid() }.log' )
        logging.basicConfig( filename=pidName, level=self.m_logLevel )
        logger = logging.getLogger()
        logger.setLevel( self.m_logLevel )
        logger.info( f'================== { datetime.now() }  {pidName}')

    def waitForFreeThread( self, src, main_thread_count, all_threads, copy_of_nodes, timeout=None ) -> None:
        ''' Wait for a free thread and remove names of completed threads from the queue '''
        logger = logging.getLogger()
        logger.info(f'total={len(copy_of_nodes)},  current={src}')
        # if we already have our limit of threads, wait for a free one.
        logger.debug( f'Threads in use: {threading.active_count() - main_thread_count}, all_theads size {len(all_threads)}   selected thread size: {self.m_threads}' )
        do_once = True
        while ( ((threading.active_count() - main_thread_count) >= self.m_threads) or (do_once) ):
            do_once = False
            logger.debug( f'Total threads = {threading.active_count()}' )
            # get the names of all the currently running threads
            running_thread_names = []
            for running_thread in threading.enumerate():
                running_thread_names.append( running_thread.name )
            # join each outstanding thread until it times out or is joinable
            for thread_name in all_threads:
                # if the thread is not in the running_threads list, remove it
                if( thread_name not in running_thread_names ):
                    logger.debug( f'removing expired thread: {thread_name}')
                    copy_of_nodes.remove( thread_name )
                    all_threads.remove( thread_name )  # this will also have issues
                else:
                    for running_thread in threading.enumerate():
                        if( running_thread.name == thread_name ):
                            logger.debug( f'waiting for up to {timeout} seconds for thread: {thread_name},  current={src}')
                            running_thread.join( timeout=timeout ) # wait for 'timeout' second to see if thread exits
                            # if we joined, remove the src from the queue
                            if not running_thread.is_alive():
                                logger.debug( f'removing joined thread: {thread_name}')
                                copy_of_nodes.remove( thread_name )
                                all_threads.remove( thread_name )  # this will also have issues
        self.updateQueueFile( copy_of_nodes )


    def mpRsyncCopy( self ) -> None:
        ''' create m_threads processes of files and do rsyncCopy on each. '''
        logger = logging.getLogger()
        logger.info( f'Threads == {self.m_threads}' )
        # For each record in the queue, create a thread (up to m_threads) and process.  Once completed, remove the record and replace the thread.
        main_thread_count = threading.active_count()
        all_threads = []
        copy_of_nodes = self.m_nodes.copy()
        for src in self.m_nodes:
            logger.info( f'Files remaining: {len(self.m_nodes)}, current={src}' )
            self.waitForFreeThread( src, main_thread_count, all_threads, copy_of_nodes, timeout=5 )
            # add a new thread for the next file
            current_thread = threading.Thread( target=self.rsyncFile, args=(f'{src}',), name=src )
            logger.debug( f'created thread: {current_thread.name}')
            all_threads.append( current_thread.name )
            current_thread.start()
        while ( len(copy_of_nodes) > 0 ):
            logger.info( f'Waiting for {len(copy_of_nodes)} final threads.' )
            self.waitForFreeThread( src, main_thread_count, copy_of_nodes, all_threads )

    def loadQueue( self ) -> None:
        ''' load the previous file into the queue '''
        logger = logging.getLogger()
        action = ""
        direction = ""
        queueFileName = os.path.join( self.m_tmpdir, self.m_queueFileName )
        if( os.path.exists( queueFileName )  and  os.path.isfile( queueFileName ) and os.path.getsize( queueFileName ) ):
            logger.info( f'Loading previous queue from {queueFileName}' )
            action = "Loaded"
            direction = "from"
            with open( queueFileName ) as inpf:
                for record in inpf:
                    self.m_nodes.append( record.strip() )
        else:
            # build a new list of leaf nodes in m_nodes
            logger.info( f'Not loading previous queue from {queueFileName}' )
            action = "Saved"
            direction = "to"
            self.findAllLeafNodes( "" )
            # write all the new values to the queue file
            self.updateQueueFile( self.m_nodes )
        logger.info( f'{action} {len(self.m_nodes)} records {direction} {queueFileName}' )

    def updateQueueFile( self, queue ):
        ''' write the m_nodes to the queue file in case the backup stops before completing. '''
        logger = logging.getLogger()
        queueFileName = os.path.join( self.m_tmpdir, self.m_queueFileName )
        logger.info( f'Writing {len(queue)} records to queue file {queueFileName}' )
        with open( queueFileName, 'w' ) as outf:
            for line in queue:
                outf.write( f'{line}{os.linesep}' )

    def run( self ) -> int:
        '''Verify that the source and target exist and are the same folder name, then find the leaf nodes of the source.'''
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
        # load the previous list if it exists into m_nodes or generate a new one
        self.loadQueue()
        logger.info( f" ======================================== listed: {datetime.now()}" )
        self.mpRsyncCopy()
        logger.info( f" ========================================    end: {datetime.now()}" )
        return ERR.OK



def logger_init( logLevel, tmpdir ):
    if( (tmpdir is not None) and (tmpdir) ):
        if( not ( os.path.exists( tmpdir ) and os.path.isdir( tmpdir ) ) ):
            try:
                os.makedirs( tmpdir )
            except PermissionError:
                print( f'Error creating tmpdir folder {tmpdir}.  Please check --tmp parameter.')
                return ERR.PERMISSION_ERROR
        logFilePath = os.path.join( tmpdir, 'mpFileCopy.control.log' )
    else:
        logFilePath = 'mpFileCopy.control.log'
    # add rotating file handler
    rfh = RotatingFileHandler( filename=logFilePath, mode='a', maxBytes=1024000, backupCount=9 )
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
    logger.info( f'CWD={os.getcwd()}' )
    logger.info( f'LogFile={logFilePath}' )
    return ERR.OK



if __name__ == '__main__':
    parser = argparse.ArgumentParser( description='Copy files from the source folder to the target path on separate processes.' )
    parser.add_argument( "--source", "-s", metavar="srcPath", help="Source Path", required=True )
    parser.add_argument( "--target", "-t", metavar="trgPath", help="Target Path", required=True )
    parser.add_argument( "--threads", "-n", metavar="threads", help="Number of threads", default=1 )
    parser.add_argument( "--execute", "-x", help="Perform the copy/move rather than a dry run.", action='store_true', default=False, required=False )
    parser.add_argument( "--move", "-m", help="Move files", action='store_true', default=False, required=False )
    parser.add_argument( "--fast", "-f", help="Fast compare - no checksum", action='store_true', default=False, required=False )
    parser.add_argument( "--log", "-l", dest="loglevel", metavar="loglevel", help="Log leval as WARN, INFO, DEBUG, etc", default="INFO" )
    parser.add_argument( "--tmpdir", "-T", help="Location of logs and work queue.  Default is current folder.", default="." )
    parser.add_argument( "--create", "-c", help="Create target folder if missing", action='store_true', default=False, required=False )
    parser.add_argument( "--queue", "-q", help="Use list from and write to this queue file.  Default is mpFileCopy.queue", default="mpFileCopy.queue", required=False )
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper(), None )
    #
    rcode = logger_init( numeric_level, args.tmpdir )
    if( ERR.OK == rcode ):
        if not isinstance( numeric_level, int ):
            raise ValueError( f"Invalid log level: {args.loglevel}" )
        logging.basicConfig( filename="mpFileCopy.log", level=numeric_level )
        leafFinder = LeafFinder( args )
        rcode = leafFinder.run()
    exit( rcode )

