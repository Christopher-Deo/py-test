"""

  Facility:         ILS

  Module Name:      MNMCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2009, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for MNM for ASAP processing.
      
  Author:
      Jarrod Wild

  Creation Date:
      01-Apr-2009

  Modification History:

"""
from ILS import ASAP
from ILS.ASAP.CaseFactory import ASAPCaseFactory
from ILS.ASAP.DocumentFactory import ASAPDocumentFactory
from ILS.ASAP.DocumentHistory import ASAPDocumentHistory
from ILS.ASAP.MainHandler import ASAPMainHandler
from ILS.ASAP.IndexHandler import ASAPIndexHandler
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler
from ILS.ASAP.Utility import ASAPUtility
from ILS import ILSDocumentBundling as idb
from DeltaIdentity import DeltaIdentity
import CRLUtility
import datetime
import glob
import os
import odbc
import sets
import sys
import time

def CreateBatchID(batchDate, idxValue):
    """
    """
    return 'IS%s%s' % (batchDate.strftime("%m%d%Y"), idxValue)

def CreateTransactIndex(polnum, docnum, batchDate, batchID):
    """
    """
    return ('14^000|ILF|%s|%s|%s|N|1223|%s|image/tiff^^'
                           % (polnum, docnum, batchDate.strftime("%m/%d/%Y"),
                              batchID) )

def BuildDocumentZip( xmitDir, transactIdx, batchid, tifflist ):
    fSuccess = False
    ptr = open(os.path.join(xmitDir, batchid + '.eob'), 'w')
    ptr.write('Individual\\%s 1 %s' % (batchid, str(len(tifflist))))
    ptr.close()
    idxString = transactIdx + '|'.join( [os.path.basename(tif) for tif in tifflist] )
    ptr = open(os.path.join(xmitDir, 'transact.dat') , 'w')
    ptr.write(idxString)
    ptr.close()
    newTifflist = []
    for tif in tifflist:
        if os.path.dirname(tif).upper() != xmitDir.upper():
            newTif = os.path.join( xmitDir, os.path.basename(tif) )
            CRLUtility.CRLCopyFile(tif, newTif, True, 5)
            newTifflist.append( newTif )
        else:
            newTifflist.append( tif )
    zipFilename = batchid + '.zip'
    for tiff in newTifflist:
        CRLUtility.CRLZIPFiles( tiff, os.path.join(xmitDir, zipFilename), True )
    CRLUtility.CRLZIPFiles( os.path.join(xmitDir, '*.eob'),
            os.path.join(xmitDir, zipFilename),
            True )
    CRLUtility.CRLZIPFiles( os.path.join(xmitDir, '*.dat'),
            os.path.join(xmitDir, zipFilename),
            True )
    fSuccess = True
    return fSuccess


class MNMIndexHandler( ASAPIndexHandler ):
    """
    Custom handler for building indexes for MNM.
    """
    
    def _postProcessIndex( self ):
        idxPaths = self._getIndexPaths()
        today = datetime.datetime.today()
        for idxPath in idxPaths:
            # build transact.dat format file to replace 'intermediary' index
            ptr = open( idxPath, 'r' )
            lines = ptr.readlines()
            ptr.close()
            docFields = lines[0].strip().split(',')
            docMap = {}
            for field in docFields:
                name, val = field.split('=')
                docMap[name] = val
 
            batchid = CreateBatchID(today, os.path.basename(idxPath)[:-4])
            
            polNum = docMap['POLNUM']
            if not polNum:
                polNum = 'LIFE000'
            transactIdx = CreateTransactIndex(polNum, docMap['DOCNUM'], today, batchid)
            
            ptr = open( idxPath, 'w' )
            ptr.write(transactIdx)
            ptr.close()
        return True

class MNMTransmitHandler( ASAPTransmitHandler ):
    """
    Custom handler for MNM transmission.
    """
    
    def _preStage( self ):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join( xmitStagingPath, 'zip' )
        xmitPgpPath = os.path.join( xmitStagingPath, 'pgp' )
        reviewPath = os.path.join( xmitStagingPath, 'review' )
        retransPath = os.path.join( xmitStagingPath, 'retrans' )
        try:
            # if there are any files here, they were left behind when a previous
            # run failed to complete, so move them to the reviewPath folder
            toXmitFiles = glob.glob( os.path.join(xmitStagingPath, '*.*') )
            for xmitFile in toXmitFiles:
                baseFileName = os.path.basename( xmitFile )
                CRLUtility.CRLCopyFile( xmitFile,
                                        os.path.join(reviewPath, baseFileName),
                                        True, 5 )
            if len( toXmitFiles ) > 0:
                self._getLogger().error(
                    'Files were left behind in ' +
                    '%s from a previous run and have been moved to the review subfolder.'
                    % xmitStagingPath )
            # move any PGP-encrypted zip files to reviewPath folder
            pgpFiles = glob.glob( os.path.join(xmitPgpPath, '*.*') )
            for pgpFile in pgpFiles:
                baseFileName = os.path.basename( pgpFile )
                CRLUtility.CRLCopyFile( pgpFile,
                                        os.path.join(reviewPath, baseFileName),
                                        True, 5 )            
            if len( pgpFiles ) > 0:
                self._getLogger().error(
                    'PGP-encrypted zip files were left behind in ' +
                    '%s from a previous run and have been moved to the review subfolder.'
                    % xmitPgpPath )
            # now move files from the retrans folder to the xmit staging folder
            toRetransFiles = glob.glob( os.path.join(retransPath, '*.*') )
            for retransFile in toRetransFiles:
                baseFileName = os.path.basename( retransFile )
                CRLUtility.CRLCopyFile( retransFile,
                                        os.path.join(xmitStagingPath, baseFileName),
                                        True, 5 )
        except:
            self._getLogger().warn(
                'Pre-stage failed with exception: ', exc_info=True )
            fSuccess = False
        return fSuccess
    
    def _isIndexedCaseReady( self ):
        fReady = False
        # check to see if time of day is within MNM's window
        # (6am - 5:30pm)
        case = self._getCurrentCase()
        xmitDir = case.contact.xmit_dir
        now = datetime.datetime.today()
        today_begin = datetime.datetime( now.year, now.month, now.day,
                                         6, 0 )
        today_end = datetime.datetime( now.year, now.month, now.day,
                                       17, 30 )
        hist = ASAPDocumentHistory()
        if ( now >= today_begin and now <= today_end and
             (os.path.isfile(os.path.join(os.path.dirname(xmitDir),
                                          'reports',
                                          '%s_01.TIF' % case.sid)) or
              hist.getTrackedDocidsForCase(case, hist.ACTION_TRANSMIT)) ):
            fReady = True
        return fReady
    
    def __buildDocumentZip(self, transactIdx, batchid, tifflist):
        xmitDir = self._getCurrentCase().contact.xmit_dir
        return BuildDocumentZip( xmitDir, transactIdx, batchid, tifflist )
        
    def _stageIndexedCase( self ):
        cursor = ASAP.xmitConfig.getCursor(ASAP.xmitConfig.DB_NAME_XMIT)
        iRet = cursor.execute("""select db_connect_string from asap_db_settings where db_name = '%s' 
                                """ % (ASAP.xmitConfig.DB_NAME_DELTA_QC))
        rec = cursor.fetch(True)
        conString, = rec
        qcCon = odbc.odbc(conString)
        case = self._getCurrentCase()
        fSuccess = True
        fromToMoves = []
        # now try to get doc/index pairs
        documents = list(case.getDocuments().values())
        processedSubdir = ASAP.xmitConfig.getSetting(
            ASAP.xmitConfig.SETTING_PROCESSED_SUBDIR )
        # map single-page images by doctype -> {'3': (transactIdx, batchId, [file1.tif,file2.tif,...])}
        docTypeFileMap = {}
        # Report Processing
        # Pull index info from Documents[0]
        if documents:
            docIndex = os.path.join( case.contact.index_dir, '%s.IDX' % documents[0].fileName[:-4])
            if os.path.isfile(docIndex):
                ptr = open(docIndex, 'r')
                idxData = ptr.read()
                ptr.close()
                idxFields = idxData.split('|')
                polnum = idxFields[2]
                batchDate = CRLUtility.ParseStrDate(idxFields[4], True)
                # Generate the batch id from a new page id
                idfact = DeltaIdentity(DeltaIdentity.DB_ILS_QC)
                pageid = idfact.getNewIdValue(DeltaIdentity.TBL_PAGES)
                batchid = CreateBatchID(batchDate, '%08d' % int(pageid)) 
                transactIdx = CreateTransactIndex(polnum, '3', batchDate, batchid)
                
        # get the single page tiff's by glob() the report folder
                reportFiles = glob.glob(os.path.join(os.path.dirname(self._getContact().xmit_dir),
                                         'reports',
                                         '%s_*.tif' % (case.sid) ) )
                if reportFiles:
                    tupleRec = docTypeFileMap.get( '3' )
                    if not tupleRec:
                        tupleRec = (transactIdx, batchid, [])
                        docTypeFileMap['3'] = tupleRec
                    tupleRec[2].extend( reportFiles )
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            docPath = os.path.join( case.contact.document_dir,
                                    processedSubdir,
                                    doc.fileName )
            idxPath = os.path.join( case.contact.index_dir, '%s.IDX' % docPrefix )
            if os.path.isfile( docPath ) and os.path.isfile( idxPath ):
                singlePageTiffs = idb.getSinglePageTiffsFromMulti(docPath,case.contact.xmit_dir,qcCon)
                ptr = open(idxPath, 'r')
                idxData = ptr.read()
                ptr.close()
                idxFields = idxData.split('|')
                batchID = idxFields[7]
                docType = idxFields[3]
                tupleRec = docTypeFileMap.get( docType )
                if not tupleRec:
                    tupleRec = (idxData, batchID, [])
                    docTypeFileMap[docType] = tupleRec
                tupleRec[2].extend( singlePageTiffs )                
                # Clean the tif's and idx's
                CRLUtility.CRLDeleteFile(idxPath)
                CRLUtility.CRLDeleteFile(docPath)
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid %s (sid %s).'
                    % (doc.getDocumentId(), case.sid) )
        for docType in list(docTypeFileMap.keys()):
            idxData, batchId, filelist = docTypeFileMap[docType]
            fSuccess = self.__buildDocumentZip( idxData, batchId, filelist ) and fSuccess            
        return fSuccess

    def _transmitStagedCases( self ):
        fSuccess = True
        MNM_FTP_HOSTNAME = 'Minnesota Life'
        MNM_REMOTE_USER = 'Individual Sales and Marketing'
        sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo( MNM_FTP_HOSTNAME )
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join( xmitStagingPath, 'zip' )
        xmitPgpPath = os.path.join( xmitStagingPath, 'pgp' )
        xmitSentPath = os.path.join( xmitStagingPath, 'sent' )
        toXmitFiles = glob.glob( os.path.join(xmitStagingPath, '*.zip') )
        if ( len(toXmitFiles) > 0 ):
            self._getLogger().info(
                'There are %d files in the transmit staging folder to process...'
                % len(toXmitFiles) )
            today = datetime.datetime.today()
            sTest = ''
            if ASAP.devState.isDevInstance():
                sTest = 'test'
            zipFileName = '%sCRLMINNLIFE%s.zip' % (sTest, today.strftime('%Y%m%d%H%M%S'))
            pgpFileName = zipFileName + '.pgp'
            CRLUtility.CRLZIPFiles( os.path.join(xmitStagingPath, '*.*'),
                                    os.path.join(xmitZipPath, zipFileName),
                                    True )
            iRet = CRLUtility.CRLPGPEncrypt( 'ILS', MNM_REMOTE_USER,
                                             os.path.join(xmitZipPath, zipFileName),
                                             os.path.join(xmitPgpPath, pgpFileName))
            self._getLogger().debug( 'PGP encrypt returned %d.' % iRet )
            # now move the zip file to the sent folder if PGP encryption
            # was successful
            if iRet == 0 and os.path.exists( os.path.join(xmitPgpPath, pgpFileName) ):
                CRLUtility.CRLCopyFile( os.path.join(xmitZipPath, zipFileName),
                                        os.path.join(xmitSentPath, zipFileName),
                                        True, 5 )
                self._getLogger().info(
                    'PGP file %s successfully created and ready for transmission.'
                    % pgpFileName )
            else:
                fSuccess = False
                self._getLogger().error( 'Failed to PGP-encrypt file %s for MNM.'
                                        % zipFileName )
        # now FTP any unsent PGP files to MNM
        if not ASAP.devState.isDevInstance():
            pgpFiles = glob.glob( os.path.join(xmitPgpPath, '*.*') )
            for pgpFile in pgpFiles:
                fileName = os.path.basename( pgpFile )
                serverPath = './Put/' + fileName
                try:
                    pass
                    CRLUtility.CRLFTPPut( sServer, pgpFile, serverPath, 'b',
                                          sUser, sPassword )
                    CRLUtility.CRLDeleteFile( pgpFile )
                except:
                    fSuccess = False
                    self._getLogger().warn( 'Failed to FTP file %s to MNM:'
                                            % pgpFile,
                                            exc_info=True )
        return fSuccess


    
    

