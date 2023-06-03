"""

  Facility:         ILS

  Module Name:      GEFCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2012, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for GEF for ASAP processing.
      
  Author:
      Komal Gandhi

  Creation Date:
      25-Oct-2012

  Modification History:
      
"""
from ILS import ASAP
from ILS                        import AcordXML
from ILS.ASAP.CaseFactory       import ASAPCaseFactory
from ILS.ASAP.DocumentFactory   import ASAPDocumentFactory
from ILS.ASAP.DocumentHistory   import ASAPDocumentHistory
from ILS.ASAP.IndexHandler      import ASAPIndexHandler
from ILS.ASAP.TransmitHandler   import ASAPTransmitHandler
from ILS.ASAP.FileManager       import ASAPFileManager
import CRLUtility
import datetime
import glob
import os
import re
import sys
import time

if ASAP.devState.isDevInstance():
    EMAIL_ADDRESS = 'gandhik@crlcorp.com'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'


class GEFIndexHandler( ASAPIndexHandler ):
    """
    Custom handler for building indexes for GEF.
    """
    def _processDerivedFields( self ):
        case = self._getCase()
        handler = self._getAcordHandler()
        return True


class GEFTransmitHandler( ASAPTransmitHandler ):
    """
    Custom handler for GEF transmission.
    """    
    def _preStage( self ):
        fSuccess = True
        print('In GEF Module')
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join( xmitStagingPath, 'zip' )
        xmitPgpPath = os.path.join( xmitStagingPath, 'pgp' )
        reviewPath = os.path.join( xmitStagingPath, 'review' )
        retransPath = os.path.join( xmitStagingPath, 'retrans' )
        fm = ASAPFileManager( contact )
        try:
            # if there are any files here, they were left behind when a previous
            # run failed to complete, so move them to the reviewPath folder
#            toXmitFiles = glob.glob( os.path.join(xmitStagingPath, '*.*') )
            toXmitFiles = fm.glob( os.path.join(xmitStagingPath, '*.*') )
            for xmitFile in toXmitFiles:
                CRLUtility.CRLCopyFile( xmitFile.getFullPath(),
                                        os.path.join(reviewPath, xmitFile.fileName),
                                        True, 5 )
            if len( toXmitFiles ) > 0:
                self._getLogger().error(
                    'Files were left behind in ' +
                    '%s from a previous run and have been moved to the review subfolder.'
                    % xmitStagingPath )
            # move any PGP-encrypted zip files to reviewPath folder
#            pgpFiles = glob.glob( os.path.join(xmitPgpPath, '*.*') )
            pgpFiles = fm.glob( os.path.join(xmitPgpPath, '*.*') )
            for pgpFile in pgpFiles:
                CRLUtility.CRLCopyFile( pgpFile.getFullPath(),
                                        os.path.join(reviewPath, pgpFile.fileName),
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
        fReady = True
        # check to see if time of day is between 3PM and 4PM or 6PM and 7PM
        # We will not be sending cases between these times as Genworth resular processing is running
        # (6am - 
        case = self._getCurrentCase()
        xmitDir = case.contact.xmit_dir
        now = datetime.datetime.today()
        today_begin_3PM = datetime.datetime( now.year, now.month, now.day,
                                         15, 0 )
        today_end_4PM = datetime.datetime( now.year, now.month, now.day,
                                       16, 0 )
        today_begin_6PM = datetime.datetime( now.year, now.month, now.day,
                                         18, 0 )
        today_end_7PM = datetime.datetime( now.year, now.month, now.day,
                                       19, 0 )
        if (( now >= today_begin_3PM and now <= today_end_4PM ) or
           ( now >= today_begin_6PM and now <= today_end_7PM )):
            self._getLogger().info(
                'Not Sending cases as it is between 3-4 PM or 6-7 PM' )
            fReady = False
        return fReady
    
    def _stageIndexedCase( self ):
        case = self._getCurrentCase()
        fSuccess = True
        fromToMoves = []
        # now try to get doc/index pairs
        documents = list(case.getDocuments().values())
        processedSubdir = ASAP.xmitConfig.getSetting(
            ASAP.xmitConfig.SETTING_PROCESSED_SUBDIR )
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            gefDocPrefix = 'CL000000%s' % docPrefix.upper()
            docPath = os.path.join( case.contact.document_dir,
                                    processedSubdir,
                                    doc.fileName )
            idxPath = os.path.join( case.contact.index_dir, '%s.IDX' % docPrefix )
            if os.path.isfile( docPath ) and os.path.isfile( idxPath ):
                xmitDocPath = os.path.join( case.contact.xmit_dir,
                                            '%s.tif' % gefDocPrefix )
                xmitIdxPath = os.path.join( case.contact.xmit_dir,
                                            '%s.ndx' % gefDocPrefix )
                fromToMoves.append( (docPath, xmitDocPath) )
                fromToMoves.append( (idxPath, xmitIdxPath) )
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid %s (sid %s).'
                    % (doc.getDocumentId(), case.sid) )
        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile( fromPath, toPath, True, 5 )
        return fSuccess

    def _transmitStagedCases( self ):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join( xmitStagingPath, 'zip' )
        xmitPgpPath = os.path.join( xmitStagingPath, 'pgp' )
        xmitSentPath = os.path.join( xmitStagingPath, 'sent' )
        retransPath = os.path.join( xmitStagingPath, 'retrans' )
        fm = ASAPFileManager( contact )
        asapToXmitFiles = fm.glob( os.path.join(xmitStagingPath, '*.*') )
        if ( len(asapToXmitFiles) > 0 ):
            self._getLogger().info(
                'There are %d files in the transmit staging folder to process...'
                % len(asapToXmitFiles) )
            today = datetime.datetime.today()
            # Move the files to the Genworth regular Image transmission
            if ASAP.devState.isDevInstance():
                work_path = r'\\ntsys1\ils_appl\data\XMIT\FTP\GEF0009\Test\Work'
            else:
                work_path = r'\\ntsys1\ils_appl\data\XMIT\FTP\GEF0009\Work'
            for asapFile in asapToXmitFiles:
                try:
                    destFile = os.path.join(work_path,os.path.basename(asapFile.getFullPath()).upper())
                    print('DestFile' + destFile)
                    # Copy the files to the Genworth regular transmission
                    CRLUtility.CRLCopyFile(asapFile.getFullPath(), destFile)
                    self._getLogger().info( 'Copied %s file to Genworth Work Folder...' % asapFile.getFullPath() )
                    if ASAP.devState.isDevInstance():
                        reconInbox = r'\\ntsys1\ils_appl\data\XMIT\FTP\GEF0009\Test\Recon\Inbox'
                    else:
                        reconInbox = r'\\ntsys1\ils_appl\data\XMIT\FTP\GEF0009\Recon\Inbox'
                    #Copy the .ndx files with extension .idx for reconciliation of Genworth process.
                    
                    # Zip the files and store in sent location for records
                    zipFileName = 'CRLGEFSLQ%s.zip' % today.strftime('%Y%m%d%H%M%S')
                    zipFilePath = os.path.join(xmitSentPath,zipFileName)
                    CRLUtility.CRLAddToZIPFile( asapFile.getFullPath(),zipFilePath,True )
                except:
                    fSuccess = False
                    destFile = os.path.join(retransPath,os.path.basename(asapFile.getFullPath()))
                    CRLUtility.CRLCopyFile(asapFile.getFullPath(), destFile, True)
                    self._getLogger().info( 'Moved %s file to Genworth Retrans Folder...' % asapFile.getFullPath() )
        return fSuccess
    
    
if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        print('In GEF Module')
        arg = ''
        logger.info( 'Time to process this pass was %s seconds.'
                     % (time.time() - begintime) )
    except:
        logger.exception( 'Error' )

