"""

  Facility:         ILS

  Module Name:      NWNCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2012, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for NWN (ING EB group) for ASAP processing.
      - there are no 103's for NWN, so index fields will be pulled from the 121 instead of the 103
      - include lab report image and index if LIMS sample has reported, and this is a full
        case transmit, or a partial case transmit with a labslip
      - script ASAP_AnalyzeUpdatedLabReports will be run daily outside of ASAP_Main to 
        restage cases with updated lab reports so they can be transmitted via NWNCustom    
      
  Author:
      Robin Underwood

  Creation Date:
      05-Sep-2012

  Modification History:
      20-Mar-2018      Manjusha    SCTASK0017613
         Added NWN Recon function (ING Recon function from ING Custom.py)
      09-Apr-2018      Manjusha    SCTASK0017613
         Updated the script to OldApphub version.
      26-Apr-2018      Manjusha    SCTASK0017656
         Updated prefix for Labs from CRL to CRL_L_.

"""
from ILS import ASAP
from ILS                        import AcordXML
from ILS.ASAP.CaseFactory       import ASAPCaseFactory
from ILS.ASAP.DocumentFactory   import ASAPDocumentFactory
from ILS.ASAP.DocumentHistory   import ASAPDocumentHistory
from ILS.ASAP.IndexHandler      import ASAPIndexHandler
from ILS.ASAP.TransmitHandler   import ASAPTransmitHandler
from ILS.ASAP.FileManager       import ASAPFileManager
from ILS.ASAP.Utility           import ASAPUtility
import CRLUtility
import datetime
import glob
import os
import re
import sys
import time

if ASAP.devState.isDevInstance():
    EMAIL_ADDRESS = 'Manjusha.Krishnan@crlcorp.com'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'


class NWNIndexHandler( ASAPIndexHandler ):
    """
    Custom handler for building indexes for NWN.
    """

    def _postProcessIndex( self ):
        """
        Build image and index for NWN lab report.
        Must be a full transmit or a partial transmit with a labslip. And the LIMS
        sample must have transmitted.
        """        

        case = self._getCase()
                            
        try: 
            if not self.isReadyToBuildLabReport(case):
                #no need to build lab report and index
                return True

            #build image from text report
            xmitDir = case.contact.xmit_dir
            txtReport = os.path.join( os.path.dirname(xmitDir), 'reports', '%s.txt' % case.sid )
            CRLUtility.CRLOasisTextReportForSid(case.sid, 'ILS', txtReport)         
            s = glob.glob(txtReport[:-4] + '.*')
            if not s:
                self._getLogger().warn('Failed to build lab report image for sid %s' % case.sid)
                return False

            nwnFolderMap = {
                'nwnacsasmm': 'NWN\NWN_ACSA',
                'nwnarlismm': 'NWN\NWN_ARLI',
                'nwnalnysmm': 'NWN\NWN_ALNY',
                'nwnrlismm' : 'NWN\NWN_RLI',
                'nwnrlnysmm': 'NWN\NWN_RLNY',
                'nwnvrlismm': 'NWN\NWN_VRLI',
                'nwnvrnysmm': 'NWN\NWN_VRNY'
            }

            fError, pageCount = self.buildLabReport(case.sid, nwnFolderMap.get(case.contact.contact_id)) 

            if fError:
                self._getLogger().warn('Unable to process lab report for %s: ' % case.sid, exc_info=True )        
                    
            self.buildLabReportIndex( case, pageCount )
            
            #remove txt lab report from reports/processed folder
            txtReport = os.path.join( os.path.join(os.path.dirname(xmitDir), 'reports'), 'processed' ,'%s.txt' % case.sid )
            s = glob.glob(txtReport)
            if s:
                CRLUtility.CRLDeleteFile( txtReport ) 
                    
        except:
            self._getLogger().warn('PostProcessIndex failed with exception: ', exc_info=True)

        return True

    def isReadyToBuildLabReport( self, case ):
        """
        Check whether lab report is needed for this case transmission.
        Must be full transmit or partial transmit with a labslip. And the LIMS
        sample must have a transmit date.
        """

        rec = ASAPUtility().getLIMSSampleFieldsForSid( case.sid, ('transmit_date',) )
        if not rec:
            #no sid in LIMS, so no lab report
            return False
        else:
            sid, transmit_date = rec
            if not transmit_date:
                #sid has not transmitted yet
                return False

        if self._isFullTransmit():
            return True
                   
        docList = list(case.getDocuments().values())
        for doc in docList:
            if doc.getDocTypeName() == 'LAB RECEIPT/URINE/BLOOD TEST':
                #lab slip is getting transmitted, so also transmit lab report
                return True
                
        return False        

    def buildLabReport( self, sid, NWNFolder ):
        """
        Convert txt file to image file.
        """
        fError = False
        pageCount = 0

        # constants that differ between prod and dev
        sTest = ''
        if ASAP.devState.isDevInstance():
            sTest = 'test'
            
        NWN_IMAGING_DIR = os.path.join( r'\\ntsys1\ils_appl\data\xmit\ftp', NWNFolder, sTest, 'imaging' )
        #NWN_IMAGING_DIR = os.path.join( r'\\ntsys1\ils_appl\data\xmit\ftp\NWN\NWN_RLI\test\imaging')

        reportStagingPath = os.path.join( NWN_IMAGING_DIR, 'reports' )
        processedPath = os.path.join( reportStagingPath, 'processed' )
        errorPath = os.path.join( reportStagingPath, 'error' )

        rptFiles = glob.glob( os.path.join(reportStagingPath, '%s.txt' % sid) )    
        for rptFile in rptFiles:
            fMoveToError = False
            sidText = os.path.splitext(os.path.basename(rptFile))[0]
            tif = os.path.join( reportStagingPath, sidText + '.tif' )
        
            #reduce font size to avoid blank pages in the lab report
            pageCount, exitCode = CRLUtility.CRLTextToTiffWithPageCount( rptFile, tif, sCanvas = '8.5/11/200', sFontSize = '10.7b' )

            if os.path.isfile(tif):
                # now move files to processed folder
                CRLUtility.CRLCopyFile(rptFile,
                                       os.path.join(processedPath, os.path.basename(rptFile)),
                                       True, 5)
            else:
                fError = True
                fMoveToError = True
                self._getLogger().warn('Failed to create TIF image for text report %s.' % rptFile)

            # report file should have been processed, so if any are left, they are unmatched
            sidRptFile = glob.glob(os.path.join(reportStagingPath, '*%s*.txt' % sid))
            if sidRptFile:
                fError = True
                fMoveToError = True
                self._getLogger().warn('Text report for %s was left in %s. Moving to error sub-directory' %
                                       (sid, reportStagingPath))

            if fMoveToError:
                CRLUtility.CRLCopyFile(rptFile, os.path.join(errorPath, os.path.basename(rptFile)), True, 5)

        return fError, pageCount

                        
    def buildLabReportIndex( self, case, pageCount ): 
        """
        Build the index for the lab report using another index file for the case as a model
        """

        delim = '='
        doctypefield = 'REQUIRE'
        pagenumfield = 'PAGES'               
        labworkdoctype = 'LABWORK' 
                                
        idxPaths = self._getIndexPaths()
        if idxPaths and os.path.isfile( idxPaths[0] ):
            idxFile = idxPaths[0]                    
            ptr = open( idxFile, 'r' )
            lines = ptr.readlines()
            ptr.close()
                
            # this section of code to build the fieldlist must be done because
            # some field values might have commas in them, and they need to be
            # identified and joined with their erroneously separated prior field
            fieldlist = []                
            for line in lines:
                line = line.strip()
                if line.find(doctypefield) < 0 and line.find(pagenumfield) < 0:
                    fieldlist.append(line)
                elif line.find(doctypefield) >= 0:
                    # change doctype to labwork
                    fieldlist.append(doctypefield + delim + labworkdoctype)
                elif line.find(pagenumfield) >= 0:
                    #update correct number of pages
                    if not pageCount or pageCount < 1:
                        pageCount = "1"
                    else:
                        pageCount = str(pageCount)

                    fieldlist.append(pagenumfield + delim + pageCount)

            # write index file for lab report
            idxFile = os.path.join(case.contact.index_dir, '%s.idx' % case.sid)
            ptr = open(idxFile, 'w')
            for line in fieldlist:
                ptr.write( "%s\n" % line )
                    
            ptr.close()

                    
                    
class NWNTransmitHandler( ASAPTransmitHandler ):
    """
    Custom handler for NWN transmission.
    """
    def __check103ProductCode( self, acord103Path ):
        """
        No current requirements to check product code for NWN
        """
        return True
    
    def _preStage( self ):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join( xmitStagingPath, 'zip' )
        xmitPgpPath = os.path.join( xmitStagingPath, 'pgp' )
        reviewPath = os.path.join( xmitStagingPath, 'review' )
        retransPath = os.path.join( xmitStagingPath, 'retrans' )
        fm = ASAPFileManager( contact )
        
        try:
            toXmitFiles = fm.glob( os.path.join(xmitStagingPath, '*.*') )
            for xmitFile in toXmitFiles:
                CRLUtility.CRLCopyFile(xmitFile.getFullPath(),
                                       os.path.join(reviewPath, xmitFile.fileName),
                                       True, 5)
            if len(toXmitFiles) > 0:
                self._getLogger().error('Files were left behind in ' +
                                        '%s from a previous run and have been moved to the review subfolder.' % xmitStagingPath)
            
            # move any PGP-encrypted zip files to reviewPath folder
            pgpFiles = fm.glob(os.path.join(xmitPgpPath, '*.*'))
            for pgpFile in pgpFiles:
                CRLUtility.CRLCopyFile(pgpFile.getFullPath(),
                                       os.path.join(reviewPath, pgpFile.fileName),
                                       True, 5)
            if len(pgpFiles) > 0:
                self._getLogger().error('PGP-encrypted zip files were left behind in ' +
                                        '%s from a previous run and have been moved to the review subfolder.' % xmitPgpPath)
            # now move files from the retrans folder to the xmit staging folder
            toRetransFiles = glob.glob( os.path.join(retransPath, '*.*') )
            for retransFile in toRetransFiles:
                baseFileName = os.path.basename(retransFile)
                CRLUtility.CRLCopyFile(retransFile, os.path.join(xmitStagingPath, baseFileName), True, 5)
        except:
            self._getLogger().warn('Pre-stage failed with exception: ', exc_info=True)
            fSuccess = False
        return fSuccess
    
    def _isIndexedCaseReady( self ):
        fReady = False
        # check to see if time of day is within ING's window
        # (3am - 8:30pm)
        now = datetime.datetime.today()
        today_begin = datetime.datetime(now.year, now.month, now.day, 5, 0)
        today_end = datetime.datetime(now.year, now.month, now.day, 20, 30)
        if now >= today_begin and now <= today_end:
            fReady = True
        return fReady

    def _stageIndexedCase( self ):
        case = self._getCurrentCase()
        fSuccess = True
        fromToMoves = []

        # now try to get doc/index pairs
        documents = list(case.getDocuments().values())
        processedSubdir = ASAP.xmitConfig.getSetting(ASAP.xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            ingDocPrefix = 'CRL%s' % docPrefix
            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '%s.IDX' % docPrefix)
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir,
                                           '%s.tif' % ingDocPrefix)
                xmitIdxPath = os.path.join(case.contact.xmit_dir,
                                           '%s.idx' % ingDocPrefix)
                fromToMoves.append((docPath, xmitDocPath))
                fromToMoves.append((idxPath, xmitIdxPath))
            else:
                fSuccess = False
                self._getLogger().warn('Failed to find matching index/image pair for docid %s (sid %s).' % (doc.getDocumentId(), case.sid))
        
        # move lab report and index
        ingSidDocPrefix = 'CRL_L_%s' % case.sid
        if os.path.isfile(os.path.join(os.path.dirname(case.contact.xmit_dir), 'reports', '%s.tif' % case.sid)):
            fromToMoves.append((os.path.join(os.path.dirname(case.contact.xmit_dir), 'reports', '%s.tif' % case.sid),
                                os.path.join(case.contact.xmit_dir, '%s.tif' % ingSidDocPrefix)))
            fromToMoves.append((os.path.join(os.path.dirname(case.contact.xmit_dir), 'indexes', '%s.idx' % case.sid),
                                os.path.join(case.contact.xmit_dir, '%s.idx' % ingSidDocPrefix)))
                             
        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile( fromPath, toPath, True, 5 )
        return fSuccess

    def _transmitStagedCases( self ):
        fSuccess = True
        ING_FTP_HOSTNAME = 'ING'
        ING_PUBLIC_KEY = r'\\Ntsys1\Crl_appl\Data\pgp\ils\ing.asc'
        ING_REMOTE_USER = 'Reliastar lockbox diffie'
        ING_CONTACT_AGENCY_MAP = {
            'nwnacsasmm': 'ACSA',
            'nwnarlismm': 'ARLI',
            'nwnalnysmm': 'ALNY',
            'nwnrlismm' : 'RLI',
            'nwnrlnysmm': 'RLNY',
            'nwnvrlismm': 'VRLI',
            'nwnvrnysmm': 'VRNY',
            }
        
        sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo( ING_FTP_HOSTNAME )
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitPgpPath = os.path.join(xmitStagingPath, 'pgp')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAPFileManager(contact)
        asapToXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
        if len(asapToXmitFiles) > 0:
            self._getLogger().info('There are %d files in the transmit staging folder to process...' % len(asapToXmitFiles))
            today = datetime.datetime.today()
            zipFileName = ''
            agencyAbbr = ING_CONTACT_AGENCY_MAP.get(contact.contact_id)
            if agencyAbbr:
                zipFileName = 'CRLING%s_%s.zip' % (today.strftime('%Y%m%d%H%M%S'), agencyAbbr)
            else:
                self._getLogger().warn('Contact %s does not have an agency abbreviation for the zip file name.' % contact.contact_id)
                return False
            pgpFileName = zipFileName
            asapZipFile = fm.newFile( os.path.join(xmitZipPath, zipFileName), True )
            for asapFile in asapToXmitFiles:
                CRLUtility.CRLAddToZIPFile(asapFile.getFullPath(), asapZipFile.getFullPath(), False)
                fm.deleteFile(asapFile)
            iRet = CRLUtility.CRLPGPEncrypt('ILS', ING_REMOTE_USER, asapZipFile.getFullPath(), os.path.join(xmitPgpPath, pgpFileName))
            self._getLogger().debug('PGP encrypt returned %d.' % iRet)
            # now move the zip file to the sent folder if PGP encryption was successful
            if iRet == 0 and os.path.exists(os.path.join(xmitPgpPath, pgpFileName)):
                asapSentZipFile = fm.moveFile(asapZipFile, os.path.join(xmitSentPath, asapZipFile.fileName))
                self._getLogger().info('PGP file %s successfully created and ready for transmission.'
                    % pgpFileName)
            else:
                fSuccess = False
                self._getLogger().warn('Failed to PGP-encrypt file %s for ING (extracting zip to retrans folder).' %
                                       asapZipFile.fileName)
                CRLUtility.CRLUnzipFile(asapZipFile.getFullPath(), retransPath)
                fm.deleteFile(asapZipFile)
                
        # now FTP any unsent PGP files to ING, only if we're not in test mode
        if not ASAP.devState.isDevInstance():
            asapPgpFiles = fm.glob( os.path.join(xmitPgpPath, '*.*') )
            for asapPgpFile in asapPgpFiles:
                serverPath = '/usr/local/ftp/CRL/' + asapPgpFile.fileName
                try:
                    CRLUtility.CRLFTPPut(sServer, asapPgpFile.getFullPath(), serverPath, 'b', sUser, sPassword)
                except:
                    fSuccess = False
                    self._getLogger().warn('Failed to FTP file %s to ING (extracting original zip to retrans folder):'
                        % asapPgpFile.getFullPath(), exc_info=True)
                    CRLUtility.CRLUnzipFile(os.path.join(xmitSentPath, asapPgpFile.fileName), retransPath)
                fm.deleteFile(asapPgpFile)
                
        return fSuccess


def NWNRecon():
    """
    Perform reconciliation of related NWN documents for ASAP.
    """
    logger = CRLUtility.CRLGetLogger()
    today = datetime.datetime.today()
    config = ASAP.xmitConfig
    # use the AMPAC recon folder for the file for both AMPAC and SelectQuote,
    # just to keep things simple
    reconContact = config.getContact('NWN', 'RLI', 'SMM')
    if reconContact:
        #reconStagingFolder = r'\\ntsys1\ils_appl\data\XMIT\FTP\NWN\NWN_RLI\test\imaging\recon'
        reconStagingFolder = os.path.join(
               os.path.dirname(reconContact.document_dir),
               'recon' )
        #reconProcessedFolder = r'\\ntsys1\ils_appl\data\XMIT\FTP\NWN\NWN_RLI\test\imaging\recon\processed'
        reconProcessedFolder = os.path.join(
            reconStagingFolder,
            config.getSetting(config.SETTING_PROCESSED_SUBDIR) )
        docFactory = ASAPDocumentFactory()
        caseFactory = ASAPCaseFactory()
        if not os.path.isdir( reconProcessedFolder ):
            os.makedirs( reconProcessedFolder )
        files = glob.glob( os.path.join(reconStagingFolder, '*.txt') )
        for reconFile in files:
            baseFileName = os.path.basename( reconFile )
            filePtr = open( reconFile, 'r' )
            dataLines = filePtr.readlines()
            filePtr.close()
            fError = False
            documents = []
            for line in dataLines:
                reconFields = []
                try:
                    if line.strip():
                        reconFields = line.strip().split('|')
                        clientID = reconFields[0]
                        reconDate = CRLUtility.ParseStrDate(reconFields[1], True)
                        imageFileName = reconFields[2].upper()
                        if ( imageFileName.startswith('CRL') and
                             len(imageFileName) == 15 ):
                            doc = docFactory.fromFileName( imageFileName[3:] )
                            if doc:
                                documents.append( doc )
                except:
                    logger.warn( 'Invalid entry found in file %s:\r\n%s'
                                 % (baseFileName, str(reconFields)) )
                    fError = True
            cases = caseFactory.casesForDocuments( documents )
            docHistory = ASAPDocumentHistory()
            for case in cases:
                for doc in list(case.getDocuments().values()):
                    docHistory.trackDocument( doc, docHistory.ACTION_RECONCILE )
            if fError:
                logger.error( 'There were one or more errors processing file %s.'
                              % baseFileName )
            CRLUtility.CRLCopyFile(
                reconFile,
                os.path.join(reconProcessedFolder,
                             baseFileName+'_'+today.strftime('%Y%m%d%H%M%S')),
                True, 5 )
        #
        # Get all samples that were transmitted but not reconciled,
        # only if at least one recon file was processed.
        #
        if files:
            sQuery = '''
                select dh.sid, dh.documentid, max(dh.actiondate) lastdate
                from %s dh
                where  dh.contact_id like 'nwn%%'
                and dh.actionitem = '%s'
                and dh.actiondate < '%s'
                and not exists
                (select historyid from %s dh2
                where dh2.documentid = dh.documentid and dh2.sid = dh.sid
                and dh2.contact_id = dh.contact_id
                and dh2.actionitem = '%s'
                and dh2.actiondate > dh.actiondate)
                group by dh.sid, dh.documentid
                order by dh.sid, dh.documentid
                     ''' % ( docHistory.TABLE_DOCUMENT_HISTORY,
                             docHistory.ACTION_TRANSMIT,
                             today.strftime('%d-%b-%Y'),
                             docHistory.TABLE_DOCUMENT_HISTORY,
                             docHistory.ACTION_RECONCILE )
            cursor = config.getCursor( config.DB_NAME_XMIT )
            iRet = cursor.execute( sQuery )
            recs = cursor.fetch()
            cursor.rollback()
            casedict = {}
            if recs:
                for sid, docid, lastdate in recs:
                    doc = docFactory.fromDocumentId( docid )
                    case = casedict.get( sid )
                    if doc and case:
                        case.addDocument( doc )
                    elif doc:
                        case = caseFactory.fromSid( sid )
                        if case:
                            casedict[sid] = case
                            case.addDocument( doc )
            #
            # Only when at least one recon file has been processed, send
            # an email notification of missing documents, if any.
            #
            sMessage = '\n'
            if casedict:
                sMessage += 'The following document files related to ASAP cases did '
                sMessage += 'not successfully transmit to NWN:\n\n'
                for case in list(casedict.values()):
                    docids = str( list(case.getDocuments().keys()) )
                    sMessage += 'Sid %s, Document IDs: "%s"\n' % ( case.sid, docids )
            else:
                sMessage += 'NWN reconciliation of documents related to ASAP cases '
                sMessage += 'completed successfully with no discrepancies.'
            sTitle = 'NWN Reconciliation of ASAP Case Documents'
            sAddress = EMAIL_ADDRESS
            CRLUtility.CRLSendEMail( sAddress, sMessage, sTitle,
                                     'noreply@crlcorp.com', '', '' )
    else:
        logger.error( 'Recon contact not configured.' )


    
if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        arg = ''
        #arg = 'recon'
        if len( sys.argv ) > 1:
            arg = sys.argv[1]
#RSU do recon from INGCustom?
        if arg == 'recon':
            NWNRecon()
        else:
            print('Argument(s) not valid. Valid arguments:')
            print('recon')
        logger.info( 'Time to process this pass was %s seconds.'
                     % (time.time() - begintime) )
    except:
        logger.exception( 'Error' )

