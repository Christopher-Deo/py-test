"""

  Facility:         ILS

  Module Name:      INGCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2006, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for ING for ASAP processing.
      
  Author:
      Jarrod Wild

  Creation Date:
      13-Nov-2006

  Modification History:
      23-Aug-2010 mayd
        Added ING INTQ into map ING_CONTACT_AGENCY_MAP
      1-Apr-20111 mayd
        Added ING EFBA into map ING_CONTACT_AGENCY_MAP
      11-Apr-2011  gandhik
        Added ING MIE into map ING_CONTACT_AGENCY_MAP
      19-JUN-2012  rsu     Ticket # 33016
        Added ING DBS and ING PHI into map ING_CONTACT_AGENCY_MAP
      12-Sep-2012  kg      Ticket 35256
        ING wants us to send the documents as Duplicate where policy number starts with AD110 and are for PHI region
      09-Oct-2012  rsu      Ticket 33107
        ING recon should also check missing ING EB (NWN) documents
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
    EMAIL_ADDRESS = 'underwoodr@crlcorp.com'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'


class INGIndexHandler( ASAPIndexHandler ):
    """
    Custom handler for building indexes for ING.
    """
    def _processDerivedFields( self ):
        case = self._getCase()
        handler = self._getAcordHandler()
        sCompany = 'RLR'
        if handler:
            appJuris = handler.txList[0].getElement(
                'ACORDInsuredHolding.Policy.ApplicationInfo.ApplicationJurisdiction' )
            if appJuris:
                attrs = appJuris.getAttrs()
                tcValue = attrs.get( 'tc' )
                # 37 is the type code for New York
                if tcValue and tcValue == '37':
                    sCompany = 'RNY'
            listField = case.contact.index.getField( 'LIST' )
            if listField and listField.getSource() == listField.SRC_DERIVED:
                sListValue = 'I3'
                agentNumber = handler.txList[0].getElement(
                'ACORDAgentParty.Producer.CarrierAppointment.CompanyProducerID' )
                if agentNumber and agentNumber.value[:7] in ( '100B00U', '100B02H' ):
                    sListValue = 'I1'
                listField.setValue( sListValue )
        case.contact.index.setValue( 'COMPANY', sCompany )
        # get dict and scrub fields
        from ILS import INGTransmitImages
        #ingDict = case.contact.index.getFieldMap()
        ingMap = {}
        ingMap['POLNO'] = case.contact.index.getValue( 'POLNO' )
        ingMap['SSN'] = case.contact.index.getValue( 'SSN' )
        INGTransmitImages.scrubINGIndex( ingMap )
        case.contact.index.setValue( 'POLNO', ingMap['POLNO'] )
        case.contact.index.setValue( 'SSN', ingMap['SSN'] )
        # if document is Physical Measurement page, apply applicant's name
        # to image
        doc = self._getCurrentDocument()
        # ING wants us to send the documents as Duplicate where policy number starts with AD110 and are for PHI region
        clientType = case.contact.index.getValue( 'REQUIRE' )
        policyNum = case.contact.index.getValue( 'POLNO' )
        provider = case.contact.index.getValue( 'PROVIDER' )
        if clientType == 'SHORTAPP' and provider == 'PHIST' and policyNum[0:5] == 'AD110':
            case.contact.index.setValue('REQUIRE', 'DUPLICATE')
        if doc.getDocTypeName() == 'PHYSICAL MEASUREMENT':
            fName = case.contact.index.getValue( 'FNAME' )
            lName = case.contact.index.getValue( 'LNAME' )
            sName = fName + ' ' + lName
            imageFile = os.path.join( case.contact.document_dir,
                                      doc.fileName )
            newImageFile = os.path.join( case.contact.document_dir,
                                         'build',
                                         doc.fileName )
            retVal = CRLUtility.CRLSystem(
                r'start /w E:\Exe\CRL\tiffdll50exe\tiffdll50exe.exe' +
                ' in=%s;out=%s;pages=0-9999;format=tif/14;text=1/2/0/%s;font=10br;save=0;err=exit'
                % (imageFile, newImageFile, sName) )
            if retVal == 0 and os.path.isfile( newImageFile ):
                CRLUtility.CRLCopyFile( newImageFile, imageFile, True, 5 )
            else:
                self._getLogger().warn( 'Failed to update image file %s with return code %d.'
                                        % (imageFile, retVal) )
                return False
        return True


class INGTransmitHandler( ASAPTransmitHandler ):
    """
    Custom handler for ING transmission.
    """
    def __check103ProductCode( self, acord103Path ):
        """
        Check policy ProductCode to see if it starts with 'ROP-', and
        if so, return False to indicate it should not be sent.  Also check
        policy number to see if it starts with AD (for Admin Server).
        """
        fSuccess = False
        parser = AcordXML.AcordXMLParser()
        handler = parser.parse( acord103Path )
        if handler:
            policyNum = handler.txList[0].getElement(
                'ACORDInsuredHolding.Policy.PolNumber' )
            productCode = handler.txList[0].getElement(
                'ACORDInsuredHolding.Policy.ProductCode' )
            if policyNum.value.startswith('AD'):
                fSuccess = True
        return fSuccess
    
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
        fReady = False
        # check to see if time of day is within ING's window
        # (3am - 8:30pm)
        now = datetime.datetime.today()
        today_begin = datetime.datetime( now.year, now.month, now.day,
                                         5, 0 )
        today_end = datetime.datetime( now.year, now.month, now.day,
                                       20, 30 )
        if now >= today_begin and now <= today_end:
            fReady = True
        return fReady

    def _stageIndexedCase( self ):
        case = self._getCurrentCase()
        fSuccess = True
        fromToMoves = []
        # get 103 if this is SelectQuote
##        if case.contact.contact_id == 'ingslqapps':
        if case.contact.acord103_dir:
            acord103Path = os.path.join( case.contact.acord103_dir,
                                         '%s.XML' % case.trackingId )
            if os.path.isfile( acord103Path ):
                # if ProductCode is not ROP, allow transmit to ING
                if self.__check103ProductCode( acord103Path ):
                    testOrProd = 'P'
                    if ASAP.devState.isDevInstance():
                        testOrProd = 'T'
                    xmit103Path = os.path.join( case.contact.xmit_dir,
                                                'AS%sCRL%s103.XML'
                                                % (testOrProd, case.trackingId) )
                    fromToMoves.append( (acord103Path, xmit103Path) )
                else:
                    CRLUtility.CRLDeleteFile( acord103Path ) 
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find ACORD 103 for case (%s/%s).'
                    % (case.sid, case.trackingId) )
        # now try to get doc/index pairs
        documents = list(case.getDocuments().values())
        processedSubdir = ASAP.xmitConfig.getSetting(
            ASAP.xmitConfig.SETTING_PROCESSED_SUBDIR )
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            ingDocPrefix = 'CRL%s' % docPrefix
            docPath = os.path.join( case.contact.document_dir,
                                    processedSubdir,
                                    doc.fileName )
            idxPath = os.path.join( case.contact.index_dir, '%s.IDX' % docPrefix )
            if os.path.isfile( docPath ) and os.path.isfile( idxPath ):
                xmitDocPath = os.path.join( case.contact.xmit_dir,
                                            '%s.tif' % ingDocPrefix )
                xmitIdxPath = os.path.join( case.contact.xmit_dir,
                                            '%s.idx' % ingDocPrefix )
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
        ING_FTP_HOSTNAME = 'ING'
        ING_PUBLIC_KEY = r'\\Ntsys1\Crl_appl\Data\pgp\ils\ing.asc'
        ING_REMOTE_USER = 'Reliastar lockbox diffie'
        ING_CONTACT_AGENCY_MAP = {
            'ingslqapps' : 'SELQ',
            'ingintqapps': 'INTQ',
            'ingampcapps': 'AMPC',
            'ingipsapps' : 'IPS',
            'ingefinapps': 'EFIN',
            'ingefbaapps': 'EFBA',
            'inggrlemsi' : 'GTERM',
            'ingmieapps' : 'MIE',
            'ingmtxapps' : 'MTRX',
            'ingspecapps': 'SPEC',
            'ingctrlapps': 'ICTRL',
            'ingdbsapps' : 'DBS',
            'ingphiapps' : 'PHI'
            }
        sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo( ING_FTP_HOSTNAME )
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
            zipFileName = ''
            agencyAbbr = ING_CONTACT_AGENCY_MAP.get( contact.contact_id )
            if agencyAbbr:
                zipFileName = 'CRLING%s_%s.zip' % ( today.strftime('%Y%m%d%H%M%S'),
                                                    agencyAbbr )
            else:
                self._getLogger().warn(
                    'Contact %s does not have an agency abbreviation for the zip file name.'
                    % contact.contact_id )
                return False
            pgpFileName = zipFileName
            asapZipFile = fm.newFile( os.path.join(xmitZipPath, zipFileName), True )
            for asapFile in asapToXmitFiles:
                CRLUtility.CRLAddToZIPFile( asapFile.getFullPath(),
                                            asapZipFile.getFullPath(),
                                            False )
                fm.deleteFile( asapFile )
            iRet = CRLUtility.CRLPGPEncrypt( 'ILS', ING_REMOTE_USER,
                                             asapZipFile.getFullPath(),
                                             os.path.join(xmitPgpPath, pgpFileName) )
            self._getLogger().debug( 'PGP encrypt returned %d.' % iRet )
            # now move the zip file to the sent folder if PGP encryption
            # was successful
            if iRet == 0 and os.path.exists( os.path.join(xmitPgpPath, pgpFileName) ):
                asapSentZipFile = fm.moveFile( asapZipFile,
                                               os.path.join(xmitSentPath, asapZipFile.fileName) )
                self._getLogger().info(
                    'PGP file %s successfully created and ready for transmission.'
                    % pgpFileName )
            else:
                fSuccess = False
                self._getLogger().warn( 'Failed to PGP-encrypt file %s for ING (extracting zip to retrans folder).'
                                        % asapZipFile.fileName )
                CRLUtility.CRLUnzipFile( asapZipFile.getFullPath(), retransPath )
                fm.deleteFile( asapZipFile )
        # now FTP any unsent PGP files to ING, only if we're not in test mode
        if not ASAP.devState.isDevInstance():
            asapPgpFiles = fm.glob( os.path.join(xmitPgpPath, '*.*') )
            for asapPgpFile in asapPgpFiles:
                serverPath = '/usr/local/ftp/CRL/' + asapPgpFile.fileName
                try:
                    CRLUtility.CRLFTPPut( sServer, asapPgpFile.getFullPath(),
                                          serverPath, 'b', sUser, sPassword )
                except:
                    fSuccess = False
                    self._getLogger().warn(
                        'Failed to FTP file %s to ING (extracting original zip to retrans folder):'
                        % asapPgpFile.getFullPath(), exc_info=True )
                    CRLUtility.CRLUnzipFile( os.path.join(xmitSentPath, asapPgpFile.fileName),
                                             retransPath )
                fm.deleteFile( asapPgpFile )
        return fSuccess


def INGRecon():
    """
    Perform reconciliation of related ING documents for ASAP.
    """
    logger = CRLUtility.CRLGetLogger()
    today = datetime.datetime.today()
    config = ASAP.xmitConfig
    # use the AMPAC recon folder for the file for both AMPAC and SelectQuote,
    # just to keep things simple
    reconContact = config.getContact( 'ING', 'SLQ', 'APPS' )
    if reconContact:
        reconStagingFolder = os.path.join(
            os.path.dirname(reconContact.document_dir),
            'recon' )
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
                where (dh.contact_id like 'ing%%' or dh.contact_id like 'nwn%%')
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
                sMessage += 'not successfully transmit to ING:\n\n'
                for case in list(casedict.values()):
                    docids = str( list(case.getDocuments().keys()) )
                    sMessage += 'Sid %s, Document IDs: "%s"\n' % ( case.sid, docids )
            else:
                sMessage += 'ING reconciliation of documents related to ASAP cases '
                sMessage += 'completed successfully with no discrepancies.'
            sTitle = 'ING Reconciliation of ASAP Case Documents'
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
        if arg == 'recon':
            INGRecon()
        else:
            print('Argument(s) not valid. Valid arguments:')
            print('recon')
        logger.info( 'Time to process this pass was %s seconds.'
                     % (time.time() - begintime) )
    except:
        logger.exception( 'Error' )

