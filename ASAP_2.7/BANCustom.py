"""

  Facility:         ILS

  Module Name:      BANCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2006, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for BAN for ASAP processing.
      
  Author:
      Jarrod Wild

  Creation Date:
      01-Nov-2007

  Modification History:
      5-Dec-2012      kg
           Banner Reconciliation changes
      
      04-Mar-2013     rsu   Ticket 39050
          Process SMM QCComplete(ExpressApp) contacts banreinsmm and wmpreinspp in this module.
          The contacts do not use 103's. Cases are released by SMM 1122's which are loaded
          via script SMMExpressAppLoad

      3-Jun-2013    KG     Ticket 40927
          Send 103 with the transmission
      
      28-Jun-2013   KG     Ticket 40927
          Get 103 only if Acord 103 directory is configured
          
      13-DEC-2013   RSU    Ticket 45677
          Change recon query to pull document history for all ban and wmp contacts.
      13-Jul-2015   venkatj    Ticket 48263
            Imaging system upgrade project. Master ticket 45490.
            Use a common tool ILS.Utils.DocServConnSettings.py to connect to the database

      15-Mar-2016   nelsonj    Ticket 68279
            Bugfix for __addAppToImage retry logic
"""
from __future__ import division, absolute_import, with_statement, print_function
from ILS.ASAP.Utility import ASAP_UTILITY
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler
import CRLUtility
from PIL import Image
from PIL import ImageSequence
import datetime
import ftplib
import glob
import os
import sys
import odbc
import time
from ILS import ILSIndexCreation
from .ASAPDocumentBundling import ASAPDocumentBundling
from ILS.Utils import DocServConnSettings as dsc

ILSDocumentBundling = ASAPDocumentBundling()

# Global variables
ILS_CLIENT_DOC_CONNECT_STRING = dsc.getConnString(dsc.DATABASES.ILS_CLIENT_DOCUMENTS, ASAP_UTILITY.devState.isDevInstance())
ILS_CONSENT_CONNECT_STRING = dsc.getConnString(dsc.DATABASES.ILS_CONSENT, ASAP_UTILITY.devState.isDevInstance())
ILS_QC_CONNECT_STRING = dsc.getConnString(dsc.DATABASES.ILS_QC, ASAP_UTILITY.devState.isDevInstance())
ILS_CONNECT_STRING = dsc.getConnString(dsc.DATABASES.ILS, ASAP_UTILITY.devState.isDevInstance())
try:
    CLIENT_CONN = odbc.odbc(ILS_CLIENT_DOC_CONNECT_STRING)
#    print 'Connected to CLIENT_CONN'
#    print CLIENT_CONN
except:
    CLIENT_CONN = None
try:
    CONSENT_CONN = odbc.odbc(ILS_CONSENT_CONNECT_STRING)
#    print 'Connected to CONSENT_CONN'
#    print CONSENT_CONN
except:
    CONSENT_CONN = None
try:
    QC_CONN = odbc.odbc(ILS_QC_CONNECT_STRING)
#    print 'Connected to QC_CONN'
#    print QC_CONN
except:
    QC_CONN = None
try:
    ILS_CONN = odbc.odbc(ILS_CONNECT_STRING)
#    print 'Connected to ILS_CONN'
#    print ILS_CONN
except:
    ILS_CONN = None

if ASAP_UTILITY.devState.isDevInstance():
    BAN_OUTBOX_PATH = r'\\ntsys1\ils_appl\log\test'
    BAN_BASE_RECON_DIR = r'\\ntsys1\ils_appl\data\XMIT\FTP\BAN0003\test\imaging\recon'
    BAN_RECON_CLIENT = os.path.join(BAN_BASE_RECON_DIR, 'Recon_From_Client')
    BAN_RECON_WORK = os.path.join(BAN_BASE_RECON_DIR, 'Work_Recon')
    BAN_BASE_TRACKING_DIR = r'\\ILSDFS\sys$\xmit\FTP\BAN0004\test\imaging\recon\tracking'
else:
    BAN_OUTBOX_PATH = r'\\ntsys1\ils_appl\data\xmit\email\CertifiedMail\BAN2\Work'
    BAN_BASE_RECON_DIR = r'\\ntsys1\ils_appl\data\XMIT\FTP\BAN0003\imaging\recon'
    BAN_RECON_CLIENT = os.path.join(BAN_BASE_RECON_DIR, 'Recon_From_Client')
    BAN_RECON_WORK = os.path.join(BAN_BASE_RECON_DIR, 'Work_Recon')
    BAN_BASE_TRACKING_DIR = r'\\ILSDFS\sys$\xmit\FTP\BAN0004\imaging\recon\tracking'

COLOR1 = 'aqua'
COLOR2 = 'white'


class BANTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for BAN transmission.
    """

    def _isIndexedCaseReady(self):
        fReady = False
        # first check to see if time of day is within BAN's window
        # (3am - 12:30pm, M-F)
        now = datetime.datetime.today()
        # now = datetime.datetime(2008, 12, 15, 5, 0)

        today_begin = datetime.datetime(now.year, now.month, now.day, 3, 0)
        today_end = datetime.datetime(now.year, now.month, now.day, 12, 30)
        if (today_begin <= now <= today_end and
                now.weekday() not in (CRLUtility.saturday, CRLUtility.sunday)):
            fReady = True
        if ASAP_UTILITY.devState.isDevInstance():
            fReady = True
        return fReady

    def __addToAppImage(self, newTiff, appTiff):
        fSuccess = False
        maxAttempts = 5
        try:
            iAttempt = 1
            while True:
                iRet = CRLUtility.CRLAppendTiff(newTiff, appTiff)
                if iRet != 0:
                    self._getLogger().warn('Append tiff ({newTiff!s:s} to {appTiff!s:s}) failed with code = {iRet:d}. Attempt {iAttempt:d} of {maxAttempts:d}'
                                           .format(newTiff=newTiff, appTiff=appTiff, iRet=iRet, iAttempt=iAttempt, maxAttempts=maxAttempts))
                    if iAttempt < maxAttempts:
                        time.sleep(2 ** (iAttempt - 1))
                        iAttempt += 1
                    else:
                        raise Exception('CRLAppendTiff Max Attempts Exhuasted')
                else:
                    fSuccess = True
                    break
        except Exception as exc:
            self._getLogger().warn('Failed to run append tiff program:')
            self._getLogger().warn(exc)
        return fSuccess

    def _stageIndexedCase(self):
        case = self._getCurrentCase()
        fSuccess = True
        # first remove the 103 XML file since we no longer need it
        # 3-Jun-2013 - Banner wants the 103 files sent to them ,
        self._getLogger().info('Not Deleting the 103 files')
        # #xmlFileName = case.trackingId + '.XML'
        # #if os.path.isfile(os.path.join(case.contact.acord103_dir, xmlFileName)):
        #     #CRLUtility.CRLDeleteFile(os.path.join(case.contact.acord103_dir, xmlFileName))
        fromToMoves = []
        filesToDelete = []
        fBuildApp = False

        # get 103 only if first transmit and acord 103 directory is configured
        if self._isFirstTransmit() and case.contact.acord103_dir:
            acord103Path = os.path.join(case.contact.acord103_dir, '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
            if os.path.isfile(acord103Path):
                banFileName = os.path.basename(acord103Path)
                proc103Path = os.path.join(case.contact.acord103_dir, 'processed', '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
                xmit103Path = os.path.join(case.contact.xmit_dir, banFileName)
                # Copy Acord103 to processed
                CRLUtility.CRLCopyFile(acord103Path, proc103Path, False, 5)
                # Move the Acord103 to to_banner
                CRLUtility.CRLCopyFile(acord103Path, xmit103Path, True, 5)
            else:
                fSuccess = False
                self._getLogger().warn('Failed to find ACORD 103 for case ({sid!s:s}/{trackingId!s:s}).'.format(sid=case.sid, trackingId=case.trackingId))
        # try to get doc/index pairs
        documents = case.getDocuments().values()
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            docPath = os.path.join(case.contact.document_dir, processedSubdir, doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '{docPrefix!s:s}.IDX'.format(docPrefix=docPrefix))
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                if fBuildApp:
                    filesToDelete.append(docPath)
                    filesToDelete.append(idxPath)
                    if not self.__addToAppImage(docPath, fromToMoves[0][0]):
                        raise Exception("addToAppImage failed for case {sid!s:s}/{trackingId!s:s}.".format(sid=case.sid, trackingId=case.trackingId))
                else:
                    case.contact.index.reset()
                    if case.contact.index.readFile(idxPath):
                        if case.contact.index.getValue('REQUIRE') == 'APPI':
                            fBuildApp = True
                            filesToDelete = [fileName for fileName, xmitFileName in fromToMoves]
                            fromToMoves = []
                            for fileName in filesToDelete:
                                if fileName.upper().endswith('TIF'):
                                    if not self.__addToAppImage(fileName, docPath):
                                        raise Exception("addToAppImage failed for case {sid!s:s}/{trackingId!s:s}.".format(sid=case.sid, trackingId=case.trackingId))
                        xmitDocPath = os.path.join(case.contact.xmit_dir, '{docPrefix!s:s}.tif'.format(docPrefix=docPrefix))
                        xmitIdxPath = os.path.join(case.contact.xmit_dir, '{docPrefix!s:s}.txt'.format(docPrefix=docPrefix))
                        fromToMoves.append((docPath, xmitDocPath))
                        fromToMoves.append((idxPath, xmitIdxPath))
                    else:
                        fSuccess = False
                        self._getLogger().warn('Index file {idxPath!s:s} missing for docid {documentId!s:s} (sid {sid!s:s}).'
                                               .format(idxPath=idxPath, documentId=doc.getDocumentId(), sid=case.sid))
            else:
                fSuccess = False
                self._getLogger().warn('Failed to find matching index/image pair for docid {documentId!s:s} (sid {sid!s:s}).'
                                       .format(documentId=doc.getDocumentId(), sid=case.sid))
        if fSuccess:
            if fBuildApp:
                case.contact.index.reset()
                case.contact.index.readFile(fromToMoves[1][0])
                case.contact.index.setValue('PAGES', str(len(list(ImageSequence.Iterator(Image.open(fromToMoves[0][0]))))))
                case.contact.index.writeFile(fromToMoves[1][0])
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)
            for fileName in filesToDelete:
                CRLUtility.CRLDeleteFile(fileName)
        return fSuccess

    def _transmitStagedCases(self):
        fSuccess = True
        BAN_FTP_HOSTNAME = 'Banner'
        BAN_REMOTE_USER = 'Legal & General America Operations <oper@lgamerica.com>'
        sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo(BAN_FTP_HOSTNAME)
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitPgpPath = os.path.join(xmitStagingPath, 'pgp')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')
        toXmitFiles = glob.glob(os.path.join(xmitStagingPath, '*.*'))
        if (len(toXmitFiles) > 0):
            self._getLogger().info('There are {toXmitFiles:d} files in the transmit staging folder to process...'
                                   .format(toXmitFiles=len(toXmitFiles)))
            for xmitFile in toXmitFiles:
                xmitFileName = os.path.basename(xmitFile)
                pgpFileName = '{xmitFileName!s:s}.pgp'.format(xmitFileName=xmitFileName)
                iRet = CRLUtility.CRLPGPEncrypt('ILS', BAN_REMOTE_USER, xmitFile, os.path.join(xmitPgpPath, pgpFileName))
                self._getLogger().debug('PGP encrypt returned {iRet:d}.'.format(iRet=iRet))
                # if successful, move file to zip path
                if iRet == 0 and os.path.exists(os.path.join(xmitPgpPath, pgpFileName)):
                    CRLUtility.CRLCopyFile(xmitFile, os.path.join(xmitZipPath, xmitFileName), True, 5)
                    self._getLogger().info('PGP file {pgpFileName!s:s} successfully created and ready for transmission.'
                                           .format(pgpFileName=pgpFileName))
                else:
                    fSuccess = False
                    self._getLogger().error('Failed to PGP-encrypt file {xmitFileName!s:s} for BAN.'
                                            .format(xmitFileName=xmitFileName))
            # zip files in zip path into the sent folder
            today = datetime.datetime.today()
            zipFileName = 'CRL{client_id!s:s}{today:%Y%m%d%H%M%S}.ZIP'.format(client_id=contact.client_id, today=today)
            CRLUtility.CRLZIPFiles(os.path.join(xmitZipPath, '*.*'), os.path.join(xmitSentPath, zipFileName), True)
        # now FTP any unsent PGP files to BAN
        testPath = '.'
        if ASAP_UTILITY.devState.isDevInstance():
            testPath = 'TEST'
        wpPath = ''
        if contact.client_id == 'WMP':
            wpPath = 'WP/'
        pgpFiles = glob.glob(os.path.join(xmitPgpPath, '*.*'))
        ftpServer = None
        for pgpFile in pgpFiles:
            fileName = os.path.basename(pgpFile)
            serverPath = '{testPath!s:s}/{wpPath!s:s}{fileName!s:s}'.format(testPath=testPath, wpPath=wpPath, fileName=fileName)
            try:
                if not ftpServer:
                    ftpServer = ftplib.FTP(sServer, sUser, sPassword)
                CRLUtility.CRLFTPPut(ftpServer, pgpFile, serverPath, 'b')
                self._getLogger().info('PGP file {pgpFile!s:s} successfully uploaded to Banner'.format(pgpFile=pgpFile))
                CRLUtility.CRLDeleteFile(pgpFile)
            except:
                fSuccess = False
                self._getLogger().warn('Failed to FTP file {pgpFile!s:s} to Banner:'.format(pgpFile=pgpFile))
        if ftpServer:
            ftpServer.close()
        return fSuccess


def writeHTMLHeader(filePtr, strTitle):
    strhtml = ""
    strhtml += "<table border=1 cellspacing=0>\n"
    strhtml += "   <caption>{strTitle!s:s}</caption>\n".format(strTitle=strTitle)
    strhtml += "   <tr style=\"background-color:black;color:{COLOR1!s:s};\">\n".format(COLOR1=COLOR1)
    strhtml += "      <th>Carrier</th>\n"
    strhtml += "      <th>File Name</th>\n"
    strhtml += "      <th>CRL ID (SID)</th>\n"
    strhtml += "      <th>Last Name</th>\n"
    strhtml += "      <th>First Name</th>\n"
    # strhtml += "      <th>DOB</th>\n"
    strhtml += "      <th>Last Transmit Date/Time</th>\n"
    strhtml += "   </tr>\n"
    filePtr.write(strhtml)


def writeHTMLBody(filePtr, dataRows):
    htmlrow = 0
    for carrier, xmitDate, fileName, sid, lname, fname in dataRows:
        bgcolor = COLOR1
        if (htmlrow % 2 == 0):
            bgcolor = COLOR2
        # if dob:
        #     dob = dob.strftime('%d-%b-%Y')
        # else:
        #     dob = 'Unknown'
        strhtml = ""
        strhtml += "   <tr style=\"background-color:{bgcolor!s:s};\">\n".format(bgcolor=bgcolor)
        strhtml += "        <td>{carrier!s:s}</td>\n".format(carrier=carrier)
        strhtml += "        <td>{fileName!s:s}</td>\n".format(fileName=fileName)
        strhtml += "        <td>{sid!s:s}</td>\n".format(sid=sid)
        strhtml += "        <td>{lname!s:s}</td>\n".format(lname=lname)
        strhtml += "        <td>{fname!s:s}</td>\n".format(fname=fname)
        # strhtml += "        <td>{dob!s:s}</td>\n".format(dob=dob)
        strhtml += "        <td>{xmitDate:%d-%b-%Y %H:%M:%S}</td>\n".format(xmitDate=xmitDate)
        strhtml += "   </tr>\n"
        filePtr.write(strhtml)
        htmlrow += 1


def writeHTMLFooter(filePtr):
    strhtml = "</table>"
    filePtr.write(strhtml)


def __build_BAN_ASAP_report(detailList, reportFile, sTitle):
    dataRows = []
    for contact_id, xmitDate, fileName, sid, lname, fname in detailList:
        carrier = 'Banner Life'
        if contact_id.upper().startswith('WMP'):
            carrier = 'William Penn'
        dataRows.append((carrier, xmitDate, fileName, sid, lname, fname))
    dataRows.sort()
    filePtr = open(reportFile, 'w')
    writeHTMLHeader(filePtr, sTitle)
    writeHTMLBody(filePtr, dataRows)
    writeHTMLFooter(filePtr)
    filePtr.close()


def getReconFile():
    """
    Download the recon file received from Banner.
    """
    sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo('CRLCORP2')
    ftpServerBAN = ftplib.FTP(sServer, sUser, sPassword)
    Recon_files = CRLUtility.CRLFTPList(ftpServerBAN, os.path.join(r'/banftp1/Recon', "*.*"))
    for f in Recon_files:
        localFilePath = os.path.join(BAN_RECON_WORK, f)
        origFilepath = os.path.join(BAN_RECON_CLIENT, f)
        CRLUtility.CRLFTPGet(ftpServerBAN, os.path.join(r'/banftp1/Recon', f), localFilePath)
        # Keep copy the files from Banner in another ORIGINAL folder in case something fails
        CRLUtility.CRLFTPGet(ftpServerBAN, os.path.join(r'/banftp1/Recon', f), origFilepath)

    # Delete after copying files, there was problem when the statement was put with above loop
    # so doing separately
    for f in Recon_files:
        logger.info('File full delete path {banftp1!s:s}'.format(banftp1=os.path.join(r'/banftp1' + f)))
        CRLUtility.CRLFTPDelete(ftpServerBAN, r'/banftp1/Recon/' + f)


def setReconDateForNonASAPDocs(tifName):
    """
    If a DSBI entry exists for tifName, set its dsbi_reconcile_date to current_timestamp.
    """
    query = """
        select count(*)
        from doc_serv_bundle_image
        where dsbi_image_file = '{tifName!s:s}'
    """.format(tifName=tifName)
    upd = """
        update doc_serv_bundle_image
        set dsbi_reconcile_date = current_timestamp
        where dsbi_image_file = '{tifName!s:s}'
    """.format(tifName=tifName)
    cursor = ILS_CONN.cursor()
    cursor.execute(query)
    data = cursor.fetchone()
    count = 0
    if (data):
        count = data[0]
    if (count > 0):
        logger.debug('Updating existing DSBI entry for {tifName!s:s}'.format(tifName=tifName))
        cursor.execute(upd)
    cursor.close()


def findDocType(tifName):
    """
    Find the document type based on the facility id.
    """
    query = """
        SELECT dsbi_facility_id
        FROM doc_serv_bundle_image
        WHERE lower(dsbi_image_file) = '{tifName!s:s}'
        """.format(tifName=tifName.lower())
    cursor = ILS_CONN.cursor()
    cursor.execute(query)
    data = cursor.fetchone()
    cursor.close()
    if data:
        facility, = data
        if facility.lower() == 'bad':
            return 'clientdoc'
        elif facility.lower() == 'bac':
            return 'consent'
        else:
            return 'paramed'


def runReconciliation():
    """
    Compare the reconciliation files and find the images that have not been reconciled.
    """
    docFactory = ASAP_UTILITY.getASAPDocumentFactory()
    caseFactory = ASAP_UTILITY.getASAPCaseFactory()
    # Make a list of files from each recon and compare them
    CRLReconList = []
    BANReconList = []
    BANfiles = glob.glob(os.path.join(BAN_RECON_WORK, 'CRL*.txt'))
    xmitConfig = ASAP_UTILITY.getXmitConfig()
    asapcursor = xmitConfig.getCursor(xmitConfig.DB_NAME_XMIT)

    for banReconFile in BANfiles:
        filePtr = open(banReconFile, 'r')
        dataLines = filePtr.readlines()
        filePtr.close()
        if len(dataLines) == 0:
            logger.info('Empty Recon File received')
            continue
        else:
            line1 = dataLines[0]
            if line1.strip() == '""':
                logger.info('Empty Recon File received')
                continue
        for line in dataLines:
            try:
                if (line.find('TIF') > 0) or (line.find('tif') > 0):
                    filename = line.split('.')[0] + '.TIF'
                    BANReconList.append(filename.upper())
                    # Add the Reconcile record in the asap_document_history table
                    doc = docFactory.fromFileName(filename)
                    # If ASAP Case
                    if doc:
                        docId = doc.getDocumentId()
                        # Reconcile this document in asap_document_history

                        # Get the sid, contact_id
                        sGetValues = """
                            select sid, contact_id
                            from asap_document_history
                            where actionitem = 'transmit'
                            and documentid = '{docId!s:s}'
                            """.format(docId=docId)
                        asapcursor.execute(sGetValues)
                        recs = asapcursor.fetch()
                        asapcursor.rollback()
                        sid, contact_id = recs[0]
                        sInsert = '''
                            insert into asap_document_history(sid, documentid, contact_id, actionitem, actiondate)
                            values ('{sid!s:s}', {docId:d}, '{contact_id!s:s}', 'reconcile', current_timestamp)
                            '''.format(sid=sid, docId=docId, contact_id=contact_id)
                        # #iRet = docHistory.trackDocument( doc, docHistory.ACTION_RECONCILE )
                        asapcursor.execute(sInsert)
                        asapcursor.commit()
                    else:
                        # This document not found in ASAP then it must be non ASAP
                        # Add the reconcile date for this document in dbo.Doc_Serv_Bundle_Image
                        setReconDateForNonASAPDocs(filename)
            except:
                logger.warn('Error occured while Reconciling Images received from the Banner')

    CRLfiles = glob.glob(os.path.join(BAN_RECON_WORK, 'recon*.log'))

    for crlReconFile in CRLfiles:
        crlfilePtr = open(crlReconFile, 'r')
        crldataLines = crlfilePtr.readlines()
        crlfilePtr.close()
        for line in crldataLines:
            CRLReconList.append(line.upper().strip())

    NonTransmittedFiles = []
    # Compare the entries in the two files
    for crlfile in CRLReconList:
        if crlfile.strip() not in BANReconList:
            # List of files not received by Banner
            NonTransmittedFiles.append(crlfile)

    BANExtraFiles = []
    for banfile in BANReconList:
        if banfile.strip() not in CRLReconList:
            # List of files not received by Banner
            BANExtraFiles.append(banfile)

    paramedFiles = []
    nonexistingFiles = []
    NonTransmittedFilesASAP = []
    NonTransmittedFilesDocConsent = []
    # Set the files to retransmit either through ASAP or NON ASAP route.
    for eachfile in NonTransmittedFiles:
        doc = docFactory.fromFileName(eachfile)
        if doc:  # ASAP
            cursor = QC_CONN.cursor()
            # #pageId = eachfile.split('.')[0]
            # #pageId = pageId.lstrip('0')
            # #docId = ILSDocumentBundling.getDocumentIdFromPageId(pageId, QC_CONN)
            # print 'pageid = ' + str(pageId) + ', docid = ' + str(docId)

            docId = doc.getDocumentId()

            ILSIndexCreation.setExportFieldForDocId(logger,
                                                    cursor,
                                                    docId,
                                                    'value_5',
                                                    ILSIndexCreation.ILS_K_EXPORTED_NO)
            cursor.close()
            # Set the ACORD 103 to retrieve
            sGetValues = """
                select sid, contact_id
                from asap_document_history
                where actionitem = 'transmit'
                and documentid = '{docId!s:s}'
                """.format(docId=docId)
            asapcursor.execute(sGetValues)
            recs = asapcursor.fetch()
            asapcursor.rollback()
            sid, contact_id = recs[0]
            case = caseFactory.fromSid(sid)

            if case.contact.acord103_dir:
                store = ASAP_UTILITY.getAcord103Store()

                acordRecs = store.getByTrackingId(case.trackingId)

                if acordRecs:
                    acordRec = acordRecs[0]
                    store.setToRetrieve(acordRec)
                    logger.info('Acord set to retrieve for {trackingId!s:s}'.format(trackingId=case.trackingId))
            logger.info('Retransmitting Banner Document id {docId:d} for tracking ID: {trackingId!s:s}'
                        .format(docId=docId, trackingId=case.trackingId))
            NonTransmittedFilesASAP.append(eachfile)
        else:
            # Find what kind of document
            doctype = findDocType(eachfile)
            if doctype == 'paramed':
                paramedFiles.append(eachfile)
                break
            pageId = eachfile[1:9]
            pageId = pageId.lstrip('0')
            if doctype == 'clientdoc':
                clientCursor = CLIENT_CONN.cursor()
                docId = ILSDocumentBundling.getDocumentIdFromPageId(pageId, CLIENT_CONN)
                if not docId:
                    nonexistingFiles.append(eachfile)
                else:
                    ILSIndexCreation.setExportFieldForDocId(logger,
                                                            clientCursor,
                                                            docId,
                                                            'value_4',
                                                            ILSIndexCreation.ILS_K_EXPORTED_NO)
                clientCursor.close()
                NonTransmittedFilesDocConsent.append(eachfile)
            elif doctype == 'consent':
                conCursor = CONSENT_CONN.cursor()
                docId = ILSDocumentBundling.getDocumentIdFromPageId(pageId, CONSENT_CONN)
                if not docId:
                    nonexistingFiles.append(eachfile)
                else:
                    ILSIndexCreation.setExportFieldForDocId(logger,
                                                            conCursor,
                                                            docId,
                                                            'value_4',
                                                            ILSIndexCreation.ILS_K_EXPORTED_NO)
                conCursor.close()
                NonTransmittedFilesDocConsent.append(eachfile)
            else:
                nonexistingFiles.append(eachfile)

    # Send emails about the non transmitted files
    sMessage = "The following files were not reconciled and have been setup to transmit again:"
    sMessage += " \n ASAP Files \n "
    for ntasap in NonTransmittedFilesASAP:
        sMessage += ntasap + "\n"
    sMessage += " \n Client Documents and Consents: \n "
    for ntdocs in NonTransmittedFilesDocConsent:
        sMessage += ntdocs + "\n"
    sMessage += " \n Smart paramed / GoParamed Image Files: \n "
    for ntParamed in paramedFiles:
        sMessage += ntParamed + '\n'
    sMessage += "\n The following files were not found in our ASAP/ Non-ASAP system: \n"
    for filenotfound in nonexistingFiles:
        sMessage += filenotfound + "\n"
    sMessage += "\n The following files are included in the Banner recon file but not in CRL recon File, please see: \n"
    for banExtra in BANExtraFiles:
        sMessage += banExtra + "\n"
    sAddress = "ilsprod@crlcorp.com"
    # #sAddress  = "gandhik@crlcorp.com"
    sSubject = " CRL - Banner / William Penn Reconciliation for all documents on {reconDate:%d-%b-%Y}".format(reconDate=reconDate)
    # #CRLUtility.CRLSendEMail( sAddress, sMessage, sSubject,
    #                         #'gandhik@crlcorp.com', '',
    #                         #'gandhik@crlcorp.com')
    CRLUtility.CRLSendEMail(sAddress, sMessage, sSubject,
                            'ilsprod@crlcorp.com', '',
                            'ilsprod@crlcorp.com')
    # Delete files in work_recon folder
    workreconfiles = glob.glob(os.path.join(BAN_RECON_WORK, '*.*'))
    for workreconfile in workreconfiles:
        CRLUtility.CRLDeleteFile(workreconfile)


def BANRecon(reconDate=datetime.datetime.today()):
    """
    Build recon file to transmit to Banner.
    """
    # Download the recon file that we have received from Banner
    getReconFile()
    runReconciliation()
    logger = ASAP_UTILITY.asapLogger
    # today = datetime.datetime.today()
    # today = datetime.datetime(2008, 3, 14)
    config = ASAP_UTILITY.getXmitConfig()
    docFactory = ASAP_UTILITY.getASAPDocumentFactory()
    docHistory = ASAP_UTILITY.getASAPDocumentHistory()
    caseFactory = ASAP_UTILITY.getASAPCaseFactory()
    reconContact = config.getContact('BAN', 'SLQ', 'APPS')
    if reconContact:
        reconStagingPath = os.path.join(
            os.path.dirname(reconContact.document_dir),
            'recon')
        rawdata = []
        logger.info('Building recon file...')
        sQuery = '''
            select contact_id, max(actiondate), sid, last_name, first_name, documentid
            from {TABLE_DOCUMENT_HISTORY!s:s} with (nolock)
            inner join ils_qc..casemaster with (nolock) on sid = sampleid
            where (contact_id like 'ban%' or contact_id like 'wmp%')
            and actionitem = '{ACTION_TRANSMIT!s:s}'
            and actiondate between '{reconDate:%d-%b-%Y} 00:00' and '{reconDate:%d-%b-%Y} 15:00'
            group by contact_id, sid, last_name, first_name, documentid
            order by contact_id, max(actiondate)    
            '''.format(TABLE_DOCUMENT_HISTORY=docHistory.TABLE_DOCUMENT_HISTORY,
                       ACTION_TRANSMIT=docHistory.ACTION_TRANSMIT,
                       reconDate=reconDate)
        cursor = config.getCursor(config.DB_NAME_XMIT)
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        detailList = []
        if recs:
            docList = []
            detailMap = {}
            for contact_id, xmitDate, sid, lname, fname, docid in recs:
                if xmitDate:
                    xmitDate = datetime.datetime.fromtimestamp(xmitDate)
                doc = docFactory.fromDocumentId(docid)
                if doc:
                    docList.append(doc)
                    detailMap[(sid, docid)] = (contact_id, xmitDate, lname, fname)
            cases = caseFactory.casesForDocuments(docList)
            for case in cases:
                docList = case.getDocuments().values()
                appDocList = docList
                for doc in docList:
                    if reconContact.docTypeNameMap[doc.getDocTypeName()] == 'APPI':
                        appDocList = [doc]
                        break
                for doc in appDocList:
                    rawdata.append(doc.fileName)
                    contact_id, xmitDate, lname, fname = detailMap[(case.sid, doc.getDocumentId())]
                    detailList.append((contact_id, xmitDate, doc.fileName, case.sid, lname, fname))
        if detailList:
            sTitle = "CRL - Banner Life and William Penn ASAP Transmissions for {reconDate:%d-%b-%Y}".format(reconDate=reconDate)
            reportFile = os.path.join(BAN_OUTBOX_PATH, 'Banner_WPenn_ASAPDailyList_{reconDate:%d%b%Y}.xls'.format(reconDate=reconDate))
            __build_BAN_ASAP_report(detailList, reportFile, sTitle)
        from ILS import BANTransmitImages
        if os.path.exists(BANTransmitImages.BAN_TRACKING_FILE):
            ptr = open(BANTransmitImages.BAN_TRACKING_FILE, 'r')
            trackingLines = ptr.readlines()
            ptr.close()
            rawdata += [line.strip() for line in trackingLines]
        BANSmartParamed = os.path.join(BAN_BASE_TRACKING_DIR, 'smartparamedtracking.dat')
        BANGoParamed = os.path.join(BAN_BASE_TRACKING_DIR, 'goparamedtracking.dat')
        BANQCCompletePassthru = os.path.join(BAN_BASE_TRACKING_DIR, 'qccomplete.dat')

        if os.path.exists(BANSmartParamed):
            sptr = open(BANSmartParamed, 'r')
            strackingLines = sptr.readlines()
            sptr.close()
            rawdata += [line.strip() for line in strackingLines]
        if os.path.exists(BANGoParamed):
            gptr = open(BANGoParamed, 'r')
            gtrackingLines = gptr.readlines()
            gptr.close()
            rawdata += [line.strip() for line in gtrackingLines]
        if os.path.exists(BANQCCompletePassthru):
            gptr = open(BANQCCompletePassthru, 'r')
            gtrackingLines = gptr.readlines()
            gptr.close()
            rawdata += [line.strip() for line in gtrackingLines]

        if rawdata:
            reconFile = os.path.join(reconStagingPath,
                                     'recon_{reconDate:%Y%m%d%H%M%S}.log'.format(reconDate=reconDate))
            reconPtr = open(reconFile, 'w')
            reconPtr.write('\n'.join(rawdata))
            reconPtr.close()
            logger.info('Recon file {reconFile!s:s} written.'.format(reconFile=reconFile))
            if os.path.exists(BANTransmitImages.BAN_TRACKING_FILE):
                CRLUtility.CRLDeleteFile(BANTransmitImages.BAN_TRACKING_FILE)
            if os.path.exists(BANSmartParamed):
                CRLUtility.CRLDeleteFile(BANSmartParamed)
            if os.path.exists(BANGoParamed):
                CRLUtility.CRLDeleteFile(BANGoParamed)
            if os.path.exists(BANQCCompletePassthru):
                CRLUtility.CRLDeleteFile(BANQCCompletePassthru)

            # send email to Johnny Maddox at Banner with attached recon file
            sMessage = ""
            sMessage += "Please find attached today's reconciliation file listing the "
            sMessage += "document image files transmitted by CRL to Banner Life on this day."
            sAddress = "jmaddox@lgamerica.com"
            # sAddress  = "gandhik@crlcorp.com"
            sSubject = "CRL - Banner Life Reconciliation File for {reconDate:%d-%b-%Y}".format(reconDate=reconDate)
            CRLUtility.CRLSendEMail(sAddress, sMessage, sSubject,
                                    'ilsprod@crlcorp.com', '',
                                    'ilsprod@crlcorp.com', reconFile)
            # Copy the recon File in the CRL_Recon folder
            crlreconfile = os.path.join(BAN_RECON_WORK, os.path.basename(reconFile))
            CRLUtility.CRLCopyFile(reconFile, crlreconfile, False, 5)
    else:
        logger.error('Recon contact not configured.')


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        arg = ''
        # arg = 'recon'
        if len(sys.argv) > 1:
            arg = sys.argv[1]
        if arg == 'recon':
            reconDate = datetime.datetime.today()
            if len(sys.argv) > 2:
                reconDate = CRLUtility.ParseStrDate(sys.argv[2], True)
            BANRecon(reconDate)
        else:
            logger.warn('Argument(s) not valid. Valid arguments:')
            logger.warn('recon [yyyy-mm-dd]')
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error')
