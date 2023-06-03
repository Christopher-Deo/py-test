"""

  Facility:         ILS

  Module Name:      AGLiteCustom

  Version:
      Software Version:          Python version 2.5

      Copyright 2016, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for AG ASAP Lite processing. So there is no 103 transmitted to the client ,
      only the images and indexes

  Author:
      Komal Gandhi

  Creation Date:
      29-Dec-2015

  Modification History:
      29-Dec-2015 gandhik
      Derived from the AIGCustom module

      12-Apr-2017 gandhik
      Removed decryption of the recon file

      14-MAY-2021 nelsonj
      Migration to new apphub and updating to python 2.7

"""

import DevInstance

# devState = DevInstance.devInstance(True)
devState = DevInstance.devInstance()

from ILS.ASAP.IndexHandler import ASAPIndexHandler
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler
from ILS.ASAP.Utility import ASAP_UTILITY
import CRLUtility
import datetime
import glob
import os
import time

if devState.isDevInstance():
    EMAIL_ADDRESS = 'nelsonj@crlcorp.com'
    LOG_DIR = r'E:\Test\Log\ILS\AGLiteCustom.log'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'
    LOG_DIR = r'E:\Log\ILS\AGLiteCustom.log'


class AGLiteIndexHandler(ASAPIndexHandler):
    """
    Custom handler for building indexes for AGLite.
    """

    def _processDerivedFields(self):
        """AGLite index should be
        PAGES = 1
        SUBJECT = #Name of the file without extension#
        LNAME = TESTLAST
        FNAME = TESTFIRST
        MI =
        DOB = 19500101 # checkl the format with AG
        SSN = 123456789
        CASENO = #Tracking Id#
        POLNO = #from 121#
        DOCTOR = N/S
        PROVIDER = CRLMATRIX
        REQUIRE = #Document type#
        COMPANY = NC
        BUS AREA = LIFE
        FACE AMOUNT = #from 121#
        REPLACE = N
        APPSTATE = #Any valid state abbr# ask AG
        AGNM =
        AFNM =
        ALNM =
        ASSN =
        GANM =
        LIST = NS
        BUS TYPE =
        MIBRESULT =
        #AGN Does not need BSTATE and BUSTYPE blank, but UL wants BSTATE and BUS TYPE both blank.
        """
        print('In _processDerivedFields AGLite')
        case = self._getCase()
        index = case.contact.index
        contact = case.contact.contact_id
        print(contact)
        doc = self._getCurrentDocument()
        print((doc.fileName))
        docName = doc.fileName.split('.')[0]
        print((case.contact.index.getValue('REQUIRE')))
        print(docName)
        index.setValue('SUBJECT', docName)
        print('In _processDerivedFields AGLite is done')
        return True


class AGLiteTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for AGLite transmission.
    """

    def _preStage(self):
        print('In AGLiteTransmitHandler _preStage AGLite')
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        try:
            # if there are any files here, they were left behind when a previous
            # run failed to complete, so move them to the retransPath folder
            toXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
            print(('To transmit files: ', toXmitFiles))
            for xmitFile in toXmitFiles:
                CRLUtility.CRLCopyFile(xmitFile.getFullPath(),
                                       os.path.join(retransPath, xmitFile.fileName),
                                       True, 5)
            if len(toXmitFiles) > 0:
                self._getLogger().error(
                    'Files were left behind in ' +
                    '{xmitStagingPath!s:s} from a previous run and have been moved to the retrans subfolder.'
                    .format(xmitStagingPath=xmitStagingPath))
            # the remaining zip files will be uploaded to FTP and not moved anywhere
            zipFiles = fm.glob(os.path.join(xmitZipPath, '*.*'))
            if len(zipFiles) > 0:
                self._getLogger().info(
                    'ZIP files were left behind in ' +
                    '{xmitZipPath!s:s} from a previous run and will be uploaded.'
                    .format(xmitZipPath=xmitZipPath))
            # As AG wants their images to be bundled per case so we will create separate zip files per case and
            # place them in zip Folder
            toRetransFiles = glob.glob(os.path.join(retransPath, '*.ndx'))
            indexData = []
            xmitZipPath = os.path.join(xmitStagingPath, 'zip')
            now = datetime.datetime.today()
            for retransIdxFile in toRetransFiles:
                retransFilePtr = open(retransIdxFile, 'r')
                indexData += retransFilePtr.readlines()
                retransFilePtr.close()
                tracking_id = ''
                for eachline in indexData:
                    if eachline.find('CASENO') >= 0:
                        tracking_id = eachline[9:21]
                        print(tracking_id)
                # Now we shall make individual zip files and place in zip directory to be retransmitted to the client
                # This saves time for the IT support person
                zipFileName = ''
                if contact.contact_id == 'agimtdapps':
                    zipFileName = 'CRLAGUL_{tracking_id!s:s}_{dt:%Y%m%d%H%M%S}.ZIP'.format(tracking_id=tracking_id, dt=now)
                    print(zipFileName)
                elif contact.contact_id == 'agnmtxapps':
                    zipFileName = 'CRLAGLA_{tracking_id!s:s}_{dt:%Y%m%d%H%M%S}.ZIP'.format(tracking_id=tracking_id, dt=now)
                    print(zipFileName)
                # Add this index and corresponding Tiff file to the above named zip files
                asapZipFile = os.path.join(xmitZipPath, zipFileName)
                retransTifFile = os.path.splitext(retransIdxFile)[0] + '.tif'
                print((retransTifFile, retransIdxFile))
                CRLUtility.CRLAddToZIPFile(str(retransIdxFile), asapZipFile, True)
                CRLUtility.CRLAddToZIPFile(str(retransTifFile), asapZipFile, True)

        except:
            self._getLogger().warn(
                'Pre-stage failed with exception: ', exc_info=True)
            fSuccess = False
        print('In AGLiteTransmitHandler _preStage AGLite done')
        return fSuccess

    def _isIndexedCaseReady(self):
        print('In AGLiteTransmitHandler _isIndexedCaseReady AGLite')
        fReady = False
        # first check to see if time of day is within AG's window
        # (3am - 6:30pm)
        now = datetime.datetime.today()
        today_begin = datetime.datetime(now.year, now.month, now.day,
                                        3, 0)
        today_end = datetime.datetime(now.year, now.month, now.day,
                                      18, 30)
        if today_begin <= now <= today_end:
            fReady = True
        print(('time, ', fReady))
        if fReady:
            # check lims transmit date to see if case is
            # ready to xmit
            fReady = False
            config = ASAP_UTILITY.getXmitConfig()
            case = self._getCurrentCase()
            sQuery = '''
                select transmit_date from sample
                where sid = '{sid!s:s}'
                '''.format(sid=case.sid)
            sip = config.getCursor(config.DB_NAME_SIP)
            snip = config.getCursor(config.DB_NAME_SNIP)
            if snip and sip:
                for cursor in snip, sip:
                    cursor.execute(sQuery)
                    rec = cursor.fetch(True)
                    cursor.rollback()
                    if rec and rec[0]:
                        fReady = True
                        break
                    print(('transmit, ', fReady))
        print(('Indexed case ready? ', fReady))
        print('In AGLiteTransmitHandler _isIndexedCaseReady AGLite done')
        return fReady

    def _stageIndexedCase(self):
        print('In AGLiteTransmitHandler _stageIndexedCase AGLite ')
        case = self._getCurrentCase()
        contact = self._getContact()
        fSuccess = True
        now = datetime.datetime.today()
        fromToMoves = []
        asapToZipFiles = []
        # try to get doc/index pairs
        documents = list(case.getDocuments().values())
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '{docPrefix!s:s}.IDX'.format(docPrefix=docPrefix))
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir, '{docPrefix!s:s}.tif'.format(docPrefix=docPrefix))
                xmitIdxPath = os.path.join(case.contact.xmit_dir, '{docPrefix!s:s}.ndx'.format(docPrefix=docPrefix))
                print(('\nXMITDOC & XMITIDX ', xmitDocPath, xmitIdxPath))
                asapToZipFiles.append(str(xmitDocPath))
                asapToZipFiles.append(str(xmitIdxPath))
                fromToMoves.append((docPath, xmitDocPath))
                fromToMoves.append((idxPath, xmitIdxPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid {docid:d} (sid {sid!s:s}).'
                    .format(docid=doc.getDocumentId(), sid=case.sid))
        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)

        # Zip individual cases as per AGLite requirement and naming scheme CRLAGLA_MAT000000001_datetimestamp.ZIP / CRLAGUL_%s_%s.ZIP
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        zipFileName = ''
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        asapToXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
        if contact.contact_id == 'agimtdapps':
            zipFileName = 'CRLAGUL_{tracking_id!s:s}_{dt:%Y%m%d%H%M%S}.ZIP'.format(tracking_id=case.trackingId, dt=now)
            print(zipFileName)
        elif contact.contact_id == 'agnmtxapps':
            zipFileName = 'CRLAGLA_{tracking_id!s:s}_{dt:%Y%m%d%H%M%S}.ZIP'.format(tracking_id=case.trackingId, dt=now)
            print(zipFileName)
        if zipFileName != '':
            asapZipFile = os.path.join(xmitZipPath, zipFileName)
            for asapFile in asapToXmitFiles:
                CRLUtility.CRLAddToZIPFile(str(asapFile.getFullPath()), asapZipFile, True)

            if os.path.exists(asapZipFile):
                self._getLogger().info(
                    'Zip file {asapZipFile!s:s} successfully created and ready for transmission.'
                    .format(asapZipFile=asapZipFile))
            else:
                print('no path exists')
                fSuccess = False
                self._getLogger().warn('Failed to zip files for AG Lite (moving files to retrans folder).')
                # Move the files to retrans folder for next run
                for asapFile in asapToXmitFiles:
                    CRLUtility.CRLCopyFile(str(asapFile.getFullPath()), os.path.join(retransPath, asapFile.fileName), True, 5)
                    fm.deleteFile(asapFile)
        print((case.trackingId, case.sid))
        print(('In AGLiteTransmitHandler _stageIndexedCase AGLite is done for case {trackingId!s:s} '.format(trackingId=case.trackingId)))
        return fSuccess

    def _transmitStagedCases(self):
        fSuccess = True
        print('In AGLiteTransmitHandler _transmitStagedCases september AGLite')
        CRL_FTP_SERVER = 'dmzftp.crlcorp.com'
        CRL_FTP_USER = 'crlftp'
        CRL_FTP_USER_PASSWORD = 'cat51blue'
        # AGLite wants us to send all the documents to where the ZIP Zapp files are sent.

        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        print(xmitStagingPath)
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        # now FTP all zip files AGLite
        asapZipFiles = fm.glob(os.path.join(xmitZipPath, '*.*'))
        for asapZipFile in asapZipFiles:
            if not ASAP_UTILITY.devState.isDevInstance():
                serverPath = '/agiftp1/outbox/ezapp/prod/' + asapZipFile.fileName
            else:
                serverPath = '/agitestftp1/outbox/ezapp/test/' + asapZipFile.fileName
            print(serverPath)
            try:
                CRLUtility.CRLFTPPut(CRL_FTP_SERVER, asapZipFile.getFullPath(),
                                     serverPath, 'b', CRL_FTP_USER, CRL_FTP_USER_PASSWORD)
                # Move the zip files to sent folder
                CRLUtility.CRLCopyFile(asapZipFile.getFullPath(),
                                       os.path.join(xmitSentPath, asapZipFile.fileName), True)
            except:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to FTP file {path!s:s} to AG (leaving original zip to be uploaded in the next run):'
                    .format(path=asapZipFile.getFullPath()), exc_info=True)

        print('In AGLiteTransmitHandler _transmitStagedCases AGLite is done')
        return fSuccess


def AGLiteRecon():
    """
    Perform reconciliation of related AG documents for ASAP Lite.
    """
    logger = CRLUtility.CRLGetLogger()
    today = datetime.datetime.today()
    yesterday = CRLUtility.CRLGetBusinessDayOffset(today, -1)
    config = ASAP_UTILITY.getXmitConfig()
    caseFactory = ASAP_UTILITY.getASAPCaseFactory()
    docFactory = ASAP_UTILITY.getASAPDocumentFactory()
    docHistory = ASAP_UTILITY.getASAPDocumentHistory()
    # Fetch the recon file from FTP server.
    # This is done in AIG.EZAP:ZipZappReconFileMover Apphub task as it downloads everything CrlRcn_*asaplite*.pgp
    # and our filename is CrlRcn_20161121_063013_asapliteonly.txt.
    # Its stored at \\ilsdfs\sys$\xmit\FTP\AGI_MTD\imaging\Recon
    reconContact = config.getContact('AGI', 'MTD', 'APPS')
    reconStagingPath = ''
    reconProcessedPath = ''
    if reconContact:
        reconStagingPath = os.path.join(
            os.path.dirname(reconContact.document_dir),
            'recon')
        reconProcessedPath = os.path.join(
            reconStagingPath,
            config.getSetting(config.SETTING_PROCESSED_SUBDIR))

    rawdata = []
    combinedReconFilesList = glob.glob(os.path.join(reconStagingPath, 'CrlRcn_*asapliteonly.txt'))
    for reconFile in combinedReconFilesList:
        reconPtr = open(reconFile, 'r')
        rawdata += reconPtr.readlines()
        reconPtr.close()
    logger.info('Processing reconciliation file(s)...{files!s:s}'.format(files=combinedReconFilesList))
    if not len(rawdata):
        logger.warn('Empty Recon file received for AG ASAP Lite.')
    if rawdata:
        logger.info('Processing reconciliation file(s)...')
        for row in rawdata:
            recs = row.strip().upper().split()
            logger.info('Processing record: {recs!s:s}'.format(recs=recs))
            if len(recs) > 0:
                if len(recs) != 4:
                    logger.warn('Improperly formatted record found.')
                else:
                    fileName, dateval, timeval, caseFileName = recs
                    baseFileName = fileName.upper()
                    caseFileName = caseFileName.upper()
                    # caseFileName like CRLAGLA_MAT003504168_10122016111200.ZIP
                    trackingID = (caseFileName.split('.')[0]).split('_')[1]
                    case = caseFactory.fromTrackingId(trackingID)
                    if case:
                        fileType = baseFileName.split('.')[-1]
                        if fileType == 'TIF':
                            doc = docFactory.fromFileName(baseFileName)
                            if doc:
                                logger.info('baseFileName: {baseFileName!s:s},caseFileName:{caseFileName!s:s} FILETYPE: {fileType!s:s}'
                                            .format(baseFileName=baseFileName, caseFileName=caseFileName, fileType=fileType))
                                doc.case = case
                                docHistory.trackDocument(doc, docHistory.ACTION_RECONCILE)
                    else:
                        logger.warn('Case not found for file {caseFileName!s:s}.'.format(caseFileName=caseFileName))

    for reconFile in combinedReconFilesList:
        CRLUtility.CRLCopyFile(reconFile,
                               os.path.join(reconProcessedPath,
                                            '{basename!s:s}_{ts:%Y%m%d%H%M%S}'
                                            .format(basename=os.path.basename(reconFile), ts=today)),
                               True, 5)
    logger.info('Reconciliation file(s) processed, initiating retransmit analysis...')
    sQuery = '''
        select dh.contact_id, dh.sid, dh.documentid, max(dh.actiondate) lastdate
        from {table!s:s} dh
        where (dh.contact_id like 'agimtdapps')
        and dh.actionitem = '{action_xmit!s:s}'
        and dh.actiondate < '{yesterday:%d-%b-%Y}'
        and not exists (select historyid from {table!s:s} dh2
                        where dh2.documentid = dh.documentid and dh2.sid = dh.sid
                        and dh2.contact_id = dh.contact_id
                        and dh2.actionitem = '{action_recon!s:s}'
                        and dh2.actiondate > dh.actiondate)
        group by dh.contact_id, dh.sid, dh.documentid
        order by dh.contact_id, dh.sid, dh.documentid
        '''.format(table=docHistory.TABLE_DOCUMENT_HISTORY,
                   action_xmit=docHistory.ACTION_TRANSMIT,
                   yesterday=yesterday,
                   action_recon=docHistory.ACTION_RECONCILE)
    cursor = config.getCursor(config.DB_NAME_XMIT)
    cursor.execute(sQuery)
    recs = cursor.fetch()
    cursor.rollback()
    if recs:
        sidDocMap = {}
        for contactid, sid, docid, lastdate in recs:
            doclist = sidDocMap.get((contactid, sid))
            if not doclist:
                doclist = []
                sidDocMap[(contactid, sid)] = doclist
            doclist.append((docid, lastdate))
        sMessage = "The following documents were not reconciled by AG and may need to "
        sMessage += "be retransmitted. Please review:\r\n\r\n"
        keys = list(sidDocMap.keys())
        keys.sort()
        for contactid, sid in keys:
            trackingid = 'Unknown'
            case = caseFactory.fromSid(sid)
            if case:
                trackingid = case.trackingId
            sMessage += "\r\nContact {contactid!s:s}: case {trackingid!s:s} (sid {sid!s:s})\r\n".format(contactid=contactid, trackingid=trackingid, sid=sid)
            for docid, lastdate in sidDocMap[(contactid, sid)]:
                lastdate = datetime.datetime.fromtimestamp(lastdate)
                filename = "Unknown"
                doc = docFactory.fromDocumentId(docid)
                if doc:
                    filename = doc.fileName
                sMessage += "{last:%d-%b-%Y %H:%M:%S}: {filename!s:s} (Document ID {docid:d})\r\n".format(last=lastdate,
                                                                                                          filename=filename,
                                                                                                          docid=docid)
        sTitle = 'AG ASAP LITE Reconciliation of ASAP Case Documents'
        sAddress = EMAIL_ADDRESS
        if not devState.isDevInstance():
            CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle,
                                    'ilsprod@crlcorp.com', 'AWDApplicationSupport@aig.com,esubmission.requests@aglife.com', '')
        else:
            CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle, 'ilsprod@crlcorp.com')
        logger.info('Retransmit analysis complete.')


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger(LOG_DIR, EMAIL_ADDRESS)
    try:
        begintime = time.time()
        AGLiteRecon()
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error in AGLiteCustom.py')
