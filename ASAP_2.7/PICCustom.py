"""

  Facility:         ILS

  Module Name:      PICCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2006, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for PIC for ASAP processing.

  Author:
      Jarrod Wild

  Creation Date:
      13-Nov-2006

  Modification History:
      21-Dec-2010 mayd
          Added in use of CVCFilter class to check for first CVC files and if found ignore other regions.  Meant to be temporary

      21-Dec-2010 mayd
          Removed use of CVCFilter

      16-Feb-2011 mayd
          Added in ftp debug log functionality

      14-Mar-2011 mayd
          Changed recon logic that determines retransmissions to include more than just picslqapps

      25-Mar-2011 mayd
          changed the CRLFTPPut call by adding in param iMaxRetryCount = 0

      06-Apr-2011 mayd
          added pause of 30 seconds in between file transmissions to Prudential to avoid possible bunching errors

      03-May-2011 mayd
          moved pause of 30 seconds to before the "put" in order to work even if there is an ftp error

      01-Jul-2011 whitea    ticket 23998
          updated error logging, invalid variable in str

      16-Jan-2012 mayd  tkt # 29041
          updated _isIndexedCaseReady() transmission time to 6:30a.m.

      01-JUL-2013 rsu       ticket 41833
          moved back transmission time to start at 4:30a.m.

      06-FEB-2014 rsu       ticket 45172
          switch transmission from ftp with pgp to sftp at Prudential's request, and drop files in the
          /home/zfpcrlin/ folder. capture paramiko logging in the Prudential_Ftp.log file.

      30-APR-2014 rsu       ticket 47749
          Fix restaging to retrans folder after ftp issues

      09-SEP-2019 leec      PRJTASK0019990
          Reformat
          Add support for handling SLQX region

      19-Dec-2019 leec      SCTASK0035887
         Update recon process to check for the case first. And only if the 103 is successfully staged
         to retransmit then stage the documents

      14-MAY-2021 nelsonj
         Migration to new apphub and updating to python 2.7
"""

from ILS.ASAP.Utility import ASAP_UTILITY
from ILS import AcordXML
from ILS.ASAP.IndexHandler import ASAPIndexHandler
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler

import CRLUtility
import datetime
import glob
import os
import sys
import time
import paramiko

import logging
import logging.handlers
from io import BytesIO

ftpServerName = None
ftpUserName = None
ftpPassword = None

ftpRemoteDir = None
_debugLoggerName = 'Pruftp'  # must supply sLoggerName when call CRLGetLogger, otherwise all the other ASAP logging will be in this file

PIC_FTP_HOSTNAME = 'Prudential'

if ASAP_UTILITY.devState.isDevInstance():
    EMAIL_ADDRESS = 'nelsonj@crlcorp.com'
    ftpLogDir = r'E:\Test\Log\ILS'
    if not os.path.exists(ftpLogDir):
        ftpLogDir = r'Z:\Log\ILS'
    ftpLogFile = os.path.join(ftpLogDir, 'Prudential_Ftp.log')
    ftpLogger = CRLUtility.CRLGetLogger(ftpLogFile, sLoggerName=_debugLoggerName)
    ftpLogger.setLevel(CRLUtility.logging.DEBUG)

    ftpServerName, ftpUserName, ftpPassword = 'pre-cfiexternal.prudential.com', 'zftcrlin', 'FqTwpO'
    ftpRemoteDir = r'/home/zftcrlin/'  # if server = test-externalftp.prudential...
    ftpPort = 2200
else:
    ftpLogDir = r'E:\Log\ILS'
    ftpLogFile = os.path.join(ftpLogDir, 'Prudential_Ftp.log')
    ftpLogger = CRLUtility.CRLGetLogger(ftpLogFile, sLoggerName=_debugLoggerName)
    ftpLogger.setLevel(CRLUtility.logging.DEBUG)
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'

    ftpServerName, ftpUserName, ftpPassword = CRLUtility.CRLGetFTPHostInfo(PIC_FTP_HOSTNAME)
    ftpRemoteDir = r'/home/zfpcrlin/'
    ftpPort = 2200

# this map is used to translate from CRL's region_id to an abbreviation that PIC likes to see in their filenames
_regionId_abbrev_map = {'SLQ': 'SQ', 'SLQX': 'SQ', 'CVC': 'CVC'}
_reconFilePrefix_source_code_map = {'181': 'ESubmissions-%_SELQ', 'CVC': 'ESubmissions-APPS_CVC'}


class PICIndexHandler(ASAPIndexHandler):
    """
    Custom handler for building indexes for PIC.
    """

    def _processDerivedFields(self):
        case = self._getCase()
        doc = self._getCurrentDocument()
        docType = doc.getDocTypeName().upper()
        if docType == 'DISCLOSURE':
            handler = self._getAcordHandler()
            appJuris = handler.txList[0].getElement(
                'ACORDInsuredHolding.Policy.ApplicationInfo.ApplicationJurisdiction')
            if appJuris:
                attrs = appJuris.getAttrs()
                tcValue = attrs.get('tc')
                if tcValue:
                    # 12 is the type code for Florida
                    if tcValue == '12':
                        case.contact.index.setValue('REQUIRE', 'FL-DISCLOSURE')
                    # 37 is the type code for New York
                    elif tcValue == '37':
                        case.contact.index.setValue('REQUIRE', 'NY-DISCL')
                    # 45 is the type code for Pennsylvania
                    elif tcValue == '45':
                        case.contact.index.setValue('REQUIRE', 'PA-DISCL')
                    # 6 is the type code for California
                    elif tcValue == '6':
                        case.contact.index.setValue('REQUIRE', 'MISC-MAIL')
        elif docType == 'SECONDARY ADDRESSEE NOTICE':
            handler = self._getAcordHandler()
            appJuris = handler.txList[0].getElement(
                'ACORDInsuredHolding.Policy.ApplicationInfo.ApplicationJurisdiction')
            if appJuris:
                attrs = appJuris.getAttrs()
                tcValue = attrs.get('tc')
                # 53 is the type code for Vermont
                if tcValue and tcValue != '53':
                    case.contact.index.setValue('REQUIRE', 'MISC-MAIL')
        return True


class PICTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for PIC transmission.
    """

    @staticmethod
    def __get103RefNum(acord103Path):
        """
        """
        refNum = '0000000'
        parser = AcordXML.AcordXMLParser()
        handler = parser.parse(acord103Path)
        if handler:
            refNumElem = handler.txList[0].getElement(
                'ACORDInsuredHolding.HoldingSysKey')
            if refNumElem:
                refNum = refNumElem.value
        return refNum

    def _preStage(self):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitPgpPath = os.path.join(xmitStagingPath, 'pgp')
        reviewPath = os.path.join(xmitStagingPath, 'review')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        try:
            # if there are any files here, they were left behind when a previous
            # run failed to complete, so move them to the reviewPath folder
            toXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
            for xmitFile in toXmitFiles:
                CRLUtility.CRLCopyFile(xmitFile.getFullPath(),
                                       os.path.join(reviewPath, xmitFile.fileName),
                                       True, 5)
            if len(toXmitFiles) > 0:
                self._getLogger().error(
                    'Files were left behind in ' +
                    '{xmitStagingPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitStagingPath=xmitStagingPath))
            # move any PGP-encrypted zip files to reviewPath folder
            pgpFiles = fm.glob(os.path.join(xmitPgpPath, '*.*'))
            for pgpFile in pgpFiles:
                CRLUtility.CRLCopyFile(pgpFile.getFullPath(),
                                       os.path.join(reviewPath, pgpFile.fileName),
                                       True, 5)
            if len(pgpFiles) > 0:
                self._getLogger().error(
                    'PGP-encrypted zip files were left behind in ' +
                    '{xmitPgpPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitPgpPath=xmitPgpPath))
            # now move files from the retrans folder to the xmit staging folder
            toRetransFiles = glob.glob(os.path.join(retransPath, '*.*'))
            for retransFile in toRetransFiles:
                baseFileName = os.path.basename(retransFile)
                CRLUtility.CRLCopyFile(retransFile,
                                       os.path.join(xmitStagingPath, baseFileName),
                                       True, 5)
        except:
            self._getLogger().warn(
                'Pre-stage failed with exception: ', exc_info=True)
            fSuccess = False
        return fSuccess

    def _isIndexedCaseReady(self):

        fReady = False
        # check to see if time of day is within PIC's window
        # (4:30am - 8:30pm)

        now = datetime.datetime.today()

        today_begin = datetime.datetime(now.year, now.month, now.day,
                                        4, 30)
        today_end = datetime.datetime(now.year, now.month, now.day,
                                      20, 30)

        if today_begin <= now <= today_end:
            fact = ASAP_UTILITY.getViableCaseFactory()
            case = fact.fromSid(self._getCurrentCase().sid)
            if case and case.sample and case.sample.transmitDate:
                fReady = True

        return fReady

    def _stageIndexedCase(self):
        case = self._getCurrentCase()
        regionAbbreviation = _regionId_abbrev_map.get(case.contact.region_id)
        fSuccess = True
        fromToMoves = []
        filesToDelete = []
        acord103Path = os.path.join(case.contact.acord103_dir,
                                    '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
        refNum = '0000000'
        if os.path.isfile(acord103Path):
            refNum = self.__get103RefNum(acord103Path)
            # if any docids are left in the list, this is a subsequent transmission
            # and not a full retransmit, so remove the 103 and don't transmit it
            if self._isFullTransmit():
                xmit103Path = os.path.join(case.contact.xmit_dir,
                                           'CRL_{regionAbbreviation!s:s}_{refNum!s:s}_{trackingId!s:s}.XML'.format(regionAbbreviation=regionAbbreviation, refNum=refNum, trackingId=case.trackingId))
                fromToMoves.append((acord103Path, xmit103Path))
            else:
                filesToDelete.append(acord103Path)
        else:
            fSuccess = False
            self._getLogger().warn(
                'Failed to find ACORD 103 for case ({sid!s:s}/{trackingId!s:s}).'
                .format(sid=case.sid, trackingId=case.trackingId))
        # now try to get doc/index pairs
        documents = list(case.getDocuments().values())
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            picDocPrefix = 'CRL_{regionAbbreviation!s:s}_{refNum!s:s}_{docPrefix!s:s}'.format(regionAbbreviation=regionAbbreviation, refNum=refNum, docPrefix=docPrefix)
            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '{docPrefix!s:s}.IDX'.format(docPrefix=docPrefix))
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir,
                                           '{picDocPrefix!s:s}.tif'.format(picDocPrefix=picDocPrefix))
                xmitIdxPath = os.path.join(case.contact.xmit_dir,
                                           '{picDocPrefix!s:s}.idx'.format(picDocPrefix=picDocPrefix))
                fromToMoves.append((docPath, xmitDocPath))
                fromToMoves.append((idxPath, xmitIdxPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid {documentId!s:s} (sid {sid!s:s}).'
                    .format(documentId=doc.getDocumentId(), sid=case.sid))
        if fSuccess:
            if self._isFirstTransmit():
                req = ASAP_UTILITY.getASAPAcordRequest()
                if not req.makeRequestBySid(case.sid):
                    fSuccess = False
        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)
            for deleteFile in filesToDelete:
                CRLUtility.CRLDeleteFile(deleteFile)
        return fSuccess

    def _transmitStagedCases(self):
        fSuccess = True
        sftpClient = None
        transport = None

        contact = self._getContact()
        regionAbbreviation = _regionId_abbrev_map.get(contact.region_id)
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')
        retransPath = os.path.join(xmitStagingPath, 'retrans')

        fm = ASAP_UTILITY.getASAPFileManager(contact)
        asapToXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))

        if (len(asapToXmitFiles) > 0):
            # build zip file
            self._getLogger().info(
                'There are {asapToXmitFiles:d} files in the transmit staging folder to process...'
                .format(asapToXmitFiles=len(asapToXmitFiles)))
            today = datetime.datetime.today()
            for asapFile in asapToXmitFiles:
                refNum = asapFile.fileName.split('_')[2]
                zipFileName = 'CRL_{regionAbbreviation!s:s}_{refNum!s:s}_{today:%Y%m%d%H%M%S}.zip'.format(
                    regionAbbreviation=regionAbbreviation, refNum=refNum, today=today)
                CRLUtility.CRLAddToZIPFile(asapFile.getFullPath(),
                                           os.path.join(xmitZipPath, zipFileName),
                                           False)
                fm.deleteFile(asapFile)

        asapZipFiles = fm.glob(os.path.join(xmitZipPath, '*.zip'))

        for asapZipFile in asapZipFiles:
            xmitZipFile = os.path.join(xmitZipPath, asapZipFile.fileName)
            serverPath = ftpRemoteDir + asapZipFile.fileName  # forward slash is in ftpRemoteDir already
            handler = stream = None
            try:
                # capture paramiko logging to a stream which will later get written to the ftpLogger
                # can't just have paramiko log to the ftpLogger since it rewrites the log file every time
                # and also needed to reimport paramiko to get this to work
                stream = BytesIO()
                handler = logging.StreamHandler(stream)
                log = paramiko.util.logging.getLogger()
                log.setLevel(logging.DEBUG)
                for oldhandler in log.handlers:
                    log.removeHandler(oldhandler)
                log.addHandler(handler)

                # noinspection PyTypeChecker
                transport = paramiko.Transport((ftpServerName, ftpPort))
                transport.connect(username=ftpUserName, password=ftpPassword)
                sftpClient = paramiko.SFTPClient.from_transport(transport)
                info = sftpClient.put(xmitZipFile, serverPath)
                self._getLogger().info("{fileName!s:s} Transmission info: {info!s:s}"
                                       .format(fileName=asapZipFile.fileName, info=str(info)))
                CRLUtility.CRLCopyFile(asapZipFile.getFullPath(),
                                       os.path.join(xmitSentPath, asapZipFile.fileName), False, 5)

            except IOError as ioe:
                if str(ioe.args[0]).startswith('size mismatch in put') or (len(ioe.args) > 1 and str(ioe.args[1]).lower().endswith('is not a valid file path')):
                    # ok if size mismatch because remote server does not support stat command
                    self._getLogger().warn(str(ioe.args))
                    self._getLogger().info("Transmitted {contact_id!s:s} file {fileName!s:s}".format(contact_id=contact.contact_id, fileName=asapZipFile.fileName))
                    CRLUtility.CRLCopyFile(asapZipFile.getFullPath(),
                                           os.path.join(xmitSentPath, asapZipFile.fileName), False, 5)
                else:
                    fSuccess = False
                    self._getLogger().warn(
                        'Failed to FTP file {fullPath!s:s} to Prudential (extracting original zip to retrans folder):'
                        .format(fullPath=asapZipFile.getFullPath()), exc_info=True)
                    CRLUtility.CRLUnzipFile(asapZipFile.getFullPath(), retransPath)

            except:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to FTP file {fullPath!s:s} to Prudential (extracting original zip to retrans folder):'
                    .format(fullPath=asapZipFile.getFullPath()), exc_info=True)
                CRLUtility.CRLUnzipFile(asapZipFile.getFullPath(), retransPath)

            finally:
                fm.deleteFile(asapZipFile)

                if (handler and stream and ftpLogger):
                    # write paramiko logging stream to the ftp logger
                    handler.flush()
                    handler.close()
                    logLines = stream.getvalue().split('\n')
                    for logLine in logLines:
                        ftpLogger.debug(logLine)

                if sftpClient:
                    sftpClient.close()

                if transport:
                    transport.close()

        return fSuccess


'''
Recon Information:
    We reconcile on a case basis ( as opposed to down to the image file level )

The recon file format will be:


181=2682986;3186717;3187761;3188406;3189568;3191691;
CVC=0ee8c4c0-f2d3-27b1-bb74-fc8cb0affab2;0ee8c4c0-f2d3-27b1-bb74-fc8cb0affab3;0ee8c4c0-f2d3-27b1-bb74-fc8cb0affab4;


where each line contains <VENDOR-CODE>=a semi-colon delimited list of cases

181 is the Select Quote line, CVC is CVC

'''


def PICRecon(regionIdIn):
    """
    Perform reconciliation of related PIC documents for ASAP.
    """

    logger = CRLUtility.CRLGetLogger()
    today = datetime.datetime.today()
    config = ASAP_UTILITY.getXmitConfig()
    reconContact = config.getContact('PIC', regionIdIn, 'APPS')
    if reconContact:
        reconStagingFolder = os.path.join(
            os.path.dirname(reconContact.document_dir),
            'recon')
        reconProcessedFolder = os.path.join(
            reconStagingFolder,
            config.getSetting(config.SETTING_PROCESSED_SUBDIR))

        docFactory = ASAP_UTILITY.getASAPDocumentFactory()
        caseFactory = ASAP_UTILITY.getASAPCaseFactory()
        viableFactory = ASAP_UTILITY.getViableCaseFactory()
        docHistory = ASAP_UTILITY.getASAPDocumentHistory()
        acordOrderFactory = ASAP_UTILITY.getAcordOrderFactory()
        if not os.path.isdir(reconProcessedFolder):
            os.makedirs(reconProcessedFolder)
        files = glob.glob(os.path.join(reconStagingFolder, '*.rec'))
        for reconFile in files:
            baseFileName = os.path.basename(reconFile)
            filePtr = open(reconFile, 'r')
            line = filePtr.read()
            logger.debug('line = ' + str(line))

            # the line will include the contact prefix  such as 181 =, or CVC =
            # so will need to remove it
            prefix, data = line.split("=")
            filePtr.close()
            fError = False
            refIds = data.split(';')
            refIds = [refId.strip() for refId in refIds if refId.strip()]
            asapDocuments = []
            for refId in refIds:
                msg = 'reconciling ref id= ' + str(refId)
                logger.debug(msg)
                sourceCode = _reconFilePrefix_source_code_map.get(prefix.strip())
                acordOrderList = acordOrderFactory.fromRequirementInfoUniqueIdAndSourceCode(refId, sourceCode)
                if (len(acordOrderList) == 0):
                    msg = "Unable to find acord order searching by RequirementInfoUniqueId = {refId!s:s} and SourceCode ={sourceCode!s:s}".format(refId=refId, sourceCode=sourceCode)
                    logger.warn(msg)
                    continue
                acordOrder = acordOrderList[0]
                msg = 'trackingId= ' + str(acordOrder.trackingId)
                logger.debug(msg)
                case = viableFactory.fromTrackingID(acordOrder.trackingId)
                if case and case.docGroup:
                    docs = case.docGroup.documents
                    if docs:
                        for doc in docs:
                            for actiondate, actionitem in doc.transmitHistory:
                                if actionitem == 'transmit':
                                    asapDoc = docFactory.fromDocumentId(doc.documentId)
                                    if asapDoc:
                                        asapDocuments.append(asapDoc)
                                    else:
                                        logger.warn("Document not found for docid {documentId:d} (sid {sid!s:s})."
                                                    .format(documentId=doc.documentId, sid=case.sample.sid))
                                        fError = True
                    else:
                        logger.warn("No documents found for sid {sid!s:s}.".format(sid=case.sample.sid))
                        fError = True
                else:
                    logger.warn("Case not found for reference ID {refId!s:s}.".format(refId=refId))
                    fError = True
            cases = caseFactory.casesForDocuments(asapDocuments)
            for case in cases:
                for doc in list(case.getDocuments().values()):
                    docHistory.trackDocument(doc, docHistory.ACTION_RECONCILE)
            if fError:
                logger.error('There were one or more errors processing file {baseFileName!s:s}.'
                             .format(baseFileName=baseFileName))
            CRLUtility.CRLCopyFile(
                reconFile,
                os.path.join(reconProcessedFolder,
                             baseFileName + '_' + today.strftime('%Y%m%d%H%M%S')),
                True, 5)
        #
        # Get all samples that were transmitted but not reconciled,
        # only if at least one recon file was processed.
        #
        if files:
            lookbackDate = today - datetime.timedelta(days=2 * 365)
            contactIds = [reconContact.contact_id]
            # We only get one recon file from PIC for both SLQ and SLQX region
            # the recon file will be processed in the PIC_SLQ folder so include SLQX region as well
            if regionIdIn == 'SLQ':
                contactIds.append('picslqxexo')
            logger.info(
                'Reconciliation file(s) processed, initiating retransmit analysis...')
            sQuery = '''
                select dh.sid, dh.documentid, max(dh.actiondate) lastdate
                from {TABLE_DOCUMENT_HISTORY!s:s} dh
                where dh.contact_id in ('{contact_id!s:s}')
                and dh.actionitem = '{ACTION_TRANSMIT!s:s}'
                and dh.actiondate >= '{lookback:%d-%b-%Y}'
                and dh.actiondate < '{today:%d-%b-%Y}'
                and not exists
                (select historyid from {TABLE_DOCUMENT_HISTORY!s:s} dh2
                where dh2.documentid = dh.documentid and dh2.sid = dh.sid
                and dh2.contact_id = dh.contact_id
                and dh2.actionitem = '{ACTION_RECONCILE!s:s}'
                and dh2.actiondate > dh.actiondate)
                group by dh.sid, dh.documentid
                order by dh.sid, dh.documentid
                '''.format(TABLE_DOCUMENT_HISTORY=docHistory.TABLE_DOCUMENT_HISTORY,
                           contact_id="','".join(contactIds),
                           ACTION_TRANSMIT=docHistory.ACTION_TRANSMIT,
                           lookback=lookbackDate,
                           today=today,
                           ACTION_RECONCILE=docHistory.ACTION_RECONCILE)
            cursor = config.getCursor(config.DB_NAME_XMIT)
            cursor.execute(sQuery)
            recs = cursor.fetch()
            cursor.rollback()
            if recs:
                from ILS.ASAP.MainHandler import ASAPMainHandler
                fError = False
                sidDocMap = {}
                for sid, docid, lastdate in recs:
                    doclist = sidDocMap.get(sid)
                    if not doclist:
                        doclist = []
                        sidDocMap[sid] = doclist
                    doclist.append(docid)

                for sid, docids in list(sidDocMap.items()):
                    case = caseFactory.fromSid(sid)
                    if case:
                        logger.info('Staging ACORD 103 for {trackingId!s:s} to retransmit...'.format(trackingId=case.trackingId))
                        success = ASAP_UTILITY.reReleaseCase(case)
                        if success:
                            for docid in docids:
                                doc = docFactory.fromDocumentId(docid)
                                if doc:
                                    logger.info('Staging document {documentId:d} to retransmit...'
                                                .format(documentId=doc.getDocumentId()))
                                    ASAP_UTILITY.setExportFlag(doc, ASAPMainHandler.DOC_EXPORTED_NO)
                                    if not success:
                                        fError = True
                                else:
                                    fError = True
                        else:
                            fError = True
                            logger.warn('Failed to stage ACORD 103 for {trackingId!s:s} to retransmit.'.format(trackingId=case.trackingId))
                    else:
                        fError = True
                if fError:
                    logger.error('There were one or more errors while staging to retransmit.')
            logger.info('Retransmit analysis complete.')
    else:
        logger.error('Recon contact not configured.')


'''
 Purpose: a simple class with a write method to be used as a substitute for sys.out so we can capture ftp debug output

'''


class WritableObject(object):

    def __init__(self):
        self.content = []

    def write(self, string):
        self.content.append(string.strip())

    def log(self, logger):
        for line in self.content:
            if (len(line) > 0):
                logger.debug(line)


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        arg = ''
        if len(sys.argv) > 1:
            arg = sys.argv[1]
        if arg == 'recon':
            PICRecon('SLQ')
            PICRecon('CVC')
        else:
            logger.warn('Argument(s) not valid. Valid arguments:')
            logger.warn('recon')
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error')
