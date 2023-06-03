"""

  Facility:         ILS

  Module Name:      AIGCustom

  Version:
      Software Version:          Python version 2.5

      Copyright 2006, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for AIG for ASAP processing.

  Author:
      Jarrod Wild

  Creation Date:
      13-Nov-2006

  Modification History:
      10-May-2007   jmw     SRF #N/A
      Embed a timestamp in filenames sent to AIG, and throw out timestamps
      from what comes back in recon file.  Format would be:
      <identifier>_<YYMMDDhhmmss>.<extension>
      10-Aug-2010   kg      Client Request
      Changed the FTP server to sftp.aig.com and files should go to /prod directory
      07-Dec-2011   rvs     Ticket # 27533
      Added AGI.EFIN and UST.EFIN regions. Using EmailAddress.
      15-Dec-2011   rvs     Support Ticket # 27533
      Set MGAID and PROVIDER to EFIN1 if AGI.EFIN or UST.EFIN
      13-Jan-2012   rvs     Support Ticket # 27533
      Set PROVIDER TO CRLEFINANCIAL if AGI.EFIN or UST.EFIN
      07-Mar-2012  mayd  modified method _processDerivedFields to account for
                  missing element <UserLoginName> for EFIN cases

     23-OCT-2015 venkatj    Paperstore migration 45490
     Added docpath to the logging.

     25-Feb-2016  gandhik   Ticket 67313
     Notify the team of any errors with the imporperly formatted records

     10-Nov-2016  gandhik   Ticket 69036
     Changing the files to go Unencrypted to AG's new Server www-263.aig.com

     17-Jan-2017  gandhik   Ticket 78783
         Include the below email addresses in case of any recon exceptions
         AWDApplicationSupport@aig.com
         esubmission.requests@aglife.com
     20-Jun-2018  whitea    CHG0032658
         Updated recon query to exclude missing and outdated documents

     14-MAY-2021 nelsonj
         Migration to new apphub and updating to python 2.7

"""
from __future__ import division, absolute_import, with_statement, print_function
from ILS.ASAP.IndexHandler import ASAPIndexHandler
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler
from ILS.ASAP.Utility import ASAP_UTILITY
from CRL.Utils.FtpUtils.Utilities import get_host_info
import CRLUtility
import datetime
import glob
import os
import sys
import time

from EmailAddresses import emailAddress

emailAddress = emailAddress()
EMAIL_ADDRESS = emailAddress['ilsprod@crlcorp.com']


def buildAIGFileName(oldFileName, timestamp):
    return '{basename!s:s}_{ts!s:s}.{ext!s:s}'.format(basename=oldFileName.split('.')[0],
                                                      ts=timestamp.strftime('%Y%m%d%H%M%S'),
                                                      ext=oldFileName.split('.')[1].upper())


def extractOldFileName(aigFileName):
    return '{basename!s:s}.{ext!s:s}'.format(basename=aigFileName.split('.')[0].split('_')[0],
                                             ext=aigFileName.split('.')[1])


class AIGIndexHandler(ASAPIndexHandler):
    """
    Custom handler for building indexes for AIG.
    """
    aigStateDict = {'1': 'AL', '2': 'AK', '4': 'AZ', '5': 'AR', '6': 'CA', '7': 'CO',
                    '8': 'CT', '9': 'DE', '10': 'DC', '12': 'FL', '13': 'GA', '15': 'HI',
                    '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY',
                    '22': 'LA', '23': 'ME', '25': 'MD', '26': 'MA', '27': 'MI', '28': 'MN',
                    '29': 'MS', '30': 'MO', '31': 'MT', '32': 'NE', '33': 'NV', '34': 'NH',
                    '35': 'NJ', '36': 'NM', '37': 'NY', '38': 'NC', '39': 'ND', '41': 'OH',
                    '42': 'OK', '43': 'OR', '45': 'PA', '47': 'RI', '48': 'SC', '49': 'SD',
                    '50': 'TN', '51': 'TX', '52': 'UT', '53': 'VT', '55': 'VA', '56': 'WA',
                    '57': 'WV', '58': 'WI', '59': 'WY', '46': 'PR', '14': 'GU', '54': 'VI'}

    AIG_REPLACEMENT_STATE_LIST = ('AZ', 'CO', 'HI', 'IA', 'LA', 'MD', 'MS', 'MT', 'NH',
                                  'NM', 'OR', 'VT', 'NC', 'AR', 'FL', 'OK', 'AL', 'NJ')

    APPRIGHT_MATRIX = 'ARMAT'
    EFIN_MGAID = 'EFIN1'
    EFIN_PROVIDER_ID = 'CRLEFINANCIAL'
    if ASAP_UTILITY.devState.isDevInstance():
        EFIN_LOGINNAME = 'EFIN1'
    else:
        EFIN_LOGINNAME = 'EFINP'

    LOGIN_NAME_MATRIX_ASAP = 'MatrixASAP'

    QC_USER_AUTOUSER = 'AUTOUSER'

    def _processDerivedFields(self):
        case = self._getCase()
        index = case.contact.index
        handler = self._getAcordHandler()
        txLifeElem = handler.txList[0]
        # birth state logic: if birth jurisdiction is a valid state, use two
        # character state code (see above), else use the tc value of the birth
        # country
        state = ''
        birthJuris = txLifeElem.getElement(
            'ACORDInsuredParty.Person.BirthJurisdictionTC')
        if birthJuris:
            state = self.aigStateDict.get(birthJuris.getAttrs().get('tc'))
        if not state:
            birthCountry = txLifeElem.getElement(
                'ACORDInsuredParty.Person.BirthCountry')
            if birthCountry:
                state = birthCountry.getAttrs().get('tc')
        if not state:
            state = ''
        index.setValue('BSTATE', state)
        # age logic: if signed date is 7 months or greater from last birthday, age
        # should be the upcoming age rather than current age; this means adding 5
        # months to the signed date (7 mo. from last birthday + 5 mo. = next birthday)
        # and subtracting birth date to get adjusted age
        age = ''
        dob = index.getValue('DOB')
        if dob:
            dob = CRLUtility.ParseStrDate(dob, True)
            signedDateElem = txLifeElem.getElement(
                'ACORDInsuredHolding.Policy.ApplicationInfo.SignedDate')
            if signedDateElem:
                signedDate = signedDateElem.value
                if signedDate:
                    signedDate = CRLUtility.ParseStrDate(signedDate, True)
                    signedDate += datetime.timedelta(150)
                    age = signedDate.year - dob.year
                    if ((signedDate.month < dob.month)
                        or ((signedDate.month == dob.month)
                            and (signedDate.day < dob.day))):
                        age -= 1
                    age = str(age)
        index.setValue('AGE', age)
        # stat co logic: if app jurisdiction state is New York, set statutory company
        # to 'USL', else use 'AGL'
        statCo = ''
        appJuris = txLifeElem.getElement(
            'ACORDInsuredHolding.Policy.ApplicationInfo.ApplicationJurisdiction')
        appJurisTc = None
        if appJuris:
            statCo = 'AGL'
            appJurisTc = appJuris.getAttrs().get('tc')
            if self.aigStateDict.get(appJurisTc) == 'NY':
                statCo = 'USL'
        index.setValue('STAT CO', statCo)
        # replacement logic: if there is a relation with a role code of 64 ("Replaced by"),
        # or if there's a relation with role code 124 ("Additional Holding") and the app
        # jurisdiction state is part of the "replacement state list" defined by AIG,
        # then this case is flagged as a replacement
        cReplacement = 'N'
        if statCo != 'USL':
            fExisting = False
            olife = txLifeElem.getElement('TXLifeRequest.OLifE')
            for elem in olife.getElements():
                if elem.name == 'Relation':
                    rolecode = elem.getElement('RelationRoleCode')
                    if rolecode:
                        tcVal = rolecode.getAttrs().get('tc')
                        if tcVal == '64':
                            cReplacement = 'Y'
                            break
                        elif tcVal == '124':
                            fExisting = True
            if fExisting and cReplacement == 'N':
                if self.aigStateDict.get(appJurisTc) in self.AIG_REPLACEMENT_STATE_LIST:
                    cReplacement = 'Y'
        index.setValue('REPLACEMENT', cReplacement)
        # existing case logic: if there is at least one recon record for a document
        # in the case, or if not all of the previously transmitted documents for this
        # case are in the list of documents in the current transmit attempt for this
        # case, then it's an existing case (the second part is meant to cover retransmits
        # of cases that were not recon'ed)
        cExistingCase = 'N'
        if not self._isFirstTransmit():
            cExistingCase = 'Y'
        index.setValue('EXISTING CASE', cExistingCase)
        # MGAID/AppRight logic: look for UserLoginName and if set to ARMAT,
        # confirm that QC case history shows an app auto-uploaded by APPS,
        # and if so then set PROVIDER and MGAID to ARMAT
        # if not, then change UserLoginName to MatrixASAP and
        # update XML file
        mgaid = ''
        loginNameElem = txLifeElem.getElement('UserAuthRequest.UserLoginName')
        if loginNameElem and loginNameElem.value == self.APPRIGHT_MATRIX:
            # confirm AppRight by checking if there's a Term Application or Application
            # uploaded by APPS using the AUTOUSER userid
            qcDocFact = ASAP_UTILITY.getQCDocumentFactory()
            qcCaseFact = ASAP_UTILITY.getCaseQCFactory()
            fAppRightConfirmed = False
            docGroup = qcDocFact.fromSid(case.sid)
            caseQc = qcCaseFact.fromSid(case.sid)
            appDocIds = [int(doc.documentId) for doc in docGroup.documents
                         if doc.documentType in ('TERM APPLICATION', 'APPLICATION')]
            for item in caseQc.history:
                if (item.action == item.ACTION_ADD and
                        int(item.documentId) in appDocIds and
                        item.createdBy == self.QC_USER_AUTOUSER):
                    fAppRightConfirmed = True
                    break
            if fAppRightConfirmed:
                mgaid = self.APPRIGHT_MATRIX
                index.setValue('PROVIDER', self.APPRIGHT_MATRIX)
            else:
                loginNameElem.value = self.LOGIN_NAME_MATRIX_ASAP
                xmlPath = os.path.join(case.contact.acord103_dir,
                                       '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
                self._getLogger().info('AppRight->standard case, updating {xmlPath!s:s}.'.format(xmlPath=xmlPath))
                handler.writeXML(xmlPath)
        if case.contact.contact_id == 'agiefinapps':
            mgaid = self.EFIN_MGAID
            index.setValue('PROVIDER', self.EFIN_PROVIDER_ID)

        index.setValue('MGAID', mgaid)
        return True

    def _postProcessIndex(self):
        # open index file for appending, and write
        # the client doc types and image file names:
        # APPII,00000050.TIF
        # last line would be the 103 file:
        # XML,AIG002299999.XML
        now = datetime.datetime.today()
        case = self._getCase()
        docs = case.getDocuments().values()
        appendlines = []
        for doc in docs:
            appendlines.append('{docTypeName!s:s},{fileName!s:s}\n'
                               .format(docTypeName=case.contact.docTypeNameMap.get(doc.getDocTypeName()),
                                       fileName=buildAIGFileName(doc.fileName, now)))
        # only add the 103 line if this is a first transmission
        if self._isFirstTransmit():
            appendlines.append('XML,{fileName!s:s}\n'
                               .format(fileName=buildAIGFileName('{trackingId!s:s}.XML'
                                                                 .format(trackingId=case.trackingId), now)))
        paths = self._getIndexPaths()
        if paths and os.path.isfile(paths[0]):
            filePtr = open(paths[0], 'a')
            filePtr.writelines(appendlines)
            filePtr.close()
        return True


class AIGTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for AIG transmission.
    """
    AIG_FTP_HOSTNAME = 'AIG ASAP SFTP-263'
    AIG_FTP_PORT = 9022

    def _preStage(self):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
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
            # 10-Nov-2016: Instead of pgp file we might have a zip file left from previous run
            zipFiles = fm.glob(os.path.join(xmitZipPath, '*.*'))
            for zipFile in zipFiles:
                self._getLogger().warn('Zip files were left behind {fileName!s:s} for AG (extracting zip to retrans folder).'
                                       .format(fileName=zipFile.fileName))
                CRLUtility.CRLUnzipFile(zipFile.getFullPath(), retransPath)
                fm.deleteFile(zipFile)
            # now move files from the retrans folder to the xmit staging folder
            toRetransFiles = glob.glob(os.path.join(retransPath, '*.*'))
            for retransFile in toRetransFiles:
                baseFileName = os.path.basename(retransFile)
                CRLUtility.CRLCopyFile(retransFile,
                                       os.path.join(xmitStagingPath, baseFileName),
                                       True, 5)
                self._getLogger().info('Moving Files from retrans %s  ', baseFileName)
        except:
            self._getLogger().warn(
                'Pre-stage failed with exception: ', exc_info=True)
            fSuccess = False
        return fSuccess

    def _isIndexedCaseReady(self):
        fReady = False
        # first check to see if time of day is within AIG's window
        # (3am - 6:30pm)
        now = datetime.datetime.today()
        today_begin = datetime.datetime(now.year, now.month, now.day,
                                        3, 0)
        today_end = datetime.datetime(now.year, now.month, now.day,
                                      18, 30)
        if today_begin <= now <= today_end:
            fReady = True
        if fReady:
            # check lims transmit date to see if case is
            # ready to xmit
            fReady = False
            config = ASAP_UTILITY.getXmitConfig()
            case = self._getCurrentCase()
            sQuery = '''
                select transmit_date
                from sample
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
        return fReady

    def _stageIndexedCase(self):
        case = self._getCurrentCase()
        fSuccess = True
        now = datetime.datetime.today()
        fromToMoves = []
        # map of old file name to AIG file name
        aigFileMap = {}
        # get case file
        idxPath = os.path.join(case.contact.index_dir,
                               '{trackingId!s:s}.IDX'.format(trackingId=case.trackingId))
        if os.path.isfile(idxPath):
            # collect doc file names for use in renaming actual files
            filePtr = open(idxPath, 'r')
            datalines = filePtr.readlines()
            filePtr.close()
            for line in datalines:
                tokens = line.strip().split(',')
                if len(tokens) == 2:
                    idxFileName = tokens[1]
                    if idxFileName[-4:] in ('.TIF', '.XML'):
                        aigFileMap[extractOldFileName(idxFileName)] = idxFileName
            aigFileName = buildAIGFileName('{trackingId!s:s}.CAS'.format(trackingId=case.trackingId),
                                           now)
            xmitIdxPath = os.path.join(case.contact.xmit_dir, aigFileName)
            fromToMoves.append((idxPath, xmitIdxPath))
        else:
            fSuccess = False
            self._getLogger().warn(
                'Failed to find index file for case ({sid!s:s}/{trackingId!s:s}).'
                .format(sid=case.sid, trackingId=case.trackingId))
        # get 103 only if first transmit
        if self._isFirstTransmit():
            acord103Path = os.path.join(case.contact.acord103_dir,
                                        '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
            if os.path.isfile(acord103Path):
                aigFileName = aigFileMap.get(os.path.basename(acord103Path))
                if not aigFileName:
                    aigFileName = os.path.basename(acord103Path)
                xmit103Path = os.path.join(case.contact.xmit_dir, aigFileName)
                fromToMoves.append((acord103Path, xmit103Path))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find ACORD 103 for case ({sid!s:s}/{trackingId!s:s}).'
                    .format(sid=case.sid, trackingId=case.trackingId))
        # now try to get docs
        documents = case.getDocuments().values()
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            if os.path.isfile(docPath):
                aigFileName = aigFileMap.get(os.path.basename(docPath).upper())
                if not aigFileName:
                    aigFileName = os.path.basename(docPath)
                xmitDocPath = os.path.join(case.contact.xmit_dir, aigFileName)
                fromToMoves.append((docPath, xmitDocPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find indexed image for docid {documentId!s:s} (sid {sid!s:s}) and docpath {docPath!s:s}.'
                    .format(documentId=doc.getDocumentId(), sid=case.sid, docPath=docPath))
        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)
        return fSuccess

    def _transmitStagedCases(self):
        fSuccess = True
        sServer, sUser, sPassword = get_host_info(self.AIG_FTP_HOSTNAME)
        contact = self._getContact()
        if contact.contact_id == 'agiampcapps':
            agency = 'AMPAC'
        elif contact.contact_id == 'agictrlapps':
            agency = 'ICTRL'
        elif contact.contact_id == 'agislqapps':
            agency = 'SELQ'
        elif contact.contact_id == 'agimtxapps':
            agency = 'MTRX'
        elif contact.contact_id == 'ustmtxapps':
            agency = 'UMTRX'
        elif contact.contact_id == 'agiefinapps':
            agency = 'EFIN'
        elif contact.contact_id == 'ustefinapps':
            agency = 'UEFIN'
        else:
            agency = ''
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        asapToXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
        if (len(asapToXmitFiles) > 0):
            self._getLogger().info(
                'There are {asapToXmitFiles:d} files in the transmit staging folder to process...'
                .format(asapToXmitFiles=len(asapToXmitFiles)))
            today = datetime.datetime.today()
            zipFileName = 'CRL{agency!s:s}{today:%Y%m%d%H%M%S}.ZIP'.format(agency=agency, today=today)
            asapZipFile = fm.newFile(os.path.join(xmitZipPath, zipFileName), True)
            for asapFile in asapToXmitFiles:
                CRLUtility.CRLAddToZIPFile(asapFile.getFullPath(),
                                           asapZipFile.getFullPath(),
                                           False)
                fm.deleteFile(asapFile)
            # No more encryption needed as per AG's new FTP migration
            # 10-Nov-2016 : FTP Files to AG new server
            if not ASAP_UTILITY.devState.isDevInstance():
                serverPath = '/home/eid1esub/prod/' + asapZipFile.fileName
            else:
                serverPath = '/home/eid1esub/test/' + asapZipFile.fileName
            try:
                import paramiko
                # noinspection PyTypeChecker
                transport = paramiko.Transport((sServer, self.AIG_FTP_PORT))
                transport.connect(username=sUser, password=sPassword)
                sftpClient = paramiko.SFTPClient.from_transport(transport)
                info = sftpClient.put(asapZipFile.getFullPath(), serverPath)
                self._getLogger().info("{fileName!s:s} Transmission info: {info!s:s}"
                                       .format(fileName=asapZipFile.fileName, info=str(info)))
                CRLUtility.CRLCopyFile(asapZipFile.getFullPath(),
                                       os.path.join(xmitSentPath, asapZipFile.fileName), False, 5)
            except:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to FTP file {fullPath!s:s} to AG (extracting original zip to retrans folder):'
                    .format(fullPath=asapZipFile.getFullPath()), exc_info=True)
                CRLUtility.CRLUnzipFile(asapZipFile.getFullPath(), retransPath, bDeleteZip=True)
            # now move the zip file to the sent folder if upload to FTP was successful
            if os.path.exists(asapZipFile.getFullPath()):
                fm.moveFile(asapZipFile, os.path.join(xmitSentPath, asapZipFile.fileName))
                self._getLogger().info('Moved {fileName!s:s} to sent folder.'.format(fileName=asapZipFile.fileName))

        return fSuccess


def AIGRecon():
    """
    Perform reconciliation of related AIG documents for ASAP.
    """
    logger = CRLUtility.CRLGetLogger()
    fError = False
    errRecList = []
    today = datetime.datetime.today()
    yesterday = CRLUtility.CRLGetBusinessDayOffset(today, -1)
    config = ASAP_UTILITY.getXmitConfig()
    caseFactory = ASAP_UTILITY.getASAPCaseFactory()
    docFactory = ASAP_UTILITY.getASAPDocumentFactory()
    docHistory = ASAP_UTILITY.getASAPDocumentHistory()
    reconContact = config.getContact('AGI', 'SLQ', 'APPS')
    if reconContact:
        reconStagingPath = os.path.join(
            os.path.dirname(reconContact.document_dir),
            'recon')
        reconProcessedPath = os.path.join(
            reconStagingPath,
            config.getSetting(config.SETTING_PROCESSED_SUBDIR))
        rawdata = []
        reconFiles = glob.glob(os.path.join(reconStagingPath, '*.txt'))
        for reconFile in reconFiles:
            reconPtr = open(reconFile, 'r')
            rawdata += reconPtr.readlines()
            reconPtr.close()
        if rawdata:
            logger.info('Processing reconciliation file(s)...')
            caseDict = {}
            for row in rawdata:
                recs = row.strip().upper().split()
                if len(recs) > 0:
                    if len(recs) != 4:
                        fError = True
                        errRecList.append(recs)
                        logger.warn('Improperly formatted record found. {recs!s:s}'.format(recs=recs))
                    else:
                        fileName, dateval, timeval, caseFileName = recs
                        fileName = extractOldFileName(fileName)
                        baseFileName = fileName.upper()
                        caseFileName = extractOldFileName(caseFileName)
                        caseFileName = caseFileName.upper()
                        case = caseFactory.fromTrackingId(caseFileName.split('.')[0])
                        if case:
                            fileType = baseFileName.split('.')[-1]
                            if fileType == 'TIF':
                                doc = docFactory.fromFileName(baseFileName)
                                if doc:
                                    sidData = caseDict.get(case.sid)
                                    if sidData:
                                        storedcase, xmlFile = sidData
                                        storedcase.addDocument(doc)
                                    else:
                                        case.addDocument(doc)
                                        caseDict[case.sid] = (case, '')
                                else:
                                    logger.warn(
                                        'Image could not be found in Delta for file {baseFileName!s:s}.'
                                        .format(baseFileName=baseFileName))
                            elif fileType == 'XML':
                                sidData = caseDict.get(case.sid)
                                if sidData:
                                    storedcase, xmlFile = sidData
                                    caseDict[case.sid] = (storedcase, baseFileName)
                                else:
                                    caseDict[case.sid] = (case, baseFileName)
                            else:
                                logger.warn(
                                    'File type could not be determined for file {baseFileName!s:s}.'
                                    .format(baseFileName=baseFileName))
                        else:
                            logger.warn('Case not found for file {caseFileName!s:s}.'.format(caseFileName=caseFileName))
            for sid in caseDict.keys():
                case, xmlFile = caseDict[sid]
                docs = case.getDocuments().values()
                if docs:
                    for doc in docs:
                        docHistory.trackDocument(doc, docHistory.ACTION_RECONCILE)
            timestamp_suffix = today.strftime('%Y%m%d%H%M%S')
            for reconFile in reconFiles:
                CRLUtility.CRLCopyFile(reconFile,
                                       os.path.join(reconProcessedPath,
                                                    '{reconFile!s:s}_{timestamp_suffix!s:s}'
                                                    .format(reconFile=os.path.basename(reconFile),
                                                            timestamp_suffix=timestamp_suffix)),
                                       True, 5)
            logger.info(
                'Reconciliation file(s) processed, initiating retransmit analysis...')
            recs = getMissingReconList(config, docHistory, yesterday)
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
                keys = sidDocMap.keys()
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
                        sMessage += "{lastdate:%d-%b-%Y %H:%M:%S}: {filename!s:s} (Document ID {docid:d})\r\n".format(lastdate=lastdate,
                                                                                                                      filename=filename, docid=docid)
                sTitle = 'AG Reconciliation of ASAP Case Documents'
                sAddress = EMAIL_ADDRESS
                CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle,
                                        'ilsprod@crlcorp.com',
                                        'AWDApplicationSupport@aig.com,esubmission.requests@aglife.com', '')
            logger.info('Retransmit analysis complete.')
    else:
        logger.error('Recon contact not configured.')
    if fError:
        msg = 'Improperly formatted record found\n'
        for eachrec in errRecList:
            msg += str(eachrec) + '\n'
        logger.error(
            'There were one or more errors reconciling files for AG. Please take a look for the failures.\n {}'.format(msg))


def getMissingReconList(config, docHistory, yesterday):
    days60 = (yesterday - datetime.timedelta(days=60))
    sQuery = '''
        select dh.contact_id, dh.sid, dh.documentid, max(dh.actiondate) lastdate
        from {TABLE_DOCUMENT_HISTORY!s:s} dh
        where (dh.contact_id like 'agi%apps' or dh.contact_id like 'ust%apps')
        and contact_id not in ('agimtdapps')
        and dh.actionitem = '{ACTION_TRANSMIT!s:s}'
        and dh.actiondate < '{yesterday:%d-%b-%Y}'
        and dh.actiondate >= '{days60:%d-%b-%Y}'
        and not exists
        (select historyid from {TABLE_DOCUMENT_HISTORY!s:s} dh2
        where dh2.documentid = dh.documentid and dh2.sid = dh.sid
        and dh2.contact_id = dh.contact_id
        and dh2.actionitem = '{ACTION_RECONCILE!s:s}'
        and dh2.actiondate > dh.actiondate)
        and exists (select documentId from ILS_QC..tblDocuments td where td.documentId = dh.documentId)
        group by dh.contact_id, dh.sid, dh.documentid
        order by dh.contact_id, dh.sid, dh.documentid
        '''.format(TABLE_DOCUMENT_HISTORY=docHistory.TABLE_DOCUMENT_HISTORY,
                   ACTION_TRANSMIT=docHistory.ACTION_TRANSMIT,
                   yesterday=yesterday,
                   days60=days60,
                   ACTION_RECONCILE=docHistory.ACTION_RECONCILE)
    cursor = config.getCursor(config.DB_NAME_XMIT)
    cursor.execute(sQuery)
    recs = cursor.fetch()
    cursor.rollback()
    return recs


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        arg = ''
        if len(sys.argv) > 1:
            arg = sys.argv[1]
        if arg == 'recon':
            AIGRecon()
        else:
            logger.warn('Argument(s) not valid. Valid arguments:')
            logger.warn('recon')
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error')
