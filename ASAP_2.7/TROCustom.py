"""

  Facility:         ILS

  Module Name:      TROCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2009, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for TRO for ASAP processing.

  Author:
      Jarrod Wild

  Creation Date:
      13-Mar-2009

  Modification History:
      08-Nov-2010   amw     Ticket # 17221
        Added TRORecon()

      09-Nov-2010   amw
        Added Logging for recon file
        Added MTX mapping to TransmitHandler

      16-Nov-2010   amw
        Modified SQL to correctly pull documents missing recon message
        starting from the 27th of Oct 2010

      24-Nov-2010   jk
        Added a check for zip files with multiple 103's to _transmitStagedCases method.

      24-Nov-2010   jk
        Bugfix in zip file check.

      29-Nov-2010   amw    Ticket 18307
        Modified recon report to look for samples with documents new then two weeks
        added logging and fields on report to facilitate research of issues.

      06-Dec-2010   amw
        Modified to sort recon error list by date.

      21-Dec-2010   amw
        Removed Policy Number Restriction

      18-Jan-2011  mayd
        For ticket # 18174 modified end of method "_stageIndexedCase" to use the ASPAFileManager class to
        delete files.  Also removed method "_zipHasIntegrity" that checked to see if xml file was included more
        than once in zip file.  The use of the ASPAFileManager to remove the xml files will prevent this.

       04-Feb-2011  amw Ticket 20210
           Added mflagel@Aegonusa.com (Michele Flagel) to recipient list

       24-Feb-2011  mayd Ticket 20210
          Added method _isWithinTROProcessingWindow to allow for checking to see if the building and transmitting
          of the case is within the period of time TRO wants to receive files ( see method for specifics ).

       23-Mar-2011  mayd Ticket 20471
          In method "_stageIndexedCase" added method call to TROSmartParamedFileHandler to update db

       20-may-2011  gandhik   Ticket 22740
        This ticket is to update ASAP to support split delivery of documents between ASAP and ACORD based on document type.
        ACORD should be notified afetr ASAP case has been transmitted.
        Add the dtssupport@transamerica.com email id.

       3-Jun-2011   gandhik
        The index had the 'Labs' info but the labs are not transmitted with the case. Also move the code to notify ACORD, to after the
        files have been FTPed and just before moving files to sent folder.

       13-Jul-2011   amw     Ticket # 24252
         Added TransRefGuid to the recon report

       14-Jul-2011  amw    Ticket # 25779
         Removed Michele from Email Address

       27-Dec-2012  rsu    Ticket # 37457
         Add folder map for trooncrapps.

       29-Aug-2013 kg      Ticket # 43066
         Added Intelliquote (INTQ).

       19-NOV-2013 rsu     Ticket # 45139
         For MTX phase 1, deliver images for initial transmissions (V2) to TRO via email.
         TRO can handle MTX resends through the normal RIP process (V1).

         In the future, phase II will ftp initial MTX transmissions through normal ASAP process.

         NOTE: MTX was changed to use normal process in 5/14 without any changes to this script.

       06-FEB-2015 rsu     Ticket # 56107
         Temporary fix to prevent sending jumbled files or files without idx or 103 due to
         recent problems with file locking/permission issues.
         These files will need to be manually retransmitted.
       22-Sep-2015 kg      Ticket # 62940
         Nishant at Transamerica has requested to add the region id to the filenames going forward.
         Add Region id at the end of the prod_crl_aps.zip eg. prod_crl_aps_INTQ.zip
       26-Oct-2015  kg    Paperstore migration
         Added docpath for logging
       26-Dec-2017  kg    SCTASK0013233
         Changes to upload the zip files to our CRL FTP server. Another process on the new AppHub
         will send these and other TRO files to the Aegon FTP server
       14-MAY-2021 nelsonj
         Migration to new apphub and updating to python 2.7
"""


from ILS.AcordXML import *
from ILS.ASAP.Utility import ASAP_UTILITY
from ILS.ASAP.IndexHandler import ASAPIndexHandler
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler
from ILS import TROSmartParamedFileHandler
import CRLUtility
import datetime
import glob
import os

import sys
import time

if ASAP_UTILITY.devState.isDevInstance():
    ftpLogDir = r'E:\Test\Log\ILS'
    EMAIL_ADDRESS_RECON = 'nelsonj@crlcorp.com'
    EMAIL_ADDRESS_V2_TRANMISSION = 'nelsonj@crlcorp.com'
else:
    ftpLogDir = r'E:\Log\ILS'
    EMAIL_ADDRESS_RECON = 'ilsprod@crlcorp.com, dtssupport@transamerica.com'
    EMAIL_ADDRESS_V2_TRANMISSION = 'SecureNBForms@transamerica.com'

# paramiko ftp log file
ftpLogFile = os.path.join(ftpLogDir, 'ASAP_TROCustom_FTP.log')
ftpLogger = CRLUtility.CRLGetLogger(ftpLogFile, sLoggerName='ASAP_TROCustom_FTP')
ftpLogger.setLevel(CRLUtility.logging.DEBUG)

TRO_V1_TYPE_MAP = {
    'MISC DOC': 'NBDOCS',
    'HIVCONSNT': 'HIVCONSNT',
    'REPL FORM': 'REPLFORMS',
    'NY REPL FRM': 'REPLFORMS',
    'APP': 'APP',
    'APP Part II': 'EXAM',
    'NONMED': 'EXAM',
    'PACFORM': 'NBDOCS',
    'CHLD RDR': 'NBDOCS',
    'NY DEF REPL': 'REPLFORMS',
    'DISCL': 'DISCLFORM',
    'NY DISCL': 'DISCLFORM',
    'NOTCEDISCL': 'DISCLFORM',
    'FIN SUPP': 'NBDOCS',
    'HIPAA AUTH': 'HIPAAAUTH',
    'NY SLS CKLIST': 'NBDOCS',
    'CC AUTH': 'NBDOCS',
    'SUPP APP': 'NBDOCS',
    'TRUST FRM': 'NBDOCS',
    'SAVE AGE': 'NBDOCS',
    'MIL DISCL FRM': 'DISCLFORM',
    'AVATN QUES': 'RESDNTQUES',
    'SPRT QUES': 'RESDNTQUES',
    'RSDNT FRM': 'RESDNTQUES',
    'NY RSDNT FRM': 'RESDNTQUES',
    'SMK QUES': 'RESDNTQUES',
    'MEDCLQUES': 'RESDNTQUES',
    'ADB': 'NBDOCS',
    'NY DISCL STMT': 'DISCLFORM',
    'Lab Slip': 'LABSLIP',
    'CONDRECPT': 'NBDOCS',
    'AGTREPORT': 'NBDOCS',
    'CORRESPOND': 'NBDOCS',
    'COVERLETTR': 'NBDOCS',
    'DMVAUTH': 'NBDOCS',
    'FIDUCIARY': 'NBDOCS',
    'IR': 'NBDOCS',
    'PROOFAGE': 'NBDOCS',
    'EKG': 'NBDOCS',
    'TREADMILL': 'NBDOCS',
    'CHECKCOPY': 'NBDOCS',
    'VOIDCHECK': 'NBDOCS',
    'Labs': 'LABS'
}

TRO_NY_TYPE_MAP = {
    'DISCL': 'NY DISCL STMT',
    'REPL FORM': 'NY REPL FRM',
    'RSDNT FRM': 'NY RSDNT FRM',
}


class TROIndexHandler(ASAPIndexHandler):
    """
    Custom handler for building indexes for TRO.
    """

    def _isReadyToIndex(self):
        fReady = False
        if self._isFullTransmit():
            case = self._getCase()
            xmitDir = case.contact.xmit_dir
            txtReport = os.path.join(os.path.dirname(xmitDir), 'reports', '{sid!s:s}.txt'.format(sid=case.sid))
            if (len(glob.glob(txtReport[:-4] + '.*')) > 0 or
                    CRLUtility.CRLOasisTextReportForSid(case.sid, 'ILS', txtReport)):
                fReady = True
        else:
            fReady = True
        return fReady

    def _postProcessIndex(self):
        idxPaths = self._getIndexPaths()
        if idxPaths and os.path.isfile(idxPaths[0]):
            # replace 'intermediary' index with either version 1 (AWD/RIP) index
            # or version 2 index
            idxFile = idxPaths[0]
            docs = list(self._getCase().getDocuments().values())
            docdates = [CRLUtility.ParseStrDate(doc.getDateCreated(), True) for doc in docs]
            docdates.sort()
            docTypeMap = self._getCase().contact.docTypeNameMap
            ptr = open(idxFile, 'r')
            lines = ptr.readlines()
            ptr.close()
            caseFields = lines[0].strip()
            delim = ', '
            subdelim = '='
            # this section of code to build the fieldlist must be done because
            # some field values might have commas in them, and they need to be
            # identified and joined with their erroneously separated prior field
            templist = caseFields.split(delim)
            fieldlist = []
            for field in templist:
                if field.find(subdelim) < 0:
                    fieldlist[-1] = '{name!s:s}{delim!s:s}{field!s:s}'.format(name=fieldlist[-1], delim=delim, field=field)
                else:
                    fieldlist.append(field)
            #
            # If 103 is present, use Policy Number from Recon File (stored in acord103 table) instead of from 121
            # so that retransmits will have the updated policy number.
            #
            if (self._getCase().contact.acord103_dir):
                Acord103Store = ASAP_UTILITY.getASAPAcord103Store()
                Acord103s = Acord103Store.getByTrackingId(self._getCase().trackingId)
                polNum103 = Acord103s[0].policyNumber
            else:
                polNum103 = None

            state = fieldlist[3].split(subdelim)[1].strip()
            caseFields = delim.join(fieldlist[:3])

            # full transmits are version 2 if no 103 or if there is no policy number with the 103
            troVersion = 1  # indexing version sent via rip process
            if self._isFullTransmit():
                if not self._getCase().contact.acord103_dir:
                    troVersion = 2
                elif not polNum103:
                    troVersion = 2

            troLines = []
            if troVersion == 1:
                troLines.append("@@BEGIN")
                troLines.append("@@KEY UNIT=NBUKC, WRKT=MAIL, STAT=RIPPED, ACTION=W")
                troLines.append("@@LOB {fields!s:s}".format(fields=caseFields.upper()))
            if docs:
                troDocList = []
                for doc in docs:
                    troDocType = docTypeMap.get(doc.getDocTypeName())
                    nyType = TRO_NY_TYPE_MAP.get(troDocType)
                    if state.upper() == 'NEW YORK' and nyType:
                        troDocType = nyType
                    if troDocType == 'APP':
                        troDocList = [(troDocType, doc.fileName)] + troDocList
                    else:
                        troDocList.append((troDocType, doc.fileName))

                for troDocType, fileName in troDocList:
                    if troVersion == 1:
                        troDocType = TRO_V1_TYPE_MAP.get(troDocType)
                        troLines.append("@@BEGIN")
                        troLines.append("@@KEY UNIT=NBUKC, OBJT={troDocType!s:s}, ACTION=S".format(troDocType=troDocType))
                        troLines.append("@@FILE=P:\\CRL\\TIF\\{fileName!s:s}".format(fileName=fileName.upper()))
                        troLines.append("@@END")
                    else:
                        troLines.append("{troDocType!s:s}={fileName!s:s}".format(troDocType=troDocType, fileName=fileName.upper()))

            if troVersion == 1:
                troLines.append("@@END")

            with open(idxFile, 'w') as ptr:
                for line in troLines:
                    ptr.write("{}\n".format(line))
        return True


class TROTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for TRO transmission.
    """

    def _preStage(self):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitUnzipPath = os.path.join(xmitStagingPath, 'unzip')
        reviewPath = os.path.join(xmitStagingPath, 'review')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        try:
            # image the text reports
            from ILS import TROImageReports
            troFolderMap = {
                'troctrlapps': 'TRO_CTRL',
                'troslqapps': 'TRO_SLQ',
                'troampcapps': 'TRO_AMPC',
                'troipsapps': 'TRO_IPS',
                'tromtxapps': 'TRO_MTX',
                'trooncrapps': 'TRO_ONCR',
                'trointqapps': 'TRO_INTQ'
            }
            TROImageReports.ProcessReports(troFolderMap.get(contact.contact_id))

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

            # move any zip files to reviewPath folder
            zipFiles = fm.glob(os.path.join(xmitZipPath, '*.*'))
            for zipFile in zipFiles:
                CRLUtility.CRLCopyFile(zipFile.getFullPath(),
                                       os.path.join(reviewPath, zipFile.fileName),
                                       True, 5)
            if len(zipFiles) > 0:
                self._getLogger().error(
                    'Zip files were left behind in ' +
                    '{xmitZipPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitZipPath=xmitZipPath))

            # move any unzip files to reviewPath folder
            if os.path.isdir(xmitUnzipPath):
                unzipFiles = fm.glob(os.path.join(xmitUnzipPath, '*.*'))
                for unzipFile in unzipFiles:
                    CRLUtility.CRLCopyFile(unzipFile.getFullPath(),
                                           os.path.join(reviewPath, unzipFile.fileName),
                                           True, 5)
                if len(unzipFiles) > 0:
                    self._getLogger().error(
                        'Unzip files were left behind in ' +
                        '{xmitUnzipPath!s:s} from a previous run and have been moved to the review subfolder.'
                        .format(xmitUnzipPath=xmitUnzipPath))

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
        case = self._getCurrentCase()
        xmitDir = case.contact.xmit_dir

        if (_isWithinTROProcessingWindow()):
            if not self._isFullTransmit() or glob.glob(os.path.join(os.path.dirname(xmitDir),
                                                                    'reports',
                                                                    '{sid!s:s}.tif'.format(sid=case.sid))):
                fReady = True
        return fReady

    def _stageIndexedCase(self):
        case = self._getCurrentCase()
        fSuccess = True
        troVersion = 1  # indexing version
        asapContact = self._getContact()
        asapFileManager = ASAP_UTILITY.getASAPFileManager(asapContact)

        fromToMoves = []
        filesToDelete = []
        # get case file
        idxPath = os.path.join(case.contact.index_dir,
                               '{trackingId!s:s}.IDX'.format(trackingId=case.trackingId))
        zipPath = ''
        if os.path.isfile(idxPath):
            ptr = open(idxPath, 'r')
            line = ptr.readline()
            ptr.close()
            ext = 'DAT'
            if line.strip() != '@@BEGIN':
                troVersion = 2
                zipPath = 'zip'
                ext = 'IDX'
            xmitIdxPath = os.path.join(case.contact.xmit_dir, zipPath,
                                       '{trackingId!s:s}.{ext!s:s}'.format(trackingId=case.trackingId, ext=ext))
            fromToMoves.append((idxPath, xmitIdxPath))
        else:
            fSuccess = False
            self._getLogger().warn(
                'Failed to find index file for case ({sid!s:s}/{trackingId!s:s}).'
                .format(sid=case.sid, trackingId=case.trackingId))
        # now try to get docs
        documents = list(case.getDocuments().values())
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            if os.path.isfile(docPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir, zipPath, os.path.basename(docPath))
                fromToMoves.append((docPath, xmitDocPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find indexed image for docid {docId:d} (sid {sid!s:s}) and Docpath: {docPath!s:s}.'
                    .format(docId=doc.getDocumentId(), sid=case.sid, docPath=docPath))

        if self._isFullTransmit():
            filesToDelete.append((os.path.join(os.path.dirname(case.contact.xmit_dir), 'reports', '{sid!s:s}.tif'.format(sid=case.sid))))

        if self._isFullTransmit() and case.contact.acord103_dir:  # process 103 is present
            acord103Path = os.path.join(case.contact.acord103_dir, '{trackingId!s:s}.xml'.format(trackingId=case.trackingId))
            self._getLogger().info('Appending {path!s:s} to filesToDelete'.format(path=os.path.join(os.path.dirname(case.contact.xmit_dir), 'reports', '{sid!s:s}.tif'.format(sid=case.sid))))
            if troVersion == 2:
                p = AcordXMLParser()
                h = p.parse(acord103Path)
                party = h.txList[0].getElement('ACORDInsuredParty')
                if party:
                    # temporary band-aid to remove empty OLifEExtension object from Risk
                    # Insurance Central shouldn't be including this
                    risk = party.getElement('Risk')
                    if risk:
                        ext = risk.getElement('OLifEExtension')
                        if ext and not ext.value and not len(ext.getElements()):
                            risk.removeElement(ext)
                    # Strip insured zip code to first 5 digits (Transamerica request)
                    zip_code = party.getElement('Address.Zip')
                    if zip_code:
                        zip_code.value = zip_code.value[:5]
                # Strip owner zip code to first 5 digits (Transamerica request)
                owner = h.txList[0].getElement('ACORDOwnerParty')
                if owner:
                    zip_code = owner.getElement('Address.Zip')
                    if zip_code:
                        zip_code.value = zip_code.value[:5]
                agency = h.txList[0].getElement('ACORDAgencyParty')
                if agency:
                    name = agency.getElement('FullName')
                    if name.value == 'Insurance Central':
                        # temporary band-aid for another Insurance Central issue:
                        # agency ID should be 12618, and agent ID should be 35889
                        producerId = agency.getElement('Producer.CarrierAppointment.CompanyProducerID')
                        if producerId:
                            producerId.value = '12618'
                        agent = h.txList[0].getElement('ACORDAgentParty')
                        if agent:
                            producerId = agent.getElement('Producer.CarrierAppointment.CompanyProducerID')
                            if producerId:
                                producerId.value = '35889'
                        # if GovtIDTC is missing, add it (Insurance Central)
                        if party:
                            govtIdTc = party.getElement('GovtIDTC')
                            if not govtIdTc:
                                govtId = party.getElement('GovtID')
                                if govtId:
                                    elems = party.getElements()
                                    idx = elems.index(govtId)
                                    govtIdTc = AcordXMLElement()
                                    govtIdTc.name = 'GovtIDTC'
                                    govtIdTc.value = 'Social Security Number'
                                    govtIdTc.getAttrs()['tc'] = '1'
                                    elems.insert(idx + 1, govtIdTc)
                # lookup Holding to make adjustments per Transamerica
                holding = h.txList[0].getElement('ACORDInsuredHolding')
                if holding:
                    # first remove Attachment objects under Holding, per Transamerica
                    attachments = []
                    for elem in holding.getElements():
                        if elem.name == 'Attachment':
                            attachments.append(elem)
                    for attachment in attachments:
                        holding.removeElement(attachment)
                    # next add SubmissionDate element if not present, and set it to
                    # today's date
                    appinfo = holding.getElement('Policy.ApplicationInfo')
                    submitdate = appinfo.getElement('SubmissionDate')
                    if not submitdate:
                        signeddate = appinfo.getElement('SignedDate')
                        elems = appinfo.getElements()
                        idx = elems.index(signeddate)
                        submitdate = AcordXMLElement()
                        submitdate.name = 'SubmissionDate'
                        elems.insert(idx + 1, submitdate)
                    submitdate.value = datetime.datetime.today().strftime('%Y-%m-%d')
                    # if PaymentMethod is present and tc set to 7 (EFT), then set to 26 (PAC)
                    paymentmethod = holding.getElement('Policy.PaymentMethod')
                    if paymentmethod:
                        attrs = paymentmethod.getAttrs()
                        if attrs and attrs.get('tc') == '7':
                            attrs['tc'] = '26'
                            paymentmethod.value = 'PAC'
                    # for SelectQuote, copy TrackingID into TransRefGUID
                    if agency:
                        name = agency.getElement('FullName')
                        if name.value == 'SelectQuote Insurance':
                            trackingId = appinfo.getElement('TrackingID')
                            transRefGuid = h.txList[0].getElement('TXLifeRequest.TransRefGUID')
                            transRefGuid.value = trackingId.value
                    h.writeXML(acord103Path)
                fromToMoves.append((acord103Path,
                                    os.path.join(case.contact.xmit_dir, zipPath,
                                                 '{trackingId!s:s}.xml'.format(trackingId=case.trackingId))))
        if fSuccess:
            '''
               Need to inidcate  that it's okay to  tranmsit the smart paramed file since the case is being transmitted...
            '''
            TROSmartParamedFileHandler.updateASAPSmartParamedFileInfoInDatabase(case.trackingId)

            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)

            if troVersion == 2:
                zipFileName = 'CRL_APS_{trackingId!s:s}.ZIP'.format(trackingId=case.trackingId)
                if isinstance(self, TROV2EmailTransmitHandler):
                    zipFileName = 'CRL_EMAIL_{trackingId!s:s}.ZIP'.format(trackingId=case.trackingId)
                asapFileList = asapFileManager.glob(os.path.join(case.contact.xmit_dir, zipPath, '*.*'))
                self._getLogger().debug('Getting files to zip in {zipPath!s:s}, zip file name: {zipFileName!s:s}'.format(zipPath=zipPath, zipFileName=zipFileName))
                for asapFile in asapFileList:
                    fullPath = asapFile.getFullPath()
                    self._getLogger().debug('adding file to zipfile: {path!s:s}'.format(path=fullPath))
                    CRLUtility.CRLAddToZIPFile(fullPath,
                                               os.path.join(case.contact.xmit_dir, zipFileName),
                                               False)
                    self._getLogger().debug('adding file to ASAP file manager: ' + str(fullPath))
                    asapFileManager.deleteFile(asapFile)

            # Delete the labreports here as these will not be sent by the ASAP but ACORD
            for eachFile in filesToDelete:
                CRLUtility.CRLDeleteFile(eachFile)
                self._getLogger().info('file deleted: {eachFile!s:s}'.format(eachFile=eachFile))
        return fSuccess

    def _transmitStagedCases(self):
        global troFtpLock
        import threading
        try:
            troFtpLock
        except NameError:
            troFtpLock = threading.RLock()
        fSuccess = True

        contact = self._getContact()
        regionid = contact.region_id
        self._getLogger().info(
            'Adding Region id to TRO filenames...')

        CRL_FTP_SERVER, CRL_FTP_USER, CRL_FTP_USER_PASSWORD = CRLUtility.CRLGetFTPHostInfo('CRLCORP2')
        TRO_V1_ZIPFILENAME = 'CRL_APS.ZIP'
        TRO_V2_ZIPFILENAME = 'prod_crl_aps_{region_id!s:s}.zip'.format(region_id=regionid.lower().strip())
        TRO_V2_EMAIL_ZIPFILENAME_PREFIX = 'CRL_EMAIL'

        if ASAP_UTILITY.devState.isDevInstance():
            logger.debug('In Test Instance')
            TRO_V1_ZIPFILENAME = 'MOD_CRL_APS.ZIP'
            TRO_V2_ZIPFILENAME = 'mod_crl_aps_{region_id!s:s}.zip'.format(region_id=regionid.lower().strip())

        self._getLogger().info(
            'TRO file name changed to {newFileName!s:s}'.format(newFileName=TRO_V2_ZIPFILENAME))

        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitRetransPath = os.path.join(xmitStagingPath, 'retrans')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')

        allFiles = glob.glob(os.path.join(xmitStagingPath, '*.*'))
        for fileItem in allFiles:
            if fileItem[-3:].upper() != 'ZIP':
                CRLUtility.CRLAddToZIPFile(fileItem,
                                           os.path.join(xmitStagingPath, TRO_V1_ZIPFILENAME))
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        asapToXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.ZIP'))
        xmitZipFile = None
        if (len(asapToXmitFiles) > 0):
            self._getLogger().info(
                'There are {count:d} zip files in the transmit staging folder to process...'
                .format(count=len(asapToXmitFiles)))
            for asapZipFile in asapToXmitFiles:
                today = datetime.datetime.today()
                # Make sure zip does not have jumbled cases or is idx, xml, dat files required for transmission.
                if (not self._zipHasIntegrity(asapZipFile)):
                    self._getLogger().error("Invalid zip file detected {path!s:s}".format(path=asapZipFile.getFullPath()))
                    ftpLogger.error("Invalid zip file detected {path!s:s}".format(path=asapZipFile.getFullPath()))
                    # do not transmit invalid zip file should be moved to review folder in the next run
                    # RSU next version straighten out fSuccess in this method
                    # fSuccess = False
                    continue

                if (not asapZipFile.fileName.startswith(TRO_V2_EMAIL_ZIPFILENAME_PREFIX)):  # ftp
                    xmitZipFile = os.path.join(xmitZipPath, asapZipFile.fileName)
                    aegonFileName = str(TRO_V2_ZIPFILENAME.split('.')[0]) + '_' + today.strftime('%Y%m%d%H%M%S') + '.ZIP'
                    if asapZipFile.fileName == TRO_V1_ZIPFILENAME:
                        aegonFileName = TRO_V1_ZIPFILENAME.split('.')[0] + '_' + today.strftime('%Y%m%d%H%M%S') + '.ZIP'
                    fm.moveFile(asapZipFile, xmitZipFile)
                    sentZipFile = '{basename!s:s}_{dt:%Y%m%d%H%M%S}.ZIP'.format(basename=asapZipFile.fileName.split('.')[0],
                                                                                dt=today)
                    # -- use SFTP/SSH to transfer the file to Aegon's FTP site,
                    #    only if we're not in test mode
                    finalZipPath = xmitSentPath
                    troFtpLock.acquire()
                    self._getLogger().info("FTP Lock acquired by thread {contact_id!s:s}..."
                                           .format(contact_id=contact.contact_id))
                    ftpLogger.debug("About to ftp {contact_id!s:s} file {path!s:s}"
                                    .format(contact_id=contact.contact_id, path=asapZipFile.fileName))
                    try:
                        if os.path.isfile(xmitZipFile):
                            # capture paramiko logging to a stream which will later get written to the ftpLogger
                            # can't just have paramiko log to the ftpLogger since it rewrites the log file every time
                            # and also needed to reimport paramiko to get this to work
                            # 26-Dec-2017
                            # With the changes to the TRO server we are not able to upload the files using the OLD Apphub
                            # and as ASAP is still running on the old AppHub we shall place all the files in one location and
                            # another script will upload them to The Aegon New server.
                            if not ASAP_UTILITY.devState.isDevInstance():
                                serverPath = '/TROftp1/' + aegonFileName
                            else:
                                serverPath = '/TROftp1/Test/' + aegonFileName
                            self._getLogger().info("About to ftp {contact_id!s:s} file {path!s:s} as {destPath!s:s}"
                                                   .format(contact_id=contact.contact_id,
                                                           path=asapZipFile.fileName,
                                                           destPath=aegonFileName))

                            CRLUtility.CRLFTPPut(CRL_FTP_SERVER, asapZipFile.getFullPath(),
                                                 serverPath, 'b', CRL_FTP_USER, CRL_FTP_USER_PASSWORD)
                            self._getLogger().info("{path!s:s} Transmitted to CRL Server".format(path=asapZipFile.fileName))

                    except IOError as ioe:
                        if not str(ioe.args[0]).startswith('size mismatch in put'):
                            finalZipPath = xmitRetransPath
                            sentZipFile = asapZipFile.fileName
                            fSuccess = False
                            self._getLogger().warn('Failed to FTP file {path!s:s} to TRO (moving to retrans folder)'
                                                   .format(path=asapZipFile.fileName),
                                                   exc_info=True)
                    except:
                        finalZipPath = xmitRetransPath
                        sentZipFile = asapZipFile.fileName
                        fSuccess = False
                        self._getLogger().warn('Failed to FTP file {path!s:s} to TRO (moving to retrans folder)'
                                               .format(path=asapZipFile.fileName),
                                               exc_info=True)
                    finally:

                        time.sleep(30.0)  # sleep for 30 seconds between ftp's per AEGON requirement
                        troFtpLock.release()
                        self._getLogger().info("FTP Lock released by thread {contact_id!s:s}."
                                               .format(contact_id=contact.contact_id))

                else:  # send by email
                    try:
                        xmitZipFile = os.path.join(xmitZipPath, asapZipFile.fileName)
                        fSuccess = self.transmitByEmail(asapZipFile)
                        if fSuccess:
                            fm.moveFile(asapZipFile, xmitZipFile)
                            finalZipPath = xmitSentPath
                            sentZipFile = '{path!s:s}_{dt:%Y%m%d%H%M%S}.ZIP'.format(path=asapZipFile.fileName.split('.')[0],
                                                                                    dt=today)
                        else:
                            finalZipPath = xmitRetransPath
                            sentZipFile = asapZipFile.fileName
                            self._getLogger().warn('Failed to email file {path!s:s} to TRO (moving to retrans folder)'
                                                   .format(path=asapZipFile.fileName))

                    except:
                        finalZipPath = xmitRetransPath
                        sentZipFile = asapZipFile.fileName
                        fSuccess = False
                        self._getLogger().warn('Failed to email file {path!s:s} to TRO (moving to retrans folder)'
                                               .format(path=asapZipFile.fileName),
                                               exc_info=True)

                # Send the notification to ACORD that the case has been uploaded
                # This is done only for version 2 files as version 1 files are basically retransmits
                # Version 2 files have tracking id in the filenames so extract from the name.
                if (fSuccess):
                    file_nm = asapZipFile.fileName.split('.')[0]
                    if len(file_nm.split('_')) == 3:
                        tracking_id = file_nm.split('_')[2]
                        req = ASAP_UTILITY.getASAPAcordRequest()
                        if not req.makeRequestByTrackingId(tracking_id):
                            fSuccess = False
                            self._getLogger().info("Error while notifying ACORD for {tracking_id!s:s}.".format(tracking_id=tracking_id))
                        else:
                            self._getLogger().info("ACORD Notified that ASAP case sent.")

                # move zip file to either sent or retrans folder
                if xmitZipFile:
                    fm.moveFile(fm.newFile(xmitZipFile, True), os.path.join(finalZipPath, sentZipFile))
                    self._getLogger().info("File moved to SENT Folder {zipFile!s:s}, {zipPath!s:s}."
                                           .format(zipFile=xmitZipFile, zipPath=os.path.join(finalZipPath, sentZipFile)))

        return fSuccess

    def transmitByEmail(self, asapZipFile):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitUnzipPath = os.path.join(xmitStagingPath, 'unzip')

        try:
            # copy to unzip folder but keep in xmitStagingPath for other processing in _transmitStagedCases
            CRLUtility.CRLUnzipFile(asapZipFile.getFullPath(), xmitUnzipPath)

            # collect tif files for email attachments
            attachments = []
            fm = ASAP_UTILITY.getASAPFileManager(contact)
            toXmitTifs = fm.glob(os.path.join(xmitUnzipPath, '*.TIF'))
            for toXmitTif in toXmitTifs:
                attachments.append(toXmitTif.getFullPath())

            # extract tif file names and document types from idx file
            docnames = []
            toXmitIdxFile = fm.glob(os.path.join(xmitUnzipPath, '*.IDX'))
            if toXmitIdxFile and toXmitIdxFile[0] and os.path.isfile(toXmitIdxFile[0].getFullPath()):
                ptr = open(toXmitIdxFile[0].getFullPath(), 'r')
                docnames = ptr.readlines()
                ptr.close()

            trackingId = ''
            lastName = ''
            firstName = ''
            case = None
            if len(asapZipFile.fileName.split('_')) == 3:
                trackingId = asapZipFile.fileName.split('_')[2]
                trackingId = trackingId.split('.')[0]
                fact = ASAP_UTILITY.getViableCaseFactory()
                case = fact.fromTrackingID(trackingId)
                if case and case.order:
                    lastName = case.order.lastName
                    firstName = case.order.firstName

            sMessage = '\n'
            sMessage += 'Insured Name: {fname!s:s} {lname!s:s}\n\n'.format(fname=firstName, lname=lastName)
            sMessage += 'Agency: {region_id!s:s}\n\n'.format(region_id=contact.region_id)
            sMessage += 'Case Tracking id: {trackingId!s:s}\n\n'.format(trackingId=trackingId)
            sMessage += 'Number of documents: {count:d}\n\n'.format(count=len(attachments))
            sMessage += 'Documents attached: \n'
            for docname in docnames:
                sMessage += '  {docname!s:s}\n'.format(docname=docname.strip())

            if case:
                sTitle = 'CRL {region_id!s:s} ASAP Case - {firstName!s:s} {lastName!s:s}'.format(region_id=contact.region_id,
                                                                                                 firstName=case.order.firstName,
                                                                                                 lastName=case.order.lastName)

                sAddress = EMAIL_ADDRESS_V2_TRANMISSION
                if ASAP_UTILITY.devState.isDevInstance():
                    sAddress = ''

                CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle, 'ilsprod@crlcorp.com',
                                        '', '', *attachments)

                # unzip files no longer needed, so remove them
                unzipFiles = fm.glob(os.path.join(xmitUnzipPath, '*.*'))
                for unzipFile in unzipFiles:
                    fm.deleteFile(unzipFile)

        except:
            fSuccess = False
            self._getLogger().warn('Failed to email file {fileName!s:s} to TRO'.format(fileName=asapZipFile.fileName), exc_info=True)

        return fSuccess

    def _zipHasIntegrity(self, zipFile):
        """

        Returns True if given zip file has valid contents.
        If original transmission with 103 (Version 2), there must be one xml file and one idx file
        If retransmission (Version 1), there must be one or more dat files
        If original transmission without 103 (TROV2EmailTransmitHandler), must be one idx file

        This is meant to prevent transmission of jumbled files in a single zip or a zip file with tifs and no index.

        """
        fSuccess = False

        try:
            # first log all files in the zip
            filesInZip = CRLUtility.CRLSearchZip(zipFile.getFullPath(), '*.*')

            idxCount = 0
            xmlCount = 0
            datCount = 0

            for fileInZip in filesInZip:
                ext = os.path.splitext(os.path.basename(fileInZip[1]))[1]
                ftpLogger.debug('Checking integrity of zip file: {zipPath!s:s} contains file: {zipFile!s:s}'
                                .format(zipPath=zipFile.fileName, zipFile=fileInZip[1]))

                if ext.upper() == '.IDX':
                    idxCount += 1
                elif ext.upper() == '.XML':
                    xmlCount += 1
                elif ext.upper() == '.DAT':
                    datCount += 1

                    # if isinstance(self, TROTransmitHandler):

            # must have one idx and one xml, or one or more dat
            if idxCount == 1 and xmlCount == 1 and datCount == 0:
                fSuccess = True
            elif datCount >= 1 and idxCount == 0 and xmlCount == 0:
                fSuccess = True

                # else: #TROV2EmailTransmitHandler
            #    if idxCount == 1:
            #        fSuccess = True

        except Exception:
            self._getLogger().warn('Error occurred while integrity-checking {zipFile!s:s}'.format(zipFile=zipFile), exc_info=True)
            raise

        return fSuccess


class TROV2EmailTransmitHandler(TROTransmitHandler):
    """
    Subclass to send initial transmission (V2) via email
    """


def TRORecon():
    """
    Perform reconciliation of related TRO documents for ASAP.
    """
    logger = CRLUtility.CRLGetLogger()
    # logger.setLevel(logging.DEBUG)
    today = datetime.datetime.today()
    # yesterday = CRLUtility.CRLGetBusinessDayOffset(today, -1)
    config = ASAP_UTILITY.getXmitConfig()
    #
    # Directories Constants
    # (note: single recon for Client
    #       will be placed into 1 asap region,
    #       although the recon is for multiple regions)
    #
    reconContact = config.getContact('TRO', 'SLQ', 'APPS')
    if reconContact:
        reconStagingFolder = os.path.join(os.path.dirname(reconContact.document_dir), 'recon')
        reconProcessedFolder = os.path.join(reconStagingFolder, config.getSetting(config.SETTING_PROCESSED_SUBDIR))
        #
        # Initialize Asap Function Classes
        #
        docFactory = ASAP_UTILITY.getASAPDocumentFactory()
        caseFactory = ASAP_UTILITY.getASAPCaseFactory()
        viableFactory = ASAP_UTILITY.getViableCaseFactory()
        docHistory = ASAP_UTILITY.getASAPDocumentHistory()
        acord103Store = ASAP_UTILITY.getASAPAcord103Store()

        if not os.path.isdir(reconProcessedFolder):
            os.makedirs(reconProcessedFolder)
        #
        # Retrieve Reconciliation Data
        #
        files = glob.glob(os.path.join(reconStagingFolder, '*.txt'))
        if files:
            logger.info('processing {files!s:s}'.format(files=files))
        for reconFile in files:
            baseFileName = os.path.basename(reconFile)
            filePtr = open(reconFile, 'r')
            data = filePtr.read()
            filePtr.close()
            fError = False
            if data:
                policyNumAndTrackingIds = [policyNumAndTrackingId.split(',') for policyNumAndTrackingId in data.strip().split('\n')]
            else:
                policyNumAndTrackingIds = []
                logger.warn('{reconFile!s:s} is a 0 byte file'.format(reconFile=reconFile))
            logger.info('processing {count:d} recon entries'.format(count=len(policyNumAndTrackingIds)))
            asapDocuments = []
            #
            # Process Recon Data
            #
            for transRefGuid, policyNum in policyNumAndTrackingIds:
                policyNum = policyNum.strip()
                transRefGuid = transRefGuid.strip()
                #
                # Since 103 Tracking Id is being placed into transrefguid for SLQ at transmittal we are looking
                # at 103 tracking id first then the original transrefguid to catch non-SLQ cases
                #
                acord103s = acord103Store.getByTrackingId103(transRefGuid)
                if not acord103s:
                    acord103s = acord103Store.getByTransRefGuid(transRefGuid)

                if not acord103s:
                    logger.error('unable to reconcile transRefGuid: {transRefGuid!s:s}'.format(transRefGuid=transRefGuid))
                    continue

                if not acord103s[0].trackingId:
                    logger.error('unable to retrieve trackingId from transRefGuid: {transRefGuid!s:s}'.format(transRefGuid=transRefGuid))
                    continue
                case = viableFactory.fromTrackingID(acord103s[0].trackingId)
                if case:
                    acord103Store.setPolicyNumber(policyNum, acord103s[0])
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
                    logger.warn("Case not found for Tracking ID {trackingI!s:s}.".format(trackingI=acord103s[0].trackingId))
                    fError = True
            #
            # Mark cases as Reconciled
            #
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
            logger.info(
                'Reconciliation file(s) processed, initiating retransmit analysis...')
            sQuery = """
                select dh.sid, count(dh.documentid), max(dh.actiondate) lastdate
                from {table!s:s} dh
                where dh.contact_id like 'tro%'
                and dh.actionitem = '{action_xmit!s:s}'
                and dh.actiondate >= '{dt1:%d-%b-%Y}'
                and dh.actiondate < '{dt2:%d-%b-%Y}'
                and not exists (select historyid from {table!s:s} dh2
                                where dh2.sid = dh.sid
                                and dh2.contact_id = dh.contact_id
                                and (dh2.actionitem = '{action_recon!s:s}'
                                     or dh2.actiondate <= '23-OCT-2010'))
                group by dh.sid
                order by lastdate, dh.sid
                """.format(table=docHistory.TABLE_DOCUMENT_HISTORY,
                           action_xmit=docHistory.ACTION_TRANSMIT,
                           dt1=(today - datetime.timedelta(days=14)),
                           dt2=today,
                           action_recon=docHistory.ACTION_RECONCILE)
            cursor = config.getCursor(config.DB_NAME_XMIT)
            iRet = cursor.execute(sQuery)
            if iRet != 0:
                logger.warn('{sQuery!s:s} returned {iRet!s:s} value when executed'.format(sQuery=sQuery, iRet=iRet))
            logger.debug(sQuery)
            recs = cursor.fetch()
            cursor.rollback()
            #
            # Check for missed documents
            #
            casedict = {}
            sidOrder = []
            if recs:
                logger.debug('records found from above query: {recs!s:s}'.format(recs=recs))
                for sid, docCount, lastdate in recs:
                    case = caseFactory.fromSid(sid)
                    if case:
                        sidOrder.append(sid)
                        casedict[sid] = case
                        #
                        # Add Fields (not native to caseFactory)
                        # to consolidate locations (lastDate, trackingId103, transRefGuid)
                        #
                        case.lastDate = str(lastdate)
                        case.docCount = docCount
                        acord103s = acord103Store.getByTrackingId(case.trackingId)
                        if acord103s:
                            case.trackingId103 = acord103s[0].trackingId103
                            case.transRefGuid = acord103s[0].transRefGuid
                        else:
                            case.trackingId103 = 'n/a'
                            case.transRefGuid = 'n/a'
            #
            # Only when at least one recon file has been processed, send
            # an email notification of missing documents, if any.
            #
            sMessage = '\n'
            if casedict:
                sMessage += 'The following document files related to ASAP cases did '
                sMessage += 'not successfully transmit to Transamerica(TRO):\n\n'
                for sid in sidOrder:
                    case = casedict[sid]
                    sMessage += ('Sid {sid:>10}, Tracking Id: {trackingId:>12}, Acord 103 Tracking Id: {trackingId103:>15}, TransRefGuid: {transRefGuid:>15}, count of Documents: {docCount:>2}, transmit date: "{lastDate:>24}"\n'
                                 .format(sid=case.sid, trackingId=case.trackingId, trackingId103=case.trackingId103, transRefGuid=case.transRefGuid, docCount=case.docCount, lastDate=case.lastDate))
                logger.warn(sMessage)
            else:
                sMessage += 'Transamerica(TRO) reconciliation of documents related to ASAP cases '
                sMessage += 'completed successfully with no discrepancies.'
            sTitle = 'TRO Reconciliation of ASAP Case Documents'
            sAddress = EMAIL_ADDRESS_RECON
            CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle,
                                    'noreply@crlcorp.com', '', '')
    else:
        logger.error('Recon contact not configured.')


def _isWithinTROProcessingWindow(timeTupleIn=time.localtime()):
    """ Per Michele Flagel: timeframe is
    6 AM to  8:30 PM CST Monday thru Friday and
    6 AM to 12:30 PM CST on Saturday
    """
    returnFlag = False
    monday, tuesday, wednesday, thursday, friday, saturday, sunday = list(range(7))

    # break the tuple into its differnt time parts so can compare...
    year, month, day, hour, minute, sec, weekday, yearday, dst = timeTupleIn
    # print 'hour=' + str(hour) + ', min=' + str(min)

    # from time import strftime
    # print strftime("%a, %d %b %Y %H:%M:%S", timeTupleIn)

    if (weekday < saturday):
        if (5 < hour < 20):
            returnFlag = True
        elif (hour == 20 and minute < 31):
            returnFlag = True
    if (weekday == saturday):
        if (5 < hour < 12):
            returnFlag = True
        elif (hour == 12 and minute < 31):
            returnFlag = True

    return returnFlag


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        arg = ''
        # arg = 'recon'
        if len(sys.argv) > 1:
            arg = sys.argv[1]
        if arg == 'recon':
            TRORecon()
        else:
            logger.warn('Argument(s) not valid. Valid arguments:')
            logger.warn('recon')
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error in TROCustom.py')
