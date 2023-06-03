"""

  Facility:         ILS

  Module Name:      MainHandler

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPMainHandler class.

  Author:
      Jarrod Wild

  Creation Date:
      07-Nov-2006

  Modification History:
      13-Aug-2007   jk   Ticket # 57401
          Changed calls of set_j_hold() and is_on_j_hold() to set_K_hold() and is_on_K_hold().

      17-Mar-2011   ckk  Ticket # 21140
          Remove Jay Wiener from "ASAP Cases Released but Moved to Pending" report.

      14-Sep-2011   rsu  Ticket # 25772
          Add Denise Perricone to "ASAP Cases Released but Moved to Pending" report.

      01-JUL-2015   rsu  Ticket # 60581
          Delegate call to pushAcordStatus to ASAPUtility

      29-Oct-2015   kg  Ticket # 63984
          Add ilsprod@crlcorp.com so we production support gets a copy of the email.
      17-Dec-2019   leec  PRJTASK0030391
          Send "Cases Released but Moved to Pending" report to the respective examiner
          based on the source code of the pending case.

      27-Sep-2019   jbn  SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""
from __future__ import division, absolute_import, with_statement, print_function
from .Utility import ASAP_UTILITY
devState = ASAP_UTILITY.devState
import CRLUtility
import glob
import os
import re
import time
from .ImageFactory import ASAPImageFactory
from .IndexHandler import ASAPIndexHandler
from .TransmitHandler import ASAPTransmitHandler
from .Case import ASAPCase
from .Contact import ASAPContact
from .Document import ASAPDocument
from .CaseQCFactory import CaseQCFactory, CaseQCHistoryItem

# @formatter:off

if devState.isDevInstance():
    APPS_NEW_DOCS_EMAIL_TO    = 'leec@crlcorp.com'
    APPS_NEW_DOCS_EMAIL_CC    = 'leec@crlcorp.com'
    EXAMONE_NEW_DOCS_EMAIL_TO = 'leec@crlcorp.com'
    EXAMONE_NEW_DOCS_EMAIL_CC = 'leec@crlcorp.com'
    EMAIL_BCC = 'leec@crlcorp.com'
else:
    APPS_NEW_DOCS_EMAIL_TO    = 'lcerrone@appshq.com,dperricone@appshq.com'
    APPS_NEW_DOCS_EMAIL_CC    = 'sschubel@appshq.com,ilsprod@crlcorp.com,esubadmin@crlcorp.com'
    EXAMONE_NEW_DOCS_EMAIL_TO = 'examoneselectquote@examone.com,Courtney.M.Thurman@ExamOne.com,Kelli.L.McKernan@ExamOne.com'
    EXAMONE_NEW_DOCS_EMAIL_CC = 'ilsprod@crlcorp.com,esubadmin@crlcorp.com'
    EMAIL_BCC = 'ilsprod@crlcorp.com'

# @formatter:on

examiners = {
    'APPS': {
        'SOURCE_CODE': 'ESubmissions-APPS',
        'EMAIL_TO': APPS_NEW_DOCS_EMAIL_TO,
        'EMAIL_CC': APPS_NEW_DOCS_EMAIL_CC,
        'EMAIL_TITLE': 'ASAP Cases Released but Moved to Pending',
        'EMAIL_BODY': 'The following released cases have been moved to Pending status '
                      'due to new documents that arrived at CRL and have been imaged. '
                      'Please review:\r\n'
    },
    'EXAMONE': {
        'SOURCE_CODE': 'ESubmissions-EXAMONE',
        'EMAIL_TO': EXAMONE_NEW_DOCS_EMAIL_TO,
        'EMAIL_CC': EXAMONE_NEW_DOCS_EMAIL_CC,
        'EMAIL_TITLE': 'Examone - Cases Released but Moved to Pending',
        'EMAIL_BODY': 'The following released cases have been moved to Pending status '
                      'due to new documents that arrived at CRL and have been imaged. '
                      'Please review:\r\n'
    }
}

XMIT_CONFIG = ASAP_UTILITY.getConfig()
CASE_FACTORY = ASAP_UTILITY.getCaseFactory()
DOCUMENT_FACTORY = ASAP_UTILITY.getDocumentFactory()
DOCUMENT_HISTORY = ASAP_UTILITY.getDocumentHistory()
ACORD_103_STORE = ASAP_UTILITY.getAcord103Store()


class ASAPMainHandler(object):
    """
    This serves as the high-level interface for ASAP indexing and
    transmission.
    """
    # export flag values for tblfolders in Delta
    DOC_EXPORTED_YES = 'Y'
    DOC_EXPORTED_NO = 'N'
    DOC_EXPORTED_IGNORE = 'I'
    # message IDs for case status in LIMS
    MSG_IMAGES_RELEASED = '377'
    MSG_IMAGES_AVAILABLE = '477'
    # base class names for those with configurable custom descendants
    BASE_INDEXHANDLER = 'ASAPIndexHandler'
    BASE_TRANSMITHANDLER = 'ASAPTransmitHandler'
    # statuses for ACORD orders
    STATUS_SENT_TO_CLIENT = ASAP_UTILITY.STATUS_SENT_TO_CLIENT

    def __init__(self, logger=None):
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger
        self.__fError = False
        self.__preExportedCasesList = []
        self.__loadedModules = {}

    def getErrorState(self):
        return self.__fError

    @staticmethod
    def getLIMSCursorForCase(asapCase):
        """
        Figure out whether to use sip or snip for case's sample
        records and return the appropriate cursor.  If sid can't
        be found for case, then return None.

        :param ASAPCase asapCase:
        """
        return ASAP_UTILITY.getLIMSCursorForSid(asapCase.sid)

    @staticmethod
    def is_on_K_hold(sid):
        """

        :param str sid:
        """
        return ASAP_UTILITY.getLIMSSampleFieldsForSid(sid, ('hold_flag_id', ))[1] == 'K'

    @staticmethod
    def set_K_hold(sid):
        """

        :param sid:
        :return: bool indicating success
        :rtype: bool
        """
        success = False
        cursor = ASAP_UTILITY.getLIMSCursorForSid(sid, restrictToDbs=(XMIT_CONFIG.DB_NAME_SIP, ))
        if cursor:
            update = "update sample set hold_flag_id = 'K' where sid = '{sid!s:s}'".format(sid=sid)
            cursor.execute(update)
            cursor.commit()
            success = True
        return success

    def addLIMSMessage(self, asapCase, msgId):
        """
        Add message to the sample in LIMS for the related case object.

        :param ASAPCase asapCase:
        :param str msgId:
        :returns: True if successful, False otherwise.
        """
        self.__fError = False
        fSuccess = False
        sid = asapCase.sid
        sInsert = '''
            insert into sample_messages
            (message_id, message_source, sid, display_flag, message_date, message_creator)
            values ('{msgId!s:s}', 'V', '{sid!s:s}', 'N', current_timestamp, 'ASAP')
            '''.format(msgId=msgId, sid=sid)
        sConfirmQuery = '''
            select sid
            from sample_messages
            where sid = '{sid!s:s}'
            and message_id = '{msgId!s:s}'
            '''.format(sid=sid, msgId=msgId)
        limsCursor = self.getLIMSCursorForCase(asapCase)
        if limsCursor:
            # check for record, insert if not present, then confirm inserted record is present
            limsCursor.execute(sConfirmQuery)
            rec = limsCursor.fetch(True)
            limsCursor.rollback()
            if not rec:
                limsCursor.execute(sInsert)
                limsCursor.commit()
                limsCursor.execute(sConfirmQuery)
                rec = limsCursor.fetch(True)
                limsCursor.rollback()
                if rec:
                    fSuccess = True
                else:
                    self.__logger.warn('Failed to add message {msgId!s:s} to sid {sid!s:s} in LIMS.'.format(msgId=msgId, sid=sid))
            else:
                # record already exists, so return success
                fSuccess = True
        else:
            self.__logger.warn('Unable to find case in LIMS (sid {sid!s:s}).'.format(sid=sid))
        self.__fError = not fSuccess
        return fSuccess

    def setExportFlag(self, asapDocument, flagValue):
        """
        Set export flag for image related to ASAPDocument object to either
        'ASAPMainHandler.DOC_EXPORTED_YES' or 'ASAPMainHandler.DOC_EXPORTED_NO'.
        Returns True if successful, False otherwise.

        :param ASAPDocument asapDocument:
        :param str flagValue:
        """
        self.__fError = False
        fSuccess = False
        docId = asapDocument.getDocumentId()

        cursor = XMIT_CONFIG.getCursor(XMIT_CONFIG.DB_NAME_DELTA_QC)
        if cursor:
            exportField = XMIT_CONFIG.getSetting(XMIT_CONFIG.SETTING_DELTA_EXPORT_FIELD)
            iRet = cursor.execute('''
                update tblfolders set {exportField!s:s} = '{flagValue!s:s}'
                where folderid = (select folderid
                from tbldocuments with (nolock)
                where documentid = {docId:d})
                '''.format(exportField=exportField, flagValue=flagValue, docId=docId))
            cursor.commit()
            if iRet == 1:
                fSuccess = True
            else:
                self.__logger.warn('Failed to set export flag to {flagValue!s:s} for docid {docId:d}.'
                                   .format(flagValue=flagValue, docId=docId))
        else:
            self.__logger.warn('Delta connection unavailable.')
        self.__fError = not fSuccess
        return fSuccess

    def getReleasedCases(self):
        """
        Look for images not exported, keep only the ones for which
        the related cases are released, then build document and
        case objects.  Return a list of case objects.
        """
        self.__fError = False
        releasedCases = []
        asapMismatchList = []
        docBillingIssuesList = []
        deltaCursor = XMIT_CONFIG.getCursor(XMIT_CONFIG.DB_NAME_DELTA_QC)
        caseCursor = XMIT_CONFIG.getCursor(XMIT_CONFIG.DB_NAME_CASE_QC)
        acordCursor = XMIT_CONFIG.getCursor(XMIT_CONFIG.DB_NAME_ACORD)
        if deltaCursor and caseCursor and acordCursor:
            # get images not exported from Delta
            exportField = XMIT_CONFIG.getSetting(XMIT_CONFIG.SETTING_DELTA_EXPORT_FIELD)
            sidField = XMIT_CONFIG.getSetting(XMIT_CONFIG.SETTING_DELTA_SID_FIELD)
            deltaCursor.execute('''
                select f.{sidField!s:s}, d.documentid from tbldocuments d with (nolock)
                inner join tblfolders f with (nolock)
                on d.folderid = f.folderid
                where f.{exportField!s:s} = '{flag!s:s}'
                and exists (select p.pageid
                            from tblpages p with (nolock)
                            where p.documentid = d.documentid)
                '''.format(sidField=sidField, exportField=exportField, flag=self.DOC_EXPORTED_NO))
            unexportedDocs = deltaCursor.fetch()
            deltaCursor.rollback()
            if unexportedDocs:
                releasedDocDict = {}
                sidDocDict = {}
                for sid, docid in unexportedDocs:
                    docids = sidDocDict.get(sid)
                    if docids:
                        docids.append(int(docid))
                    else:
                        sidDocDict[sid] = [int(docid), ]
                # check if related cases are released and keep only those images
                for sid in sidDocDict:
                    caseCursor.execute('''
                        select trackingid from casemaster with (nolock)
                        where sampleid = '{sid!s:s}'
                        and source_code like 'ESubmissions-%'
                        and upper(state) = 'RELEASED'
                        '''.format(sid=sid))
                    rec = caseCursor.fetch(True)
                    caseCursor.rollback()
                    if rec:
                        trackingid = rec[0]
                        # check sid's client, region, examiner and see if
                        # an ASAP contact exists...if not, then use j-hold
                        # logic from old process (email at the end of this method)
                        lims = ASAP_UTILITY.getLIMSCursorForSid(sid)
                        clientId, regionId, examiner = ('', '', '')
                        if lims:
                            lims.execute('''
                                select client_id, region_id, examiner
                                from sample where sid = '{sid!s:s}'
                                '''.format(sid=sid))
                            rec = lims.fetch(True)
                            lims.rollback()
                            if rec:
                                clientId, regionId, examiner = rec
                        contact = None
                        if clientId:
                            contact = XMIT_CONFIG.getContact(clientId, regionId, examiner)
                        if contact:
                            caseCursor.execute('''
                                select count(*) from casemaster with (nolock)
                                where trackingid = '{trackingid!s:s}'
                                '''.format(trackingid=trackingid))
                            rec = caseCursor.fetch(True)
                            caseCursor.rollback()
                            if rec[0] == 1:
                                acordCursor.execute('''
                                    select count(trackingid) from acord_order
                                    where sampleid = '{sid!s:s}'
                                    and source_code like 'ESubmissions-%'
                                    '''.format(sid=sid))
                                rec = acordCursor.fetch(True)
                                acordCursor.rollback()
                                if rec[0] == 1:
                                    releasedDocDict[sid] = sidDocDict[sid]
                                else:
                                    self.__logger.error(
                                        'There are multiple matched ACORD orders for ' +
                                        'released case for sid {sid!s:s}. Please review and/or correct.'
                                        .format(sid=sid))
                            else:
                                self.__logger.error(
                                    'There are multiple case QC records for ' +
                                    'tracking ID {trackingid!s:s}. Please correct.'
                                    .format(trackingid=trackingid))
                        elif (clientId and
                              not self.is_on_K_hold(sid) and
                              not ASAP_UTILITY.getASAPRelatedClients(clientId)):
                            fHold = self.set_K_hold(sid)
                            asapMismatchList.append((sid, clientId, regionId, fHold))
                if releasedDocDict:
                    # build ASAPDocument objects from docids, then build ASAPCase objects
                    for sid in releasedDocDict.keys():
                        case = CASE_FACTORY.fromSid(sid)
                        if case:
                            docids = releasedDocDict[sid]
                            fAddCase = True
                            fDocBillable = True
                            for docid in docids:
                                doc = DOCUMENT_FACTORY.fromDocumentId(docid)
                                if doc:
                                    fDocAdded = case.addDocument(doc)
                                    if (not fDocAdded and doc.fBill):
                                        msg = ('Document not set up for billing, please review (docid {docId:d}, doctype {docType!s:s}, contact {contact!s:s}).'
                                               .format(docId=doc.getDocumentId(), docType=doc.getDocTypeName(), contact=case.contact.contact_id))
                                        docBillingIssuesList.append(msg)
                                        self.__logger.warn(msg)
                                        fAddCase = False
                                        fDocBillable = False
                                    elif not fDocAdded:
                                        # mark the doc as exported since it's being bypassed
                                        # and we don't want to review it over and over
                                        self.setExportFlag(doc, self.DOC_EXPORTED_IGNORE)
                                else:
                                    # we failed to build a document object
                                    fAddCase = False
                            if fAddCase:
                                releasedCases.append(case)
                            elif not fDocBillable:
                                pass
                            else:
                                self.__fError = True
                        else:
                            self.__fError = True
                    if self.__fError:
                        self.__logger.warn('Failed to create complete case objects for ' +
                                           'all released cases in Delta.')
                else:
                    self.__logger.info('No released cases for unexported documents in Delta.')
            else:
                self.__logger.info('No unexported documents found in Delta.')
        else:
            self.__fError = True
            self.__logger.warn('Delta, ACORD, and/or case QC connection unavailable.')
        if asapMismatchList:
            sMessage = "For each of the following samples, there is no related "
            sMessage += "E-Submissions contact in LIMS. For samples not on 'K' "
            sMessage += "hold, the sample is either in SNIP or already on hold.\n\n\n"
            for sid, client, region, fOnHold in asapMismatchList:
                sOnHold = 'No'
                if fOnHold:
                    sOnHold = 'Yes'
                sMessage += "{sid!s:s}\tClient/Region: {client!s:s}/{region!s:s}\tOn 'K' hold: {sOnHold!s:s}\n".format(sid=sid,
                                                                                                                       client=client,
                                                                                                                       region=region,
                                                                                                                       sOnHold=sOnHold)
            sTitle = 'ILS E-Submissions Error: No Contact for Sample'
            if devState.isDevInstance():
                sAddress = 'gandhik@crlcorp.com'
                ccAddress = 'nelsonj@crlcorp.com'
            else:
                sAddress = 'esubadmin@crlcorp.com'
                ccAddress = 'ilsprod@crlcorp.com'
            CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle,
                                    'noreply@crlcorp.com', '', ccAddress)
        if docBillingIssuesList:
            sMessage = '\n'.join(docBillingIssuesList)
            sTitle = 'ILS E-Submissions Error: Documents not set up for billing'
            if devState.isDevInstance():
                sAddress = 'nelsonj@crlcorp.com'
                ccAddress = 'nelsonj@crlcorp.com'
            else:
                sAddress = 'esubadmin@crlcorp.com'
                ccAddress = 'ilsprod@crlcorp.com'
            CRLUtility.CRLSendEMail(sAddress, sMessage, sTitle,
                                    'noreply@crlcorp.com', '', ccAddress)
        return releasedCases

    def exportCase(self, asapCase):
        """
        Build and export the images for the case's documents, and export
        the ACORD 103 XML file (if necessary).  If successful, mark the
        images as exported in Delta.

        :param ASAPCase asapCase:
        """
        begintime = time.time()
        self.__fError = False
        fSuccess = False
        # if there's a 103 path for the case's contact, try to get the 103 file
        acord103FileName = asapCase.trackingId + '.XML'
        acordRec = None
        if asapCase.contact.acord103_dir:
            acordRecs = ACORD_103_STORE.getByTrackingId(asapCase.trackingId)
            if acordRecs:
                acordRec = acordRecs[0]
                fWriteFile = True
                if not acordRec.retrieve:
                    docids = [str(docid) for docid in asapCase.getDocuments().keys()]
                    modCount = 0
                    if docids:
                        qc = XMIT_CONFIG.getCursor(XMIT_CONFIG.DB_NAME_DELTA_QC)
                        qc.execute('''
                            select count(folderid) from tblfolders with (nolock)
                            where value_3 = 'Y' and folderid in (
                            select folderid from tbldocuments with (nolock)
                            where documentid in ({docIds!s:s}))
                            '''.format(docIds=','.join(docids)))
                        rec = qc.fetch(True)
                        qc.rollback()
                        modCount, = rec
                    else:
                        self.__logger.info(
                            "For case {sid!s:s}/{trackingid!s:s} (contact {contact!s:s}), there are no documents to transmit."
                            .format(sid=asapCase.sid, trackingid=asapCase.trackingId, contact=asapCase.contact.contact_id))
                    if modCount != len(docids):
                        fWriteFile = False
                if fWriteFile:
                    ACORD_103_STORE.writeToFile(acordRec, asapCase.contact.acord103_dir, True)
                else:
                    caseCursor = XMIT_CONFIG.getCursor(XMIT_CONFIG.DB_NAME_CASE_QC)
                    caseCursor.execute('''
                        update casemaster set state = 'Pending'
                        where sampleid = '{sid!s:s}'
                        '''.format(sid=asapCase.sid))
                    caseCursor.commit()
                    caseQc = CaseQCFactory().fromSid(asapCase.sid)
                    histItem = CaseQCHistoryItem()
                    histItem.action = histItem.ACTION_PEND
                    histItem.createdBy = 'CRL_ASAP'
                    histItem.comment = 'pended by user {user!s:s}'.format(user=histItem.createdBy)
                    CaseQCFactory().addHistoryItem(caseQc, histItem)
                    ASAP_UTILITY.reReleaseCase(asapCase)
                    self.__logger.info(
                        ('For case {sid!s:s}/{trackingId!s:s} (contact {contact!s:s}), there are documents to ' +
                         'transmit, but the ACORD 103 record has been previously retrieved. ' +
                         'Please review.').format(sid=asapCase.sid, trackingId=asapCase.trackingId, contact=asapCase.contact.contact_id))
                    self.__preExportedCasesList.append(asapCase)
                    return fSuccess
            else:
                # make an info message and return from here, simply because
                # a separate script will collect this info and report in an email
                # each day
                self.__logger.info(
                    'ACORD 103 record is missing and is required for this case ({sid!s:s}/{trackingId!s:s}).'
                    .format(sid=asapCase.sid, trackingId=asapCase.trackingId))
                # self.__fError = True
                return fSuccess

        # if we either don't need the 103 or need it and found it, continue processing
        if not self.__fError:
            # use ASAPImageFactory to build images for each document object in case
            imageFactory = ASAPImageFactory()
            for asapDocument in asapCase.getDocuments().values():
                if not imageFactory.fromDocument(asapDocument):
                    self.__fError = True
                    break
        # if successful, mark the images as exported and note "released" in history
        if not self.__fError:
            for asapDocument in asapCase.getDocuments().values():
                self.setExportFlag(asapDocument, self.DOC_EXPORTED_YES)
                DOCUMENT_HISTORY.trackDocument(asapDocument, DOCUMENT_HISTORY.ACTION_RELEASE)
            fSuccess = True
        else:
            # if anything fails, back out any work (remove built images, remove the 103)
            if asapCase.contact.acord103_dir and acordRec:
                ACORD_103_STORE.setToRetrieve(acordRec, True)
                local103File = os.path.join(asapCase.contact.acord103_dir,
                                            acord103FileName)
                if os.path.isfile(local103File):
                    CRLUtility.CRLDeleteFile(local103File)
            for asapDocument in asapCase.getDocuments().values():
                localDocFile = os.path.join(asapCase.contact.document_dir,
                                            asapDocument.fileName)
                if os.path.isfile(localDocFile):
                    CRLUtility.CRLDeleteFile(localDocFile)
        self.__logger.info('Time for case {sid!s:s}/{trackingId!s:s}/{client_id!s:s}/{region_id!s:s}/{examiner!s:s} took {elapsed:5.3f} seconds.'
                           .format(sid=asapCase.sid,
                                   trackingId=asapCase.trackingId,
                                   client_id=asapCase.contact.client_id,
                                   region_id=asapCase.contact.region_id,
                                   examiner=asapCase.contact.examiner,
                                   elapsed=(time.time() - begintime)))
        return fSuccess

    def reportPreExportedCases(self):
        if self.__preExportedCasesList:
            casesByExaminer = {}
            for examiner, examinerConfig in examiners.items():
                sourceCodeRegex = re.compile(r'{source_code!s:s}\w+'.format(source_code=examinerConfig['SOURCE_CODE']))
                casesByExaminer[examiner] = []
                for case in self.__preExportedCasesList:
                    sourceCode = case.contact.source_code
                    if sourceCodeRegex.match(sourceCode.strip()):
                        casesByExaminer[examiner].append(case)

            for examiner, cases in casesByExaminer.items():
                if cases:
                    examinerConfig = examiners[examiner]
                    sMsg = examinerConfig['EMAIL_BODY']
                    for case in cases:
                        carrier = ASAP_UTILITY.getCarrierCodeForCase(case)
                        sMsg += ('\r\n\r\nNew documents for case #{trackingId!s:s} (Carrier Code {carrier!s:s}, CRL ID {sid!s:s}):'
                                 .format(trackingId=case.trackingId, carrier=carrier, sid=case.sid))
                        docids = case.getDocuments().keys()
                        docids.sort()
                        for docid in docids:
                            sMsg += '\r\n%s' % (case.getDocuments()[docid].getDocTypeName())

                    CRLUtility.CRLSendEMail(examinerConfig['EMAIL_TO'], sMsg, examinerConfig['EMAIL_TITLE'],
                                            'ilsprod@crlcorp.com', examinerConfig['EMAIL_CC'], EMAIL_BCC)
            self.__preExportedCasesList = []

    def getExportedCasesForContact(self, asapContact):
        """ Get cases whose images have been exported for a contact.

        :param ASAPContact asapContact:
        """
        self.__fError = False
        exportedCases = []
        # if contact supports document processing, proceed
        if asapContact.document_dir:
            exportedDocs = []
            imageFiles = glob.glob(os.path.join(asapContact.document_dir, '*.TIF'))
            for imageFile in imageFiles:
                fileName = os.path.basename(imageFile)
                asapDocument = DOCUMENT_FACTORY.fromFileName(fileName)
                if asapDocument:
                    exportedDocs.append(asapDocument)
            if exportedDocs:
                exportedCases = CASE_FACTORY.casesForDocuments(exportedDocs)
                if exportedCases:
                    caseDocCount = 0
                    for case in exportedCases:  # type: ASAPCase
                        caseDocCount += len(case.getDocuments())
                    if len(exportedDocs) != caseDocCount:
                        self.__logger.warn('Failed to create complete case objects for ' +
                                           'all exported cases for contact {contact_id!s:s}.'
                                           .format(contact_id=asapContact.contact_id))
                        self.__fError = True
                    # get released docids for each case and see if any are in the processed
                    # subfolder, and if so, add to case and move to parent images folder
                    # --this is done to make sure all untransmitted case documents are grouped
                    #   together (may lead to some redundant indexing, but should be rare)
                    processedSubdir = XMIT_CONFIG.getSetting(XMIT_CONFIG.SETTING_PROCESSED_SUBDIR)
                    for case in exportedCases:  # type: ASAPCase
                        docIds = DOCUMENT_HISTORY.getTrackedDocidsForCase(case, DOCUMENT_HISTORY.ACTION_RELEASE)
                        docDict = case.getDocuments()
                        for docId, auditstamp in docIds:
                            if docId not in docDict:
                                doc = DOCUMENT_FACTORY.fromDocumentId(docId)
                                if doc:
                                    docPath = os.path.join(asapContact.document_dir,
                                                           processedSubdir,
                                                           doc.fileName)
                                    if os.path.isfile(docPath) and case.addDocument(doc):
                                        CRLUtility.CRLCopyFile(
                                            docPath,
                                            os.path.join(asapContact.document_dir, doc.fileName),
                                            True, 5)
        return exportedCases

    def getIndexedCasesForContact(self, asapContact):
        """
        Get cases with index files.  If indexes are document-level, then
        document objects are created first (then cases from those).  If
        case-level, case objects are created first, then document objects
        are built from them based upon what images are tracked as released
        and have not been tracked as transmitted since.

        :param ASAPContact asapContact:
        """
        self.__fError = False
        indexedCases = []
        indexedDocs = []
        # if contact supports indexing, proceed
        if asapContact.index_dir:
            idxFiles = glob.glob(os.path.join(asapContact.index_dir, '*.IDX'))
            fNotSupported = False
            for idxFile in idxFiles:
                fileName = os.path.basename(idxFile)
                if asapContact.index.type == asapContact.index.IDX_TYPE_DOCUMENT:
                    docFileName = '{fileName!s:s}.TIF'.format(fileName=os.path.splitext(fileName)[0])
                    asapDocument = DOCUMENT_FACTORY.fromFileName(docFileName)
                    if asapDocument:
                        indexedDocs.append(asapDocument)
                elif asapContact.index.type == asapContact.index.IDX_TYPE_CASE:
                    trackingId = os.path.splitext(fileName)[0]
                    asapCase = CASE_FACTORY.fromTrackingId(trackingId)
                    if asapCase:
                        # --get docs for case by checking doc history for docs
                        #   released but not transmitted (don't check invoice dates)
                        docid_dates = DOCUMENT_HISTORY.getTrackedDocidsForCase(
                            asapCase, DOCUMENT_HISTORY.ACTION_RELEASE)
                        if docid_dates:
                            for docid, releaseDate in docid_dates:
                                asapDocument = DOCUMENT_FACTORY.fromDocumentId(docid)
                                if asapDocument:
                                    asapDocument.case = asapCase
                                    transmitDate = DOCUMENT_HISTORY.getDateTracked(
                                        asapDocument, DOCUMENT_HISTORY.ACTION_TRANSMIT)
                                    if not transmitDate or transmitDate < releaseDate:
                                        asapCase.addDocument(asapDocument)
                        if asapCase.getDocuments():
                            indexedCases.append(asapCase)
                else:
                    fNotSupported = True
                    break
            if fNotSupported:
                self.__logger.warn('Contact {contact_id!s:s} index type {type!s:s} not supported.'
                                   .format(contact_id=asapContact.contact_id, type=asapContact.index.type))
                self.__fError = True
            elif indexedDocs:
                indexedCases = CASE_FACTORY.casesForDocuments(indexedDocs)
                caseDocCount = 0
                for case in indexedCases:
                    caseDocCount += len(case.getDocuments())
                if len(indexedDocs) != caseDocCount:
                    self.__logger.warn('Failed to create complete case objects for ' +
                                       'all indexed cases in Delta.')
                    self.__fError = True
        return indexedCases

    def buildIndexesForCase(self, asapCase):
        """ Given a fully qualified ASAPCase object, build index(es) for case
        and write file(s) to contact-specified staging location.

        :param ASAPCase asapCase:
        """
        self.__fError = False
        fSuccess = False
        # need to get custom class from custom module (table-based data)
        # and check that it's derived from ASAPIndexHandler, then call
        # buildIndexesForCase if it is
        customRec = asapCase.contact.customClasses.get(self.BASE_INDEXHANDLER)
        handler = None
        if customRec:
            customModule, customClass = customRec
            if customModule in self.__loadedModules:
                modObj = self.__loadedModules[customModule]
            else:
                modObj = CRLUtility.CRLLoadModule(customModule)
                self.__loadedModules[customModule] = modObj
            if modObj:
                classObj = None
                try:
                    classObj = getattr(modObj, customClass)
                except Exception:
                    self.__logger.warn('Module {customModule!s:s} does not contain class {customClass!s:s}.'
                                       .format(customModule=customModule, customClass=customClass))

                if (isinstance(classObj, type) and issubclass(classObj, ASAPIndexHandler)):
                    handler = classObj()
                else:
                    self.__logger.warn(
                        'Class {customClass!s:s} (module {customModule!s:s}) is not a subclass of {BASE_INDEXHANDLER!s:s}.'
                        .format(customClass=customClass, customModule=customModule, BASE_INDEXHANDLER=self.BASE_INDEXHANDLER))
            else:
                self.__logger.warn('Module {customModule!s:s} does not exist.'.format(customModule=customModule))
        else:
            # if no custom class is configured, use the base class by default
            handler = ASAPIndexHandler()
        if handler:
            fSuccess = handler.buildIndexesForCase(asapCase)
            # noinspection PyProtectedMember
            if handler._isReadyToIndex():
                self.__fError = not fSuccess
        else:
            self.__fError = not fSuccess
        return fSuccess

    def stageAndTransmitCases(self, asapCases, asapContact, stagedCases):
        """ Given a list of ASAPCases (all presumed to have been indexed or appropriately
        prepared to be staged for transmission by their related contact), prepare for
        and perform transmission based upon contact-specific custom class.
        NOTE: The parameter stagedCases *must* be a list (preferably an empty list)
        that will be appended to with the cases that were successfully staged.

        :param list[ASAPCase] asapCases:
        :param ASAPContact asapContact:
        :param list[ASAPCase] stagedCases:
        """
        self.__fError = False
        fSuccess = False
        # need to get custom class from custom module (table-based data)
        # and check that it's derived from ASAPTransmitHandler, then call
        # stageAndTransmitCases if it is
        customRec = asapContact.customClasses.get(self.BASE_TRANSMITHANDLER)
        handler = None
        if customRec:
            customModule, customClass = customRec
            if customModule in self.__loadedModules:
                modObj = self.__loadedModules[customModule]
            else:
                modObj = CRLUtility.CRLLoadModule(customModule)
                self.__loadedModules[customModule] = modObj
            if modObj:
                classObj = None
                try:
                    classObj = getattr(modObj, customClass)
                except Exception:
                    self.__logger.warn('Module {customModule!s:s} does not contain class {customClass!s:s}.'
                                       .format(customModule=customModule, customClass=customClass))
                if (isinstance(classObj, type) and issubclass(classObj, ASAPTransmitHandler)):
                    handler = classObj()
                else:
                    self.__logger.warn(
                        'Class {customClass!s:s} (module {customModule!s:s}) is not a subclass of {BASE_TRANSMITHANDLER!s:s}.'
                        .format(customClass=customClass, customModule=customModule, BASE_TRANSMITHANDLER=self.BASE_TRANSMITHANDLER))
            else:
                self.__logger.warn('Module {customModule!s:s} does not exist.'.format(customModule=customModule))
        else:
            # if no custom class is configured, log and ignore
            self.__logger.info('Contact {contact_id!s:s} not configured for transmitting cases.'
                               .format(contact_id=asapContact.contact_id))
            fSuccess = True
        if handler:
            fSuccess = handler.stageAndTransmitCases(asapCases, asapContact, stagedCases)
            # push Acord status in the MainThread instead
        #            for asapCase in stagedCases:
        #                self.pushAcordStatus(asapCase, self.STATUS_SENT_TO_CLIENT)
        self.__fError = not fSuccess
        return fSuccess

    def billCase(self, asapCase):
        """ Given a fully qualified ASAPCase object, bill for document services in
        LIMS using the document-type billing code mapping for the contact.

        :param ASAPCase asapCase:
        """
        self.__fError = False
        fSuccess = False
        limsCursor = self.getLIMSCursorForCase(asapCase)
        if limsCursor:
            # map billing code to service count
            billingInfo = {}
            docsNotBilled = []
            for asapDocument in asapCase.getDocuments().values():
                invoiceDate = DOCUMENT_HISTORY.getDateTracked(asapDocument, DOCUMENT_HISTORY.ACTION_INVOICE)
                if not invoiceDate:
                    billingCode = asapCase.contact.docTypeBillingMap.get(asapDocument.getDocTypeName())
                    if billingCode:
                        if asapDocument.fBill:
                            serviceCount = billingInfo.get(billingCode)
                            if not serviceCount:
                                serviceCount = 0
                            billingInfo[billingCode] = serviceCount + asapDocument.pageCount
                        else:
                            self.__logger.info(
                                'Document {docId:d} for case {sid!s:s}/{trackingId!s:s} with document type of {docTypeName!s:s} will not be billed.'
                                .format(docId=asapDocument.getDocumentId(), sid=asapCase.sid,
                                        trackingId=asapCase.trackingId, docTypeName=asapDocument.getDocTypeName()))
                            docsNotBilled.append(asapDocument.getDocumentId())
                    else:
                        self.__logger.info(
                            'No billing code found for document ' +
                            '{docId:d} for case {sid!s:s}/{trackingId!s:s} with document type of {docTypeName!s:s}.'
                            .format(docId=asapDocument.getDocumentId(), sid=asapCase.sid,
                                    trackingId=asapCase.trackingId, docTypeName=asapDocument.getDocTypeName()))
                        docsNotBilled.append(asapDocument.getDocumentId())
                else:
                    self.__logger.info(
                        'Docid {docId:d} for case {sid!s:s}/{trackingId!s:s} has already been billed.'
                        .format(docId=asapDocument.getDocumentId(), sid=asapCase.sid,
                                trackingId=asapCase.trackingId))
                    docsNotBilled.append(asapDocument.getDocumentId())
            for billingCode in billingInfo.keys():
                sConfirmQuery = '''
                    select service_count
                    from service_performed
                    where sid = '{sid!s:s}'
                    and test_id = '{billingCode!s:s}'
                    '''.format(sid=asapCase.sid, billingCode=billingCode)
                sDelete = '''
                    delete from service_performed
                    where sid = '{sid!s:s}' and test_id = '{billingCode!s:s}'
                    '''.format(sid=asapCase.sid, billingCode=billingCode)
                limsServiceCount = 0
                # get existing count to increment from, if exists
                limsCursor.execute(sConfirmQuery)
                rec = limsCursor.fetch(True)
                limsCursor.rollback()
                if rec:
                    # must delete old record since table will not permit updates
                    limsServiceCount, = rec
                    limsCursor.execute(sDelete)
                    limsCursor.commit()
                # now perform insert of new record
                sInsert = '''
                    insert into service_performed (sid, test_id, service_count)
                    values ('{sid!s:s}', '{billingCode!s:s}', {count:d})
                    '''.format(sid=asapCase.sid, billingCode=billingCode,
                               count=billingInfo[billingCode] + limsServiceCount)
                limsCursor.execute(sInsert)
                limsCursor.commit()
            # write to doc history that each document was invoiced
            for asapDocument in asapCase.getDocuments().values():
                if asapDocument.getDocumentId() not in docsNotBilled:
                    DOCUMENT_HISTORY.trackDocument(asapDocument, DOCUMENT_HISTORY.ACTION_INVOICE)
            fSuccess = True
        else:
            self.__logger.warn('Unable to find case in LIMS (sid {sid!s:s}).'
                               .format(sid=asapCase.sid))
        self.__fError = not fSuccess
        return fSuccess

    def pushAcordStatus(self, asapCase, statusValue):
        """
        ContactThread delegates to MainHandler to push ACORD status, and this method was
        updated to delegate to ASAPUtility.
        However, in the future, a better and more accurate method would modify each custom.py class
        to push ACORD sent_to_client status at the time the case is successfully transmitted.

        :param ASAPCase asapCase:
        :param statusValue:
        """
        fSuccess = ASAP_UTILITY.pushAcordStatus(asapCase.trackingId, asapCase.source_code, statusValue)
        self.__fError = not fSuccess
        return fSuccess
