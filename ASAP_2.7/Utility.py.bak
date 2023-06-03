"""

  Facility:         ILS

  Module Name:      Utility.py

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPUtility class.

  Author:
      Jarrod Wild

  Creation Date:
      08-Mar-2007

  Modification History:
      27-Oct-2008   jmw
         Moved a couple functions from ILSeSubReporting to ASAPUtility as methods.

      17-Feb-2014   rsu     Ticket # 46913
         Update updateClientRegionExaminer to pass back optional warning information in the method signature. In particular, ASAP_BuildCaseQCRecord
         needs to know whether the sample wasn't recoded due to a panel change.

      05-Nov-2014   rsu     Ticket # 53818
         Prevent exception in updateClientRegionExaminer when discarded samples have no specimen containers.

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
          Added lots of additional objects to the purview of the ASAPUtility class
          Made object creation as lazy as possible
"""
from __future__ import division, absolute_import, with_statement, print_function

import os
import datetime
import traceback
from ..ASAP import asapLogger, devState
from CRL.DBCursor import CRLDBCursor

from .TransmitConfig import ASAPTransmitConfig
from .CaseFactory import ASAPCaseFactory, ASAPCase
from .DocumentFactory import ASAPDocumentFactory, ASAPDocument
from .DocumentHistory import ASAPDocumentHistory
from .Acord103Store import ASAPAcord103Store, ASAPAcord103Record
from ..AcordOrderModel import AcordOrderFactory
from ..LimsModel import LimsSampleFactory
from .QCDocumentFactory import QCDocumentFactory
from .CaseQCFactory import CaseQCFactory
from .ViableCaseFactory import ViableCaseFactory
from .FileManager import ASAPFileManager
from .Contact import ASAPContact
from .AcordRequest import ASAPAcordRequest
from ILS.ilshelp import addComment
import DeltaUtility

try:
    # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
    ASAP_UTILITY
except NameError:

    class GetCaseException(Exception):
        def __init__(self, message):
            Exception.__init__(self, message)


    class ASAPUtility(object):
        """
        This is a warehouse class for miscellaneous utilities.
        """
        # messages
        WARNING_NEEDS_PANEL_CHANGE = "FLAG_NEEDS_PANEL_CHANGE"
        WARNING_NO_SPECIMEN_CONTAINERS = "FLAG_NO_SPECIMEN_CONTAINERS"
        ERROR_UNKNOWN = "FLAG_UNKNOWN_ERROR"

        # statuses for ACORD orders
        STATUS_SENT_TO_CLIENT = 9
        STATUS_APPROVED_BY_CLIENT = 5

        # Paperstore Export Flags
        DOC_EXPORTED_YES = 'Y'
        DOC_EXPORTED_NO = 'N'

        # noinspection PyTypeChecker
        def __init__(self):
            self._xmit_config = ASAPTransmitConfig()
            self._asap_case_factory = None         # type: ASAPCaseFactory
            self._asap_document_factory = None     # type: ASAPDocumentFactory
            self._asap_document_history = None     # type: ASAPDocumentHistory
            self._asap_acord_103_store = None      # type: ASAPAcord103Store
            self._asap_acord_order_factory = None  # type: AcordOrderFactory
            self._asap_qc_document_factory = None  # type: QCDocumentFactory
            self._asap_case_qc_factory = None      # type: CaseQCFactory
            self._asap_viable_case_factory = None  # type: ViableCaseFactory
            self._asap_file_manager = {}           # type: dict[str, ASAPFileManager]
            self._asap_acord_request = None        # type: ASAPAcordRequest
            self._lims_sample_factory = None       # type: LimsSampleFactory
            self.devState = devState
            self.asapLogger = asapLogger

        @property
        def xmitConfig(self):
            return self._xmit_config

        def getConfig(self):
            """
            Return the ASAPTransmitConfig object.

            :rtype: ASAPTransmitConfig
            """
            return self._xmit_config

        def getXmitConfig(self):
            """
            Return the ASAPTransmitConfig object.

            :rtype: ASAPTransmitConfig
            """
            return self._xmit_config

        def getCaseFactory(self):
            """
            Return case factory instance.

            :rtype: ASAPCaseFactory
            """
            if not self._asap_case_factory:
                self._asap_case_factory = ASAPCaseFactory(self.asapLogger)
            return self._asap_case_factory

        def getCase(self, valueType, listItem):
            caseFactory = self.getCaseFactory()
            orderFactory = self.getAcordOrderFactory()
            store = self.getAcord103Store()

            case = None
            if valueType == 'sid':
                case = caseFactory.fromSid(listItem)
            elif valueType == 'tracking ID':
                case = caseFactory.fromTrackingId(listItem)
            elif valueType == 'policy number':
                recs = store.getByPolicyNumber(listItem)
                if recs:
                    case = caseFactory.fromTrackingId(recs.pop().trackingId)
                else:
                    raise GetCaseException("Unable to locate %s %s" % (valueType, listItem))
            elif valueType == 'reference ID':
                orders = orderFactory.fromSelectQuoteRefId(listItem)
                if orders:
                    if len(orders) == 1:
                        order, = orders
                        case = caseFactory.fromSid(order.sid)
                    else:
                        raise GetCaseException("Multiple ACORD orders found for %s %s. " % (valueType, listItem))

                else:
                    raise GetCaseException("No ACORD orders found for %s %s" % (valueType, listItem))
            return case

        def getDocumentFactory(self):
            """
            Return document factory instance.

            :rtype: ASAPDocumentFactory
            """
            if not self._asap_document_factory:
                self._asap_document_factory = ASAPDocumentFactory(self.asapLogger)
            return self._asap_document_factory

        def getDocumentHistory(self):
            """
            Return document history instance.

            :rtype: ASAPDocumentHistory
            """
            if not self._asap_document_history:
                self._asap_document_history = ASAPDocumentHistory(self.asapLogger)
            return self._asap_document_history

        def getAcord103Store(self):
            """
            Return acord 103 store instance.

            :rtype: ASAPAcord103Store
            """
            if not self._asap_acord_103_store:
                self._asap_acord_103_store = ASAPAcord103Store(self.asapLogger)
            return self._asap_acord_103_store

        def getAcordOrderFactory(self):
            """
            Return the AcordOrderFactory object.

            :rtype: AcordOrderFactory
            """
            if not self._asap_acord_order_factory:
                self._asap_acord_order_factory = AcordOrderFactory(self.asapLogger)
            return self._asap_acord_order_factory

        def getQCDocumentFactory(self):
            """
            Return the QCDocumentFactory object.

            :rtype: QCDocumentFactory
            """
            if not self._asap_qc_document_factory:
                self._asap_qc_document_factory = QCDocumentFactory()
            return self._asap_qc_document_factory

        def getCaseQCFactory(self):
            """
            Return the CaseQCFactory object.

            :rtype: CaseQCFactory
            """
            if not self._asap_case_qc_factory:
                self._asap_case_qc_factory = CaseQCFactory()
            return self._asap_case_qc_factory

        def getViableCaseFactory(self):
            """
            Return the ViableCaseFactory object.

            :rtype: ViableCaseFactory
            """
            if not self._asap_viable_case_factory:
                self._asap_viable_case_factory = ViableCaseFactory(self.asapLogger)
            return self._asap_viable_case_factory

        def getASAPFileManager(self, contact=None):
            """
            Return the ASAPFileManager object.

            :param ASAPContact contact:
            :rtype: ASAPFileManager
            """
            if contact:
                key = contact.contact_id
            else:
                key = ''
            if key not in self._asap_file_manager:
                self._asap_file_manager[key] = ASAPFileManager(contact)
            return self._asap_file_manager[key]

        def getASAPAcordRequest(self):
            """ Return the ASAPAcordRequest object.

            :rtype: ASAPAcordRequest
            """
            if not self._asap_acord_request:
                self._asap_acord_request = ASAPAcordRequest(self.asapLogger)
            return self._asap_acord_request

        def getLimsSampleFactory(self):
            """ Return the LimsSampleFactory object.

            :rtype: LimsSampleFactory
            """
            if not self._lims_sample_factory:
                self._lims_sample_factory = LimsSampleFactory()
            return self._lims_sample_factory

        def getLIMSCursorForSid(self, sid, restrictToDbs=None):
            """
            Figure out whether to use sip or snip for sample
            records and return the appropriate cursor.

            :param str|unicode sid: 8-digit sample_id from LIMS
            :param tuple[str]|list[str] restrictToDbs: list of dbs to check (defaults to sip and snip
            :rtype: CRLDBCursor|None
            """
            limsCursor = None
            config = self._xmit_config
            sQuery = '''
                select sid
                from sample
                where sid = '{sid!s:s}'
                and hold_flag_id not in ('~','#')
                '''.format(sid=sid)

            # check snip first since it is likely the sid has already
            # transmitted before we are ready to check
            dbs_to_check = (config.DB_NAME_SNIP, config.DB_NAME_SIP)
            if restrictToDbs:
                dbs_to_check = [db for db in restrictToDbs if db in dbs_to_check]

            for db in dbs_to_check:
                cursor = config.getCursor(db)

                if cursor:
                    cursor.execute(sQuery)
                    rec = cursor.fetch(True)
                    cursor.rollback()
                    if rec:
                        limsCursor = cursor
                        break
                else:
                    self.asapLogger.warn('Unable to connect to LIMS.')
            return limsCursor

        def pushAcordStatus(self, trackingId, source_code, statusValue):
            """
            Given a asap case trackingid, set the ACORD Gateway status to statusValue.
            """
            fSuccess = False
            config = self.getXmitConfig()
            cursor = config.getCursor(config.DB_NAME_ACORD)
            if cursor:
                cursor.execute('''
                    update acord_order_requirement
                    set req_status = {statusValue:d}
                    where acord_order_id = (select acord_order_id
                                            from acord_order
                                            where trackingid = '{trackingId!s:s}'
                                            and source_code = '{source_code!s:s}')
                    '''.format(statusValue=statusValue, trackingId=trackingId, source_code=source_code))
                cursor.execute('''
                    update acord_order
                    set last_status_push = null, status = {statusValue:d}
                    where trackingid = '{trackingId!s:s}'
                    and source_code = '{source_code!s:s}'
                    '''.format(statusValue=statusValue, trackingId=trackingId, source_code=source_code))
                cursor.commit()
                self.asapLogger.info('Updated status to {statusValue:d} for case {trackingId!s:s}.'
                                     .format(statusValue=statusValue, trackingId=trackingId))
                fSuccess = True
            else:
                self.asapLogger.warn('Unable to connect to ACORD Gateway.')

            return fSuccess

        def reReleaseCase(self, asapCase, fRetransmitAll=False):
            """

            :param ASAPCase asapCase: the ASAPCase object to re-release
            :param bool fRetransmitAll: if set, set all documents for asapCase to re-export
            :return: a bool indicating if the re-release was successful
            :rtype: bool
            """
            if asapCase is None:
                return False
            fSuccess = True
            # noinspection PyTypeChecker
            acord103Rec = None  # type: ASAPAcord103Record
            if asapCase.contact.acord103_dir:
                acord103Recs = self.getAcord103Store().getByTrackingId(asapCase.trackingId)
                if acord103Recs:
                    acord103Rec = acord103Recs[0]
                else:
                    fSuccess = False
            if fSuccess and fRetransmitAll:
                docs = self.getDocumentFactory().documentsFromSid(asapCase.sid)
                if docs:
                    for doc in docs:
                        if not self.setExportFlag(doc, self.DOC_EXPORTED_NO):
                            fSuccess = False
                            break
                else:
                    fSuccess = False
            if fSuccess and acord103Rec:
                self.getAcord103Store().setToRetrieve(acord103Rec)
            return fSuccess

        def reStageToTransmit(self, asapCase, fOutput=True):
            """

            :param Optional[ASAPCase] asapCase: the ASAPCase object to re-stage
            :param bool fOutput: if True, prints an output statement
            :return:
            """
            fReset = False
            cursor = self._xmit_config.getCursor(self._xmit_config.DB_NAME_XMIT)
            cursor.execute('''
                select h1.contact_id, h1.documentid, min(h1.actiondate)
                from ils..asap_document_history h1 with (nolock)
                where h1.actionitem = 'release' and
                sid = '{sid!s:s}' and
                not exists (select historyid from ils..asap_document_history h2 with (nolock)
                            where h2.sid = h1.sid and h2.contact_id = h1.contact_id
                            and h2.documentid = h1.documentid
                            and h2.actiondate >= h1.actiondate
                            and h2.actionitem in ('transmit'))
                group by h1.contact_id, h1.documentid
                order by h1.contact_id, min(h1.actiondate)
                '''.format(sid=asapCase.sid))
            recs = cursor.fetch()
            cursor.rollback()
            if recs:
                fact = self.getDocumentFactory()
                for contactId, docId, actionDate in recs:
                    doc = fact.fromDocumentId(docId)
                    if doc:
                        self.setExportFlag(doc, self.DOC_EXPORTED_NO)
                        if fOutput:
                            print('Sid {sid!s:s}, docid {docid:d} restaged to transmit.'.format(sid=asapCase.sid, docid=doc.getDocumentId()))
                        fReset = True
                if fReset:
                    fReset = self.reReleaseCase(asapCase)
            return fReset

        def getLIMSSampleFieldsForSid(self, sid, fieldTuple, limsCursor=None):
            """

            :param str|unicode sid: 8-digit sid from lims
            :param tuple|list fieldTuple: tuple or list of columns to select from the sample table
            :param CRLDBCursor|None limsCursor: cursor for the database containing the active sample record for sid
            :return: sample data for specified sid and columns
            """
            returnRec = None
            if not limsCursor:
                limsCursor = self.getLIMSCursorForSid(sid)
            if limsCursor:
                sQuery = '''
                    select sid, {cols!s:s} from sample
                    where sid = '{sid!s:s}'
                    and hold_flag_id not in ('#','~')
                    '''.format(cols=','.join(fieldTuple), sid=sid)
                limsCursor.execute(sQuery)
                returnRec = limsCursor.fetch(True)
                limsCursor.rollback()
            return returnRec

        def getLIMSResultFieldsForSid(self, sid, testId, fieldTuple, limsCursor=None):
            """

            :param str sid: 8-digit sid from lims
            :param str testId: 4-character test_id for which to select result table data
            :param tuple fieldTuple: tuple or list of columns to select from the result table
            :param CRLDBCursor|None limsCursor: cursor for the database containing the active sample record for sid
            :return:
            """
            returnRec = None
            if not limsCursor:
                limsCursor = self.getLIMSCursorForSid(sid)
            if limsCursor:
                sQuery = '''
                    select test_id, {cols!s:s}
                    from results
                    where sid = '{sid!s:s}'
                    and test_id = '{testId!s:s}'
                    and not exists (select sid from sample where sid = results.sid
                                    and hold_flag_id in ('#','~'))
                    '''.format(cols=','.join(fieldTuple), sid=sid, testId=testId)
                limsCursor.execute(sQuery)
                returnRec = limsCursor.fetch(True)
                limsCursor.rollback()
            return returnRec

        def updateClientRegionExaminer(self, sid, limsCursor=None, fReportError=True, warnings=None):
            """ Update client, region, AND examiner.

            :param str sid: 8-Digit sid from LIMS
            :param CRLDBCursor limsCursor: cursor for the database containing the active sample record for sid
            :param bool fReportError: if True, log contact configuration issues at ERROR level (sends email)
            :param list|None warnings: a list to which warnings will be appended, to be checked by the caller
            :return: a boolean indicating success
            """
            if warnings is None:
                warnings = []
            fSuccess = False

            try:
                config = self._xmit_config
                if not limsCursor:
                    limsCursor = self.getLIMSCursorForSid(sid)
                sidRec = self.getLIMSSampleFieldsForSid(sid, ('client_id',
                                                              'region_id',
                                                              'examiner',
                                                              'state_id',
                                                              'sex',
                                                              'policy_amount',
                                                              'entry_date',
                                                              'dob'), limsCursor)
                if not sidRec:
                    sidRec = ('', '', '', '', '', '', 0, datetime.datetime(1900, 1, 1), datetime.datetime(1900, 1, 1))
                sidDummy, client, region, examiner, state, sex, policy, entry_date, dob = sidRec
                entry_date = entry_date.strftime('%d-%b-%Y')
                if dob:
                    dob = "'{dob:%d-%b-%Y}'".format(dob=dob)
                else:
                    dob = 'null'
                if not examiner:
                    examiner = ''
                asapContact = self.getASAPContactForSid(sid, fReportError)
                if asapContact:
                    limsCursor.execute('''
                        select specimen_container_id
                        from sample_specimen_containers
                        where sid = '{sid!s:s}'
                        '''.format(sid=sid))
                    rec = limsCursor.fetch(True)
                    limsCursor.rollback()
                    if not rec:
                        # cannot check if no specimen containers, so return false for recoding
                        warnings.append(self.WARNING_NO_SPECIMEN_CONTAINERS)
                        self.asapLogger.warn(
                            'Unable to updateClientRegionExaminer due to missing containers for sid {sid!s:s}'.format(sid=sid))
                        return False

                    contId = rec[0]
                    sPanelQuery = '''
                        select ils_db_calc_panel_sf('{client!s:s}', '{region!s:s}', '{contId!s:s}', '{state!s:s}', '{sex!s:s}', {policy:d}, '{entry_date!s:s}', {dob!s:s})
                        from sample limit to 1 row
                        '''
                    sip = config.getCursor(config.DB_NAME_SIP)
                    sip.execute(sPanelQuery.format(client=client, region=region,
                                                   contId=contId, state=state, sex=sex,
                                                   policy=policy, entry_date=entry_date, dob=dob))
                    rec = sip.fetch(True)
                    sip.rollback()
                    oldPanel = rec[0]
                    sip.execute(sPanelQuery.format(client=asapContact.client_id, region=asapContact.region_id,
                                                   contId=contId, state=state, sex=sex,
                                                   policy=policy, entry_date=entry_date, dob=dob))
                    rec = sip.fetch(True)
                    sip.rollback()
                    newPanel = rec[0]
                    if not oldPanel == newPanel:
                        warnings.append(self.WARNING_NEEDS_PANEL_CHANGE)  # panel change is needed, so cannot recode
                    else:
                        updateExaminer = examiner
                        if asapContact.examiner:
                            updateExaminer = asapContact.examiner

                        limsCursor.execute('''
                            update sample set client_id = '{client_id!s:s}', region_id = '{region_id!s:s}',
                            examiner = '{examiner!s:s}'
                            where sid = '{sid!s:s}'
                            '''.format(client_id=asapContact.client_id,
                                       region_id=asapContact.region_id,
                                       examiner=updateExaminer,
                                       sid=sid))
                        limsCursor.commit()
                        commentState = 'prod'
                        if self.devState.isDevInstance():
                            commentState = 'dev'
                        if (asapContact.client_id.strip() != client.strip() or
                                asapContact.region_id.strip() != region.strip()):
                            addComment(sid,
                                       ('CLNT/RGN UPDATED TO {newClient!s:s}/{newRegion!s:s} FROM {oldClient!s:s}/{oldRegion!s:s}'
                                        .format(newClient=asapContact.client_id, newRegion=asapContact.region_id,
                                                oldClient=client, oldRegion=region),),
                                       commentState)
                        if updateExaminer.strip() != examiner.strip():
                            addComment(sid,
                                       ('EXAMINER UPDATED TO {newExaminer!s:s} FROM {oldExaminer!s:s}'
                                        .format(newExaminer=updateExaminer, oldExaminer=examiner),),
                                       commentState)
                        # if sid is in snip and either client or region changed, add to sidlist
                        # to send to VMS to be picked up by HL7 driver and retransmitted to WebOasis
                        if (limsCursor.name == config.DB_NAME_SNIP and (asapContact.client_id != client or
                                                                        asapContact.region_id != region)):

                            sTest = ''
                            if self.devState.isDevInstance():
                                sTest = 'test'
                            sidFile = open(os.path.join(r'\\ntsys1\ils_appl\data\xmit\ftp\asap',
                                                        sTest, r'oasis_retrans\sidlist.txt'), 'a')
                            sidFile.write('{sid!s:s}\n'.format(sid=sid))
                            sidFile.close()

                        fSuccess = True
            except Exception:
                warnings.append(self.ERROR_UNKNOWN)  # flag unknown error
                self.asapLogger.warn(
                    'Unable to updateClientRegionExaminer for sid {sid!s:s}: {tb!s:s}'.format(sid=sid, tb=str(traceback.format_exc())))

            return fSuccess

        def getASAPRelatedClients(self, clientId):
            """
            Determines if a client is related via verifier ID
            to a client that has a region setup for ASAP
            (the ESUB report ID). The client ID is returned if
            one exists.

            :param str clientId: client_id from LIMS
            :return: a list of client_id's
            :rtype: list[str]
            """
            relatedClientIds = []
            cursor = self._xmit_config.getCursor(self._xmit_config.DB_NAME_SIP)
            cursor.execute('''
                select crr.client_id
                from client_region_reports crr inner join client c
                on crr.client_id = c.client_id
                where crr.report_id = 'ESUB'
                and c.verifier_id = (select c2.verifier_id
                                     from client c2
                                     where c2.client_id = '{clientId!s:s}')
                order by crr.client_id
                '''.format(clientId=clientId))
            recs = cursor.fetch()
            cursor.rollback()
            if recs:
                relatedClientIds = [rec for rec, in recs]
            return relatedClientIds

        def getASAPContactForSid(self, sid, fReportError=True):
            """
            Given a sid that may not be coded correctly,
            determine the contact for its related ASAP setup.

            :param str sid:
            :param bool fReportError:
            :returns: the matching Contact
            """
            newContact = None
            rec = None
            limsCursor = self.getLIMSCursorForSid(sid)

            if limsCursor:
                sQuery = '''
                select client_id
                from sample
                where sid = '{sid!s:s}'
                and hold_flag_id not in ('~','#')
                '''.format(sid=sid)
                limsCursor.execute(sQuery)
                rec = limsCursor.fetch(True)
                limsCursor.rollback()
            if rec:
                clientId, = rec
                newClients = self.getASAPRelatedClients(clientId)
                if newClients:
                    acord = self._xmit_config.getCursor(self._xmit_config.DB_NAME_ACORD)
                    acord.execute('''
                        select ao.source_code, ap.full_name
                        from acord_party ap, acord_order ao
                        where ap.acord_order_id = ao.acord_order_id and
                        ao.sampleid = '{sid!s:s}' and
                        ao.source_code like 'ESubmissions-%' and
                        ap.uuid = (select related_obj_uuid
                                   from acord_relation
                                   where acord_order_id = ap.acord_order_id
                                   and relation_role_code = 87)
                        '''.format(sid=sid))
                    rec = acord.fetch(True)
                    acord.rollback()
                    if rec:
                        sourceCode, carrierName = rec
                        issueList = []
                        for contact in self._xmit_config.getContacts().values():
                            fClientMatch = False
                            if contact.client_id in newClients:
                                fClientMatch = True
                            fSourceMatch = False
                            if contact.source_code == sourceCode:
                                fSourceMatch = True
                            fCarrierNameMatch = False
                            if carrierName in contact.acordCarrierNames:
                                fCarrierNameMatch = True
                            if fClientMatch and fSourceMatch and fCarrierNameMatch:
                                newContact = contact
                                break
                            if (fClientMatch and fSourceMatch) and not fCarrierNameMatch:
                                issueList.append(
                                    "Contact {contact_id!s:s} matches client and source, but carrier name '{carrier!s:s}' not found."
                                    .format(contact_id=contact.contact_id, carrier=carrierName)
                                )
                            if (fClientMatch and fCarrierNameMatch) and not fSourceMatch:
                                issueList.append(
                                    "Contact {contact_id!s:s} matches client and carrier name, but source '{source!s:s}' not found."
                                    .format(contact_id=contact.contact_id, source=sourceCode)
                                )
                            if (fCarrierNameMatch and fSourceMatch) and not fClientMatch:
                                issueList.append(
                                    "Contact {contact_id!s:s} matches source and carrier name, but clients {clients!s:s} not found."
                                    .format(contact_id=contact.contact_id, clients=str(newClients))
                                )
                        if not newContact and issueList:
                            logger = self.asapLogger
                            message = ("ASAP contact not found for sid {sid!s:s} with the following comments:\n\n{issues!s:s}"
                                       .format(sid=sid, issues='\n'.join(issueList)))
                            if fReportError:
                                logger.error(message)
                            else:
                                logger.warn(message)
            return newContact

        def getCarrierCodeForCase(self, asapCase):
            """

            :param ASAPCase asapCase:
            :return: the CASESOURCE.CARRIERDESC associated with the sids source_code in ILS_QC
            """
            carrier = ''
            qc = self._xmit_config.getCursor(self._xmit_config.DB_NAME_CASE_QC)
            sQuery = '''
                select s.carrierdesc
                from casesource s with (nolock), casemaster m with (nolock)
                where s.sourcecode = m.source_code
                and s.naic = m.naic
                and m.sampleid = '{sid!s:s}'
                '''.format(sid=asapCase.sid)
            qc.execute(sQuery)
            rec = qc.fetch(True)
            qc.rollback()
            if rec:
                carrier, = rec
            return carrier

        def getTrackingIdForSid(self, sid):
            trackingId = None
            xmitConfig = self.getXmitConfig()
            qc = xmitConfig.getCursor(xmitConfig.DB_NAME_CASE_QC)
            sQuery = '''
                select trackingid
                from casemaster with (nolock)
                where sampleid = '{sid!s:s}'
                '''.format(sid=sid)
            qc.execute(sQuery)
            rec = qc.fetch(True)
            qc.rollback()
            if rec:
                trackingId, = rec
            return trackingId

        def addConsentForSid(self, sid, sDeltaDb='ils_consent'):
            """
            Adds most recently scanned consent for sid to ils_qc

            :param str sid:
            :param str sDeltaDb:
            """
            fRet = False
            cursor = self._xmit_config.getCursor(self._xmit_config.DB_NAME_DELTA_QC)
            sMaxDocIdQuery = '''
                select max(d.documentid)
                from {sDeltaDb!s:s}..tbldocuments d with (nolock)
                inner join {sDeltaDb!s:s}..tblfolders f with (nolock)
                on d.folderid = f.folderid
                where f.value_1 = '{sid!s:s}'
                '''.format(sDeltaDb=sDeltaDb, sid=sid)
            cursor.execute(sMaxDocIdQuery)
            rec = cursor.fetch(True)
            cursor.rollback()
            if rec and rec[0]:
                fRet = DeltaUtility.copyDocument(rec[0], 'Lab Receipt/Urine/Blood test',
                                                 sDeltaDb, 'ils_qc')
            return fRet

        def setExportFlag(self, asapDocument, flagValue):
            """
            Set export flag for image related to ASAPDocument object to either
            'self.DOC_EXPORTED_YES' or 'self.DOC_EXPORTED_NO'.

            :param ASAPDocument asapDocument: the document for which to set the export flag
            :param str flagValue: one character string [Y|N|I|P]
            :return: bool indicating success
            """
            fSuccess = False

            cursor = self._xmit_config.getCursor(self._xmit_config.DB_NAME_DELTA_QC)
            if cursor:
                exportField = self._xmit_config.getSetting(self._xmit_config.SETTING_DELTA_EXPORT_FIELD)
                iRet = cursor.execute('''
                    update tblfolders set {exportField!s:s} = '{flagValue!s:s}'
                    where folderid = (select folderid
                    from tbldocuments with (nolock)
                    where documentid = {docid:d})
                    '''.format(exportField=exportField,
                               flagValue=flagValue,
                               docid=asapDocument.getDocumentId()))
                cursor.commit()
                if iRet == 1:
                    fSuccess = True
                else:
                    self.asapLogger.warn('Failed to set export flag to {flagValue!s:s} for docid {docid:d}.'
                                         .format(flagValue=flagValue,
                                                 docid=asapDocument.getDocumentId()))
            else:
                self.asapLogger.warn('Delta connection unavailable.')
            return fSuccess

        def moveDocumentsForSid(self, sid, sourceDeltaDb='ils_qc', destDeltaDb='ils_client_documents', deleteSrcDoc=False):
            """ Copies all documents for sid from sourceDeltaDb to destDeltaDb

            :param str sid:
            :param str sourceDeltaDb:
            :param str destDeltaDb:
            """
            processed = []
            failed = []
            cursor = self._xmit_config.getCursor(self._xmit_config.DB_NAME_DELTA_QC)
            # sDocumentTypeQuery = """select IndexId from tblIndexes where IndexName = 'Document Code'"""
            # cursor.execute(sDocumentTypeQuery)
            # docTypeColumn = cursor.fetch(True)
            # cursor.rollback()

            sDocIdQuery = '''
                select distinct d.documentId, t.documentTypeName
                from {sourceDeltaDb!s:s}..tbldocuments d with (nolock)
                inner join {sourceDeltaDb!s:s}..tblfolders f with (nolock) on d.folderid = f.folderid
                inner join {sourceDeltaDb!s:s}..tblDocumentTypes t with (nolock) on t.documentTypeId = d.documentTypeId
                where f.value_1 = '{sid!s:s}'
                '''.format(sourceDeltaDb=sourceDeltaDb, sid=sid)

            cursor.execute(sDocIdQuery)
            recs = cursor.fetch()
            cursor.rollback()

            for documentId, documentTypeName in recs:
                fRet = DeltaUtility.copyDocument(documentId, documentTypeName, sourceDeltaDb, destDeltaDb, True)
                if fRet:
                    # processed.append([documentId, documentTypeName])
                    if deleteSrcDoc:
                        fRet2 = DeltaUtility.deleteDocument(documentId, sourceDeltaDb)
                        if fRet2:
                            processed.append([documentId, documentTypeName])
                        else:
                            failed.append([documentId, documentTypeName])
                    else:
                        processed.append([documentId, documentTypeName])
                else:
                    failed.append([documentId, documentTypeName])
            return processed, failed


    ASAP_UTILITY = ASAPUtility()
