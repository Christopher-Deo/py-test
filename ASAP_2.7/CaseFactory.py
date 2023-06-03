"""

  Facility:         ILS

  Module Name:      CaseFactory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPCaseFactory class.

  Author:
      Jarrod Wild

  Creation Date:
      02-Nov-2006

  Modification History:
      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
from .Case import ASAPCase
from .Document import ASAPDocument


class ASAPCaseFactory(object):
    """
    This is a factory class for building ASAPCase objects from
    certain key fields.
    """
    def __init__(self, logger=None):
        self._xmitConfig = None
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger

    def _getXmitConfig(self):
        if not self._xmitConfig:
            from .Utility import ASAP_UTILITY
            self._xmitConfig = ASAP_UTILITY.getXmitConfig()
        return self._xmitConfig

    def fromSid(self, sid, asapDocuments=None):
        """ Given a sid, build an ASAPCase object.
        Optionally add documents to case.

        :param str sid:
        :param list[ASAPDocument]|None asapDocuments:
        :rtype: ASAPCase
        """
        if not asapDocuments:
            asapDocuments = []
        case = None
        config = self._getXmitConfig()
        sip = config.getCursor(config.DB_NAME_SIP)
        snip = config.getCursor(config.DB_NAME_SNIP)
        qcCursor = config.getCursor(config.DB_NAME_CASE_QC)
        if sip and snip and qcCursor:
            tempCase = ASAPCase()
            tempCase.sid = sid
            sContactQuery = '''
                select client_id, region_id, upper(examiner)
                from sample where sid = '{sid!s:s}'
                and hold_flag_id not in ('~','#')
                '''.format(sid=sid)
            # the majority of the time, the sample will be in
            # snip by the time it is reviewed for transmission,
            # so check there first to save time
            limsCursor = snip
            limsCursor.execute(sContactQuery)
            rec = limsCursor.fetch(True)
            limsCursor.rollback()
            if not rec:
                limsCursor = sip
                limsCursor.execute(sContactQuery)
                rec = limsCursor.fetch(True)
                limsCursor.rollback()
            if rec:
                client, region, examiner = rec
                if region:
                    region = region.strip()
                if examiner:
                    examiner = examiner.strip()
                contact = config.getContact(client, region, examiner)
                if contact:
                    tempCase.contact = contact
                    sCaseQuery = '''
                        select trackingid, source_code
                        from casemaster with (nolock)
                        where sampleid = '{sid!s:s}'
                        '''.format(sid=sid)
                    qcCursor.execute(sCaseQuery)
                    rec = qcCursor.fetch(True)
                    qcCursor.rollback()
                    if rec:
                        tempCase.trackingId, tempCase.source_code = rec
                        fAllDocsAdded = True
                        for asapDocument in asapDocuments:
                            if not tempCase.addDocument(asapDocument) and asapDocument.fBill and asapDocument.fSend:
                                self.__logger.warn('Document {docid:d} not set up for billing, please review (contact {contact_id!s:s}).'
                                                   .format(docid=asapDocument.getDocumentId(), contact_id=contact.contact_id))
                                fAllDocsAdded = False
                        if fAllDocsAdded:
                            case = tempCase
                    else:
                        self.__logger.warn(
                            'No case record found in casemaster table ' +
                            'for sample {sid!s:s}.'.format(sid=sid))
                else:
                    self.__logger.warn(
                        'No ASAP contact found for sample {sid!s:s}.'.format(sid=sid))
            else:
                self.__logger.warn(
                    'Sample for sid {sid!s:s} does not exist in LIMS.'.format(sid=sid))
        else:
            self.__logger.warn('One or more cursors unavailable.')
        return case

    def fromTrackingId(self, trackingId, asapDocuments=None):
        """ Given a trackingid, build an ASAPCase object.
        Optionally add documents to case.

        :param str|unicode trackingId:
        :param list[ASAPDocument]|None asapDocuments:
        :rtype: ASAPCase
        """
        if not asapDocuments:
            asapDocuments = []
        case = None
        config = self._getXmitConfig()
        sip = config.getCursor(config.DB_NAME_SIP)
        snip = config.getCursor(config.DB_NAME_SNIP)
        qcCursor = config.getCursor(config.DB_NAME_CASE_QC)
        if sip and snip and qcCursor:
            tempCase = ASAPCase()
            tempCase.trackingId = trackingId
            sCaseQuery = '''
                select sampleid, source_code
                from casemaster with (nolock)
                where trackingid = '{trackingId!s:s}'
                '''.format(trackingId=trackingId)
            qcCursor.execute(sCaseQuery)
            rec = qcCursor.fetch(True)
            qcCursor.rollback()
            if rec:
                tempCase.sid, tempCase.source_code = rec
                sContactQuery = '''
                    select client_id, region_id, upper(examiner)
                    from sample where sid = '{sid!s:s}'
                    and hold_flag_id not in ('~','#')
                    '''.format(sid=tempCase.sid)
                # the majority of the time, the sample will be in
                # snip by the time it is reviewed for transmission,
                # so check there first to save time
                limsCursor = snip
                limsCursor.execute(sContactQuery)
                rec = limsCursor.fetch(True)
                limsCursor.rollback()
                if not rec:
                    limsCursor = sip
                    limsCursor.execute(sContactQuery)
                    rec = limsCursor.fetch(True)
                    limsCursor.rollback()
                if rec:
                    client, region, examiner = rec
                    contact = config.getContact(client, region, examiner)
                    if contact:
                        tempCase.contact = contact
                        fAllDocsAdded = True
                        for asapDocument in asapDocuments:
                            if not tempCase.addDocument(asapDocument) and asapDocument.fBill and asapDocument.fSend:
                                self.__logger.warn('Document {docid:d} not set up for billing, please review (contact {contact_id!s:s}).'
                                                   .format(docid=asapDocument.getDocumentId(), contact_id=contact.contact_id))
                                fAllDocsAdded = False
                        if fAllDocsAdded:
                            case = tempCase
                    else:
                        self.__logger.warn(
                            'No ASAP contact found for sample {sid!s:s}.'.format(sid=tempCase.sid))
                else:
                    self.__logger.warn(
                        'Sample for sid {sid!s:s} does not exist in LIMS.'.format(sid=tempCase.sid))
            else:
                self.__logger.warn(
                    'No case record found in casemaster table ' +
                    'for order {trackingId!s:s}.'.format(trackingId=trackingId))
        else:
            self.__logger.warn('One or more cursors unavailable.')
        return case

    def casesForDocuments(self, asapDocuments):
        """ Given a list of ASAPDocument objects, build the appropriate
        ASAPCase objects and add the documents to those cases, then
        return the list of ASAPCase objects.

        :param list[ASAPDocument] asapDocuments:
        :rtype: list[ASAPCase]
        """
        # map sid to case
        caseDict = {}
        config = self._getXmitConfig()
        cursor = config.getCursor(config.DB_NAME_DELTA_QC)
        for doc in asapDocuments:
            sidField = config.getSetting(config.SETTING_DELTA_SID_FIELD)
            sQuery = '''
                select f.{sidField} from tblfolders f with (nolock)
                inner join tbldocuments d with (nolock)
                on f.folderid = d.folderid
                where d.documentid = {docid:d}
                '''.format(sidField=sidField, docid=doc.getDocumentId())
            cursor.execute(sQuery)
            rec = cursor.fetch(True)
            cursor.rollback()
            if rec:
                sid, = rec
                case = caseDict.get(sid)
                if not case:
                    case = self.fromSid(sid)
                    if case:
                        caseDict[sid] = case
                if case:
                    case.addDocument(doc)
                else:
                    self.__logger.warn(
                        'Unable to locate a case for document (docid {docid:d}, sid {sid!s:s}).'
                        .format(docid=doc.getDocumentId(), sid=sid))
            else:
                self.__logger.warn(
                    'Unable to find document for docid {docid:d}.'.format(docid=doc.getDocumentId()))
        return list(caseDict.values())
