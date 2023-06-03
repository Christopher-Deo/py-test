"""
  Facility:         ILS

  Module Name:      CaseQCFactory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the CaseQCFactory class
      Extracted during refactor/Python 2.7 upgrade of ASAP python modules.
      Originally defined in ASAPSupport.py

  Author:
      Josh Nelson

  Creation Date:
      16-Apr-2019

  Modification History:
      dd-mmm-yyyy   iii     Ticket #
          Desc

"""
from __future__ import division, absolute_import, with_statement, print_function

import os

from .CaseQC import CaseQC, CaseQCHistoryItem, CaseQCIdentity
from CRL.DBCursor import CRLDBCursor


class CaseQCFactory(object):
    """
    Retrieve QC case records by sid or trackingId.
    """

    def __init__(self):
        self._xmitConfig = None
        self.__cursor = None

    def getXmitConfig(self):
        if not self._xmitConfig:
            from .Utility import ASAP_UTILITY
            self._xmitConfig = ASAP_UTILITY.getXmitConfig()
        return self._xmitConfig

    def _getCursor(self):
        """

        :rtype: CRLDBCursor
        """
        if not self.__cursor:
            config = self.getXmitConfig()
            self.__cursor = config.getCursor(config.DB_NAME_CASE_QC)
        return self.__cursor

    def __getCases(self, whereClause):
        """

        :param whereClause:
        :return:
        :rtype: list[CaseQC]
        """
        cases = []
        sQuery = '''
            select sampleid, trackingid, state, created_dt, lastviewed_by,
            lastviewed_dt, first_name, last_name, ssn, policy_number,
            c.source_code, c.naic, s.carrierdesc, date_received
            from casemaster c with (nolock), casesource s with (nolock)
            where c.source_code = s.sourcecode and c.naic = s.naic
            and {whereClause!s:s}
            '''.format(whereClause=whereClause)
        sHisQuery = '''
            select comment, action, documentid, h.documenttypeid, documenttypename, pageid,
            created_by, created_dt from casehistory h with (nolock)
            left outer join tbldocumenttypes t with (nolock)
            on h.documenttypeid = t.documenttypeid
            where sampleid = '{sid!s:s}'
            order by created_dt
            '''
        cursor = self._getCursor()
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            for rec in recs:
                (sid, trackingId, state, createdDate, lastViewedBy, lastViewedDate, firstName,
                 lastName, ssn, policyNumber, sourceCode, naic, carrierDesc, dateReceived) = rec

                case = CaseQC(str(sid), trackingId, state, createdDate, lastViewedBy, lastViewedDate, firstName,
                              lastName, ssn, policyNumber, sourceCode, naic, carrierDesc, dateReceived)
                cursor.execute(sHisQuery.format(sid=case.sid))
                hisRecs = cursor.fetch()
                cursor.rollback()
                if hisRecs:
                    for hisRec in hisRecs:
                        (comment, action, documentId, documentTypeId, documentType,
                         pageId, createdBy, createdDate) = hisRec
                        if not documentType:
                            documentType = ''

                        hist = CaseQCHistoryItem(comment, action, documentId, documentTypeId, documentType,
                                                 pageId, createdBy, createdDate)
                        case.history.append(hist)
                cases.append(case)
        return cases

    def fromSid(self, sid, returnAll=False):
        cases = self.__getCases("sampleid = '{sid!s:s}'".format(sid=sid))
        if cases:
            if returnAll:
                return cases
            else:
                return cases[0]
        return []

    def fromTrackingId(self, trackingId):
        return self.__getCases("trackingid = '{trackingId!s:s}'".format(trackingId=trackingId))

    def addNewCase(self, caseQc):
        """
        Will add new case record to casemaster table given CaseQC object.
        Safeguards are in place to prevent an insert if either the sid or trackingId
        already exist in the table.
        """
        fSuccess = False
        self.__cursor.execute('''
                select count(objectid) from casemaster with (nolock)
                where sampleid = '%s' or trackingid = '%s'
                ''' % (caseQc.sid, caseQc.trackingId))
        count, = self.__cursor.fetch(True)
        self.__cursor.rollback()
        if count == 0:
            objectid = CaseQCIdentity().getNewIdValue(CaseQCIdentity.TBL_CASEMASTER)
            if objectid:
                sInsert = '''
                    insert into casemaster (objectid, state, trackingid, sampleid,
                        created_by, created_dt, first_name, last_name, ssn,
                        policy_number, source_code, naic, date_received)
                    values ({objectid:d},'{state!s:s}','{trackingId!s:s}','{sid!s:s}',
                        'ESubmissions', current_timestamp,'{firstName!s:s}', '{lastName!s:s}','{ssn!s:s}',
                        '{policyNumber!s:s}', '{sourceCode!s:s}','{naic!s:s}','{dateReceived:%d-%b-%Y %H:%M:%S}')
                    '''.format(objectid=objectid, state=caseQc.state, trackingId=caseQc.trackingId, sid=caseQc.sid,
                               firstName=caseQc.firstName.replace("'", "''"), lastName=caseQc.lastName.replace("'", "''"), ssn=caseQc.ssn,
                               policyNumber=caseQc.policyNumber, sourceCode=caseQc.sourceCode, naic=caseQc.naic, dateReceived=caseQc.dateReceived)
                self.__cursor.execute(sInsert)
                self.__cursor.commit()
                fSuccess = True
        return fSuccess

    def addNewCaseFromOrder(self, acordOrder):
        caseQc = CaseQC()
        caseQc.sid = acordOrder.sid
        caseQc.trackingId = acordOrder.trackingId
        caseQc.sourceCode = acordOrder.sourceCode
        caseQc.naic = acordOrder.naic
        caseQc.firstName = acordOrder.firstName
        caseQc.lastName = acordOrder.lastName
        caseQc.ssn = acordOrder.ssn
        caseQc.policyNumber = acordOrder.policyNumber
        caseQc.dateReceived = acordOrder.dateReceived
        return self.addNewCase(caseQc)

    def addHistoryItem(self, caseQc, histItem):
        """

        :param CaseQC caseQc:
        :param CaseQCHistoryItem histItem:
        :return: bool indicating success
        """
        key = CaseQCIdentity().getNewIdValue(CaseQCIdentity.TBL_CASEHISTORY)
        sInsert = '''
            insert into casehistory values
            ({key:d}, '{sid!s:s}', '{comment!s:s}', {pageId:d}, {docId:d}, '{action!s:s}', '{createdBy!s:s}', current_timestamp, null, null, {docTypeId:d})
            '''.format(key=key, sid=caseQc.sid, comment=histItem.comment,
                       pageId=histItem.pageId, docId=histItem.documentId,
                       action=histItem.action, createdBy=histItem.createdBy,
                       docTypeId=histItem.documentTypeId)
        cursor = self._getCursor()
        cursor.execute(sInsert)
        cursor.commit()
        caseQc.history.append(histItem)
        fSuccess = True
        return fSuccess

    def cancelCase(self, caseQc):
        """ Given a well-formed CaseQC object, cancel it in the database.

        :param CaseQC caseQc:
        :returns: bool indicating success
        """
        fSuccess = False
        sCancel = '''
            update casemaster set source_code = 'CANCEL'
            where sampleid = '{sid!s:s}'
            '''.format(sid=caseQc.sid)
        cursor = self._getCursor()
        iret = cursor.execute(sCancel)
        cursor.commit()
        if iret == 1:
            fSuccess = True
        return fSuccess

    def uncancelCase(self, caseQc, acordOrder):
        """
        Uncancel a case that was previously cancelled
        order is uncancelled separately (for now)
        """
        fSuccess = False
        sUncancelCaseMaster = """
            update casemaster
            set source_code = '{source_code!s:s}'
            where Trackingid = '{trackingId!s:s}'
            """.format(source_code=acordOrder.sourceCode,
                       trackingId=caseQc.trackingId)
        cursor = self._getCursor()
        iret = cursor.execute(sUncancelCaseMaster)
        cursor.commit()
        if iret == 1:
            histItem = CaseQCHistoryItem()
            histItem.comment = 'uncancelled by user {user!s:s}'.format(user=os.getenv('username'))
            histItem.createdBy = os.getenv('username')
            histItem.action = 'Uncancel'
            if self.addHistoryItem(caseQc, histItem):
                fSuccess = True
            else:
                fSuccess = False
        return fSuccess

    def deleteCase(self, caseQc):
        """
        THIS PERMANENTLY REMOVES THE CASE AND ITS HISTORY/COMMENT RECORDS!
        DO NOT USE UNLESS ABSOLUTELY NECESSARY!

        :param CaseQC caseQc:
        """
        sDeleteCase = '''
            delete from casemaster where sampleid = '{sid!s:s}'
            '''.format(sid=caseQc.sid)
        sDeleteHistory = '''
            delete from casehistory where sampleid = '{sid!s:s}'
            '''.format(sid=caseQc.sid)
        sDeleteComments = '''
            delete from comments where sampleid = '{sid!s:s}'
            '''.format(sid=caseQc.sid)
        cursor = self._getCursor()
        cursor.execute(sDeleteCase)
        cursor.execute(sDeleteHistory)
        cursor.execute(sDeleteComments)
        cursor.commit()

    def setCaseState(self, caseQc, qcState):
        """
        Given a well-formed CaseQC object and a valid state, update the state
        in the database.
        """
        fSuccess = False
        if qcState in (caseQc.STATE_NEW,
                       caseQc.STATE_PENDING,
                       caseQc.STATE_RELEASED):
            cursor = self._getCursor()
            cursor.execute('''
                update casemaster set state = '{state!s:s}'
                where sampleid = '{sid!s:s}'
                '''.format(state=qcState, sid=caseQc.sid))
            cursor.commit()
            fSuccess = True
        return fSuccess
