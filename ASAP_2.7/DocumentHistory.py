"""

  Facility:         ILS

  Module Name:      DocumentHistory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPDocumentHistory class.

  Author:
      Jarrod Wild

  Creation Date:
      31-Oct-2006

  Modification History:
      19-JUN-2012   rsu
        Add fromtimestamp check to allow new apphub runs in the future

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
import datetime
import time
from .Case import ASAPCase
from .Document import ASAPDocument
from .TransmitConfig import ASAPTransmitConfig


class ASAPDocumentHistory(object):
    """
    This class provides access to the asap_document_history table for
    retrieving and adding to the audit trail.
    """
    # table constant for audit trail
    TABLE_DOCUMENT_HISTORY = 'asap_document_history'
    # actionitem constants
    ACTION_RELEASE = 'release'
    ACTION_INVOICE = 'invoice'
    ACTION_TRANSMIT = 'transmit'
    ACTION_RECONCILE = 'reconcile'

    def __init__(self, logger=None):
        self._xmitConfig = None
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger

    def getXmitConfig(self):
        """

        :rtype: ASAPTransmitConfig
        """
        if not self._xmitConfig:
            from .Utility import ASAP_UTILITY
            self._xmitConfig = ASAP_UTILITY.getXmitConfig()
        return self._xmitConfig

    def trackDocument(self, asapDocument, actionItem):
        """

        :param ASAPDocument asapDocument:
        :param str actionItem:
        :return: bool indicating success
        :rtype: bool
        """
        fSuccess = False
        xmitConfig = self.getXmitConfig()
        cursor = xmitConfig.getCursor(xmitConfig.DB_NAME_XMIT)
        sInsert = '''
            insert into {table!s:s} (sid, documentid, contact_id, actionitem, actiondate)
            values ('{sid!s:s}', {docid:d}, '{contact!s:s}', '{action!s:s}', current_timestamp)
            '''.format(table=self.TABLE_DOCUMENT_HISTORY,
                       sid=asapDocument.case.sid,
                       docid=asapDocument.getDocumentId(),
                       contact=asapDocument.case.contact.contact_id,
                       action=actionItem)
        self.__logger.info(sInsert)
        iAttempts = 0
        totalAttempts = 5
        sleepTime = 0.1
        while iAttempts < totalAttempts:
            iAttempts += 1
            iRet = cursor.execute(sInsert)
            if iRet == 1:
                cursor.commit()
                fSuccess = True
                break
            else:
                cursor.rollback()
                if iAttempts < totalAttempts:
                    time.sleep(sleepTime)
                else:
                    self.__logger.warn("Tried insert query {totalAttempts:d} times and failed: {sInsert!s:s}"
                                       .format(totalAttempts=totalAttempts, sInsert=sInsert))
        return fSuccess

    def getDateTracked(self, asapDocument, actionItem):
        """

        :param ASAPDocument asapDocument:
        :param str actionItem:
        :return: the max actiondate of type actionItem for the given document
        :rtype: datetime.datetime
        """
        dateVal = None
        xmitConfig = self.getXmitConfig()
        cursor = xmitConfig.getCursor(xmitConfig.DB_NAME_XMIT)
        sQuery = '''
            select max(actiondate)
            from {table!s:s} with (nolock)
            where sid = '{sid!s:s}'
            and documentid = {docid:d}
            and contact_id = '{contact!s:s}'
            and actionitem = '{action!s:s}'
            '''.format(table=self.TABLE_DOCUMENT_HISTORY,
                       sid=asapDocument.case.sid,
                       docid=asapDocument.getDocumentId(),
                       contact=asapDocument.case.contact.contact_id,
                       action=actionItem)
        cursor.execute(sQuery)
        rec = cursor.fetch(True)
        cursor.rollback()
        if rec:
            dateVal, = rec

        return dateVal

    def getTrackedDocidsForCase(self, asapCase, actionItem):
        """
        Return list of tuples of (documentid, actiondate) for
        case's sid.

        :param ASAPCase asapCase:
        :param str actionItem:
        :rtype: list[(int, datetime.datetime)]
        """
        docid_dates = []
        xmitConfig = self.getXmitConfig()
        cursor = xmitConfig.getCursor(xmitConfig.DB_NAME_XMIT)
        sQuery = '''
            select documentid, max(actiondate)
            from {table!s:s} with (nolock)
            where sid = '{sid!s:s}'
            and contact_id = '{contact!s:s}'
            and actionitem = '{action!s:s}'
            group by documentid
            order by documentid
            '''.format(table=self.TABLE_DOCUMENT_HISTORY,
                       sid=asapCase.sid,
                       contact=asapCase.contact.contact_id,
                       action=actionItem)
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            for docid, dateVal in recs:
                docid_dates.append((docid, dateVal))
        return docid_dates
