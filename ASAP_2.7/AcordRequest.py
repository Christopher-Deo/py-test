"""

  Facility:         ILS

  Module Name:      ASAPAcordRequest

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the custom class that wraps the ACORD_ASAP_REQUEST
      table, for alerting ACORD to act on an action ASAP has taken (i.e. transmitting files).

  Author:
      Jarrod Wild

  Creation Date:
      01-Jun-2009

  Modification History:
      2-Jun-2011  kg   Ticket 22740
          Replaced the ASAP.getLogger() with self.__getLogger. ASAP.getLogger() doesn't exists.

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7

"""
from __future__ import division, absolute_import, with_statement, print_function
import CRLUtility


class ASAPAcordRequest(object):
    REQ_ACORD_TRANSMIT = 1

    def __init__(self, logger=None):
        self.__xmitConfig = None
        self.__cursor = None
        self.__fact = None
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger

    def _getXmitConfig(self):
        if not self.__xmitConfig:
            from .Utility import ASAP_UTILITY
            self.__xmitConfig = ASAP_UTILITY.getXmitConfig()
        return self.__xmitConfig

    def _getCursor(self):
        if not self.__cursor:
            config = self._getXmitConfig()
            self.__cursor = config.getCursor(config.DB_NAME_ACORD)
        return self.__cursor

    def _getCaseFact(self):
        if not self.__fact:
            from .Utility import ASAP_UTILITY
            self.__fact = ASAP_UTILITY.getViableCaseFactory()
        return self.__fact

    # noinspection PyUnusedLocal
    def __makeRequest(self, case, request=REQ_ACORD_TRANSMIT):
        """

        :param ViableCase case:
        :param int request: currently not used
        :return:
        """
        fSuccess = False
        sid = ''
        trackingId = ''
        naic = ''
        sourceCode = ''
        if case.sample:
            sid = case.sample.sid
        if case.order:
            trackingId = case.order.trackingId
            naic = case.order.naic
            sourceCode = case.order.sourceCode
        # print('sid={sid!s:s}, trackingid={trackingId!s:s}, source={sourceCode!s:s}, naic={naic!s:s}'.format(sid=sid, trackingId=trackingId, sourceCode=sourceCode, naic=naic))
        if sid and trackingId and naic and sourceCode:
            cursor = self._getCursor()
            cursor.execute('''
                select creation_time, completion_time, error from acord_asap_request
                where source_code = '{sourceCode!s:s}'
                and sampleid = '{sid!s:s}'
                and trackingid = '{trackingId!s:s}'
                '''.format(sourceCode=sourceCode, sid=sid, trackingId=trackingId))
            rec = cursor.fetch(True)
            cursor.rollback()
            if not rec:
                cursor.execute("select kennewick_domain_seq.nextval from dual")
                rec = cursor.fetch(True)
                cursor.rollback()
                idVal, = rec
                sInsert = '''
                    insert into acord_asap_request
                    (acord_asap_request_id, source_code, sampleid, trackingid, naic)
                    values ({idVal:d}, '{sourceCode!s:s}', '{sid!s:s}', '{trackingId!s:s}', '{naic!s:s}')
                    '''.format(idVal=int(idVal), sourceCode=sourceCode, sid=sid, trackingId=trackingId, naic=naic)
                cursor.execute(sInsert)
                cursor.commit()
            fSuccess = True
        else:
            self.__logger.warn("Required information missing for ACORD request: " +
                               "sid='{sid!s:s}', trackingid='{trackingId!s:s}', naic='{naic!s:s}', sourcecode='{sourceCode!s:s}'"
                               .format(sid=sid, trackingId=trackingId, naic=naic, sourceCode=sourceCode))
        return fSuccess

    def makeRequestBySid(self, sid, request=REQ_ACORD_TRANSMIT):
        fSuccess = False
        fact = self._getCaseFact()
        case = fact.fromSid(sid)
        if case:
            fSuccess = self.__makeRequest(case, request)
        return fSuccess

    def makeRequestByTrackingId(self, trackingId, request=REQ_ACORD_TRANSMIT):
        fSuccess = False
        fact = self._getCaseFact()
        case = fact.fromTrackingID(trackingId)
        if case:
            fSuccess = self.__makeRequest(case, request)
        return fSuccess

    def makeRequestByRefId(self, refId, request=REQ_ACORD_TRANSMIT):
        fSuccess = False
        fact = self._getCaseFact()
        case = fact.fromRefID(refId)
        if case:
            fSuccess = self.__makeRequest(case, request)
        return fSuccess
