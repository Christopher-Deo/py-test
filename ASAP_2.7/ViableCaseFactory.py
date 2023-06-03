"""
  Facility:         ILS

  Module Name:      ViableCaseFactory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ViableCaseFactory class.
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

from .ViableCase import ViableCase
from .QCDocumentFactory import QCDocumentFactory
from ..AcordOrderModel import AcordOrderFactory
from .CaseQCFactory import CaseQCFactory
from .Acord103Store import ASAPAcord103Store
from ILS.LimsModel import LimsSampleFactory


class ViableCaseFactory(object):
    """
    Builds ViableCase objects and ViableCases linked to these cases, if there are any.
    Follows search tracks based upon the starting search criterion, and jumps from track
    to track until all data is collected.
    """
    # search tracks:
    # sid: LIMS ->> ACORD 121 -> Delta QC -> Case QC -> ASAP Xmit
    # trackingId: ACORD 121 ->> Case QC -> ACORD 103
    # policyNumber: ACORD 121 -> Case QC -> ACORD 103
    # refId: ACORD 121 -> Delta QC (only via manual review of images)
    def __init__(self, logger=None):
        if not logger:
            import CRLUtility
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger
        self.__sampleFact = None
        self.__util = None
        self.__docFact = None
        self.__orderFact = None
        self.__qcFact = None
        self.__103Store = None
        self.__sidCaseMap = {}
        self.__trackingIdCaseMap = {}

    def _getUtil(self):
        if not self.__util:
            from .Utility import ASAP_UTILITY
            self.__util = ASAP_UTILITY
        return self.__util

    def _getSampleFact(self):
        """

        :rtype: LimsSampleFactory
        """
        if not self.__sampleFact:
            self.__sampleFact = LimsSampleFactory(True)
        return self.__sampleFact

    def _getDocFact(self):
        """

        :rtype: QCDocumentFactory
        """
        if not self.__docFact:
            self.__docFact = self._getUtil().getQCDocumentFactory()
        return self.__docFact

    def _getOrderFact(self):
        """

        :rtype: AcordOrderFactory
        """
        if not self.__orderFact:
            self.__orderFact = self._getUtil().getAcordOrderFactory()
        return self.__orderFact

    def _getCaseQCFact(self):
        """

        :rtype: CaseQCFactory
        """
        if not self.__qcFact:
            self.__qcFact = self._getUtil().getCaseQCFactory()
        return self.__qcFact

    def _get103Store(self):
        """

        :rtype: ASAPAcord103Store
        """
        if not self.__103Store:
            self.__103Store = self._getUtil().getAcord103Store()
        return self.__103Store

    def __getMethod(self, paramId):
        method = None
        try:
            # method = getattr(self, '__{paramId!s:s}SearchTrack__'.format(paramId=paramId))
            method = getattr(self, '__{paramId!s:s}SearchTrack2__'.format(paramId=paramId))
        except Exception:
            pass
        return method

    @staticmethod
    def __getMember(obj, memberId):
        member = None
        try:
            member = getattr(obj, memberId)
        except Exception:
            pass
        return member

    def __sidSearchTrack2__(self, sid, viableCase):
        self.__logger.debug('sid search track2 for "{sid!s:s}"'.format(sid=sid))
        viableCase.dbgPrint(self.__logger)
        # break out of search track if sid is the placeholder xxxxxxxx
        # (to avoid horrible and unnecessary recursive searching)
        if sid and sid.upper() == 'XXXXXXXX':
            return
        if not self.__sidCaseMap.get(sid):
            self.__sidCaseMap[sid] = viableCase
            util = self._getUtil()
            viableCase.asapContact = util.getASAPContactForSid(sid, False)
            viableCase.sample = self._getSampleFact().fromSid(sid)
            viableCase.docGroup = self._getDocFact().fromSid(sid)
            viableCase.caseQc = self._getCaseQCFact().fromSid(sid)
            if viableCase.sample and viableCase.asapContact:
                asapCase = util.getCaseFactory().fromSid(sid)
                if asapCase:
                    hist = util.getDocumentHistory()
                    histMap = {}
                    for actionItem in (hist.ACTION_RELEASE, hist.ACTION_INVOICE,
                                       hist.ACTION_TRANSMIT, hist.ACTION_RECONCILE):
                        histRecs = hist.getTrackedDocidsForCase(asapCase, actionItem)
                        for docid, actionDate in histRecs:
                            dochist = histMap.get(docid)
                            if not dochist:
                                dochist = []
                                histMap[docid] = dochist
                            dochist.append((actionDate, actionItem))
                    if histMap and viableCase.docGroup:
                        for doc in viableCase.docGroup.documents:
                            doc.transmitHistory = histMap.get(doc.documentId)
                            if not doc.transmitHistory:
                                doc.transmitHistory = []
        orders = self._getOrderFact().fromSid(sid)
        if orders:
            for order in orders:
                if viableCase.order and viableCase.order.trackingId == order.trackingId:
                    orders.remove(order)
                    break
            asapOrders = []
            nonAsapOrders = []
            for order in orders:
                if order.sourceCode.startswith('ESubmissions-'):
                    asapOrders.append(order)
                else:
                    nonAsapOrders.append(order)
            if asapOrders:
                viableCase.order = asapOrders[0]
                for order in asapOrders[1:]:
                    if not order.dateCancelled:
                        newCase = ViableCase()
                        newCase.sample = viableCase.sample
                        newCase.order = order
                        cases = viableCase.viableCaseMap.get(viableCase.ID_TRACKINGID)
                        if not cases:
                            cases = []
                            viableCase.viableCaseMap[viableCase.ID_TRACKINGID] = cases
                        cases.append((newCase.SRC_ACORD_121, newCase.SRC_LIMS, newCase))
                self.__trackingIdSearchTrack2__(viableCase.order.trackingId, viableCase)
            elif not viableCase.order:
                viableCase.order = nonAsapOrders[0]
        cases = viableCase.viableCaseMap.get(viableCase.ID_SID)
        if cases:
            for fromSrc, toSrc, case in cases:
                memberObj = self.__getMember(case, fromSrc)
                if memberObj:
                    caseSid = self.__getMember(memberObj, case.ID_SID)
                    if caseSid == sid:
                        case.sample = memberObj
                    else:
                        self.__sidSearchTrack2__(caseSid, case)

    def __trackingIdSearchTrack2__(self, trackingId, viableCase):
        self.__logger.debug('trackingId search track2 for "{trackingId!s:s}"'.format(trackingId=trackingId))
        viableCase.dbgPrint(self.__logger)
        if not self.__trackingIdCaseMap.get(trackingId):
            self.__trackingIdCaseMap[trackingId] = viableCase
            if not viableCase.order:
                viableCase.order = self._getOrderFact().fromTrackingId(trackingId)
            caseQcs = self._getCaseQCFact().fromTrackingId(trackingId)
            if caseQcs:
                for caseQc in caseQcs:
                    if viableCase.caseQc and viableCase.caseQc.sid == caseQc.sid:
                        caseQcs.remove(caseQc)
                        break
                for caseQc in caseQcs:
                    if viableCase.order and viableCase.order.sid == caseQc.sid:
                        viableCase.caseQc = caseQc
                        caseQcs.remove(caseQc)
                        break
                for caseQc in caseQcs:
                    newCase = ViableCase()
                    newCase.order = viableCase.order
                    newCase.caseQc = caseQc
                    cases = viableCase.viableCaseMap.get(viableCase.ID_SID)
                    if not cases:
                        cases = []
                        viableCase.viableCaseMap[viableCase.ID_SID] = cases
                    cases.append((newCase.SRC_CASE_QC, newCase.SRC_ACORD_121, newCase))
        if viableCase.order:
            self.__sidSearchTrack2__(viableCase.order.sid, viableCase)
        if not viableCase.acord103:
            xmlRecs = self._get103Store().getByTrackingId(trackingId)
            if xmlRecs:
                viableCase.acord103 = xmlRecs[0]
        cases = viableCase.viableCaseMap.get(viableCase.ID_TRACKINGID)
        if cases:
            for fromSrc, toSrc, case in cases:
                memberObj = self.__getMember(case, fromSrc)
                if memberObj:
                    caseTrackingId = self.__getMember(memberObj, case.ID_TRACKINGID)
                    if caseTrackingId == trackingId:
                        case.order = memberObj
                    else:
                        self.__trackingIdSearchTrack2__(caseTrackingId, case)

    def __policyNumberSearchTrack2__(self, policyNumber, viableCase):
        self.__logger.debug('policyNumber search track2 for "{policyNumber!s:s}"'.format(policyNumber=policyNumber))
        viableCase.dbgPrint(self.__logger)
        if not viableCase.acord103:
            xmlRecs = self._get103Store().getByPolicyNumber(policyNumber)
            if xmlRecs:
                viableCase.acord103 = xmlRecs[0]
                self.__trackingIdSearchTrack2__(viableCase.acord103.trackingId, viableCase)

    def __refIdSearchTrack2__(self, refId, viableCase):
        self.__logger.debug('refId search track2 for "{refId!s:s}"'.format(refId=refId))
        viableCase.dbgPrint(self.__logger)
        if not viableCase.order:
            orders = self._getOrderFact().fromSelectQuoteRefId(refId)
            if orders:
                viableCase.order = orders[0]
                for order in orders[1:]:
                    newCase = ViableCase()
                    newCase.order = order
                    cases = viableCase.viableCaseMap.get(viableCase.ID_TRACKINGID)
                    if not cases:
                        cases = []
                        viableCase.viableCaseMap[viableCase.ID_TRACKINGID] = cases
                    cases.append((newCase.SRC_ACORD_121, newCase.SRC_ACORD_121, newCase))
                self.__trackingIdSearchTrack2__(viableCase.order.trackingId, viableCase)

    def __documentIdSearchTrack2__(self, documentId, viableCase):
        self.__logger.debug('documentId search track2 for "{documentId!s:s}"'.format(documentId=documentId))
        viableCase.dbgPrint(self.__logger)
        if not viableCase.sample:
            doc = self._getDocFact().fromDocumentId(int(documentId))
            if doc:
                self.__sidSearchTrack2__(doc.indexes[doc.IDX_SID], viableCase)

    def fromParameter(self, paramId, paramValue):
        """
        Build case based on ViableCase.ID_*, passing value for ID.
        Factory will determine the search track and begin gathering
        all information possible.
        """
        self.__sidCaseMap = {}
        self.__trackingIdCaseMap = {}
        viableCase = ViableCase()
        method = self.__getMethod(paramId)
        if method:
            method(paramValue, viableCase)
            return viableCase
        else:
            return None

    def fromSid(self, paramValue):
        """
        Build case based on ViableCase.ID_*, passing value for ID.
        Factory will determine the search track and begin gathering
        all information possible.
        """
        return self.fromParameter(ViableCase.ID_SID, paramValue)

    def fromTrackingID(self, paramValue):
        """
        Build case based on ViableCase.ID_*, passing value for ID.
        Factory will determine the search track and begin gathering
        all information possible.
        """
        return self.fromParameter(ViableCase.ID_TRACKINGID, paramValue)

    def fromRefID(self, paramValue):
        """
        Build case based on ViableCase.ID_*, passing value for ID.
        Factory will determine the search track and begin gathering
        all information possible.
        """
        return self.fromParameter(ViableCase.ID_REFID, paramValue)

    def analyzeCase(self, viableCase):
        """
        Return an output string summarizing the status of the case
        as it relates to its readiness to transmit to the carrier.

        :param ViableCase viableCase:
        """
        resultsDependentClientsMap = {
            'AGI': 'American General',
            'MNM': 'Minnesota Life',
            'TRO': 'Transamerica',
            'PIC': 'Prudential',
            'UST': 'American General'
        }
        if not viableCase:
            sOutput = "This case could not be located in CRL's system"
        elif viableCase.order.dateCancelled:
            sOutput = "This case has been cancelled"
        elif not viableCase.caseQc:
            sOutput = "There is no case record for APPS to review at this time"
        elif not viableCase.sample:
            sOutput = 'CRL has not received a lab sample'
        elif viableCase.caseQc.state != viableCase.caseQc.STATE_RELEASED:
            sOutput = "The case images have not been released by APPS at this time"
        elif not viableCase.sample.transmitDate and viableCase.sample.clientId in resultsDependentClientsMap.keys():
            sOutput = 'Lab results are not yet ready for this case (required for {clientId!s:s})'.format(clientId=resultsDependentClientsMap[viableCase.sample.clientId])
        elif viableCase.sample.clientId == 'ORP':
            sOutput = 'Sample is coded to ORP in CRL\'s system'
        elif not viableCase.asapContact:
            sOutput = ('No ASAP contact found for CLI/REG/EXAMINER {clientId!s:s}/{regionId!s:s}/{examiner!s:s}'
                       .format(clientId=viableCase.sample.clientId, regionId=viableCase.sample.regionId, examiner=viableCase.sample.examiner))
        elif viableCase.asapContact.acord103_dir and not viableCase.acord103:
            sOutput = "CRL has not received an ACORD 103 XML file from APPS at this time"
        elif viableCase.sample.transmitDate:
            sOutput = "Case has previously transmitted to carrier, transmit date = " + str(viableCase.sample.transmitDate)
        else:
            util = self._getUtil()
            xmitCase = util.getCaseFactory().fromSid(viableCase.sample.sid)
            if not util.reStageToTransmit(xmitCase, False):
                sOutput = "Case has previously transmitted to carrier"
            else:
                sOutput = "Case has been restaged to transmit to carrier"
        if viableCase and viableCase.sample and viableCase.sample.sid:
            sOutput += ', sid = {sid!s:s}'.format(sid=viableCase.sample.sid)
        return sOutput
