"""
  Facility:         ILS

  Module Name:      ViableCase

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ViableCase class.
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


class ViableCase(object):
    """
    Representation of a collection of objects that have the
    potential to be an ASAP case (and may meet the criteria
    of an actual ASAP case).
    """
    ID_SID = 'sid'
    ID_TRACKINGID = 'trackingId'
    ID_POLICYNUMBER = 'policyNumber'
    ID_REFID = 'refId'
    ID_DOCUMENTID = 'documentId'

    SRC_LIMS = 'LIMS'
    SRC_DELTA_QC = 'Delta QC'
    SRC_ACORD_121 = 'ACORD 121'
    SRC_CASE_QC = 'Case QC'
    SRC_ACORD_103 = 'ACORD 103'
    SRC_ASAP_XMIT = 'ASAP Xmit'

    srcMemberMap = {
        SRC_LIMS: 'sample',
        SRC_DELTA_QC: 'docGroup',
        SRC_ACORD_121: 'order',
        SRC_CASE_QC: 'caseqc',
        SRC_ACORD_103: 'acord103',
        SRC_ASAP_XMIT: 'asapContact'
    }

    ERR_NONE = 0
    ERR_MULTIPLE_ORDERS_ONE_SAMPLE = 1 << 0
    ERR_CASE_EXISTS_FOR_ORDER = 1 << 1
    ERR_NON_ASAP_SAMPLE = 1 << 2
    ERR_CARRIER_MISMATCH = 1 << 3
    ERR_NO_SAMPLE_EXISTS = 1 << 4
    ERR_MISSING_CONSENT = 1 << 5
    ERR_MULTIPLE_SELQ_ORDERS = 1 << 6

    errorDescMap = {
        ERR_MULTIPLE_ORDERS_ONE_SAMPLE: 'The LIMS sample is matched to more than one ACORD ASAP order.',
        ERR_CASE_EXISTS_FOR_ORDER: 'A case QC record already exists for the ACORD order(s).',
        ERR_NON_ASAP_SAMPLE: 'The LIMS sample is not associated with an ASAP imaging contact:',
        ERR_CARRIER_MISMATCH: 'The ACORD order carrier does not match the LIMS sample:',
        ERR_NO_SAMPLE_EXISTS: 'No sample exists in LIMS for this case.',
        ERR_MISSING_CONSENT: 'Consent/labslip document is missing for this case.',
        ERR_MULTIPLE_SELQ_ORDERS: 'There are one or more unmatched SelectQuote orders that match this case:',
    }

    STAT_ORDER_RECEIVED = 'Order received.'
    STAT_KIT_PENDING = 'Labkit is pending.'
    STAT_DOCS_PENDING = 'Documents are pending.'
    STAT_KIT_DOCS_PENDING = 'Labkit and documents are pending.'
    STAT_KIT_RECEIVED = 'Labkit received.'
    STAT_DOCS_RECEIVED = 'Documents received.'
    STAT_KIT_DOCS_RECEIVED = 'Labkit and documents received.'
    STAT_KIT_ORDER_MATCH = 'Labkit and order are matched.'
    STAT_CASE_AVAILABLE = 'Case is available for QC review.'
    STAT_CASE_REVIEW = 'Case is under QC review.'
    STAT_CASE_RELEASED = 'Case images are QC released.'
    STAT_103_PENDING = 'ACORD 103 transaction is pending.'
    STAT_103_RECEIVED = 'ACORD 103 transaction received.'
    STAT_CASE_TRANSMITTED = 'Case transmitted to client.'
    STAT_CASE_RECONCILED = 'Transmission confirmed by client.'

    def __init__(self):
        self.sample = None
        self.asapContact = None
        self.docGroup = None
        self.order = None
        self.caseQc = None
        self.acord103 = None
        self.viableCaseMap = {}  # map of links from this case to other cases: key is ID, value is (fromSrc,toSrc,case)
        self.errors = self.ERR_NONE
        self.errorDetailMap = {}  # map of error IDs to detail info for this case

    def dbgPrint(self, logger=None):
        data = '\n'
        data += '{}: {}\n'.format(self.SRC_LIMS, self.sample)
        data += '{}: {}\n'.format(self.SRC_ACORD_121, self.order)
        data += '{}: {}\n'.format(self.SRC_DELTA_QC, self.docGroup)
        data += '{}: {}\n'.format(self.SRC_CASE_QC, self.caseQc)
        data += '{}: {}\n'.format(self.SRC_ACORD_103, self.acord103)
        data += '{}: {}\n'.format(self.SRC_ASAP_XMIT, self.asapContact)
        if logger:
            logger.debug(data)
        else:
            print(data)
