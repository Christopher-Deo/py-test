"""
  Facility:         ILS

  Module Name:      CaseQC

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the CaseQC, CaseQCHistoryItem, and CaseQCIdentity classes.
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


import datetime

from .Utility import ASAPTransmitConfig
from CRL.DBCursor import CRLDBCursor


class CaseQC(object):
    """
    Object representation of casemaster record.
    """
    STATE_NEW = 'New'
    STATE_PENDING = 'Pending'
    STATE_RELEASED = 'Released'

    # noinspection PyUnresolvedReferences
    def __init__(self, sid='', trackingId='', state=STATE_NEW, createdDate=None, lastViewedBy='',
                 lastViewedDate=None, firstName='', lastName='', ssn='', policyNumber='',
                 sourceCode='', naic='', carrierDesc='', dateReceived=None):
        self.sid = sid
        self.trackingId = trackingId
        self.state = state
        self.createdDate = createdDate  # type: datetime.datetime
        self.lastViewedBy = lastViewedBy
        self.lastViewedDate = lastViewedDate  # type: datetime.datetime
        self.firstName = firstName
        self.lastName = lastName
        self.ssn = ssn
        self.policyNumber = policyNumber
        self.sourceCode = sourceCode
        self.naic = naic
        self.carrierDesc = carrierDesc
        self.dateReceived = dateReceived  # type: datetime.datetime
        self.history = []  # type: list[CaseQCHistoryItem]


class CaseQCHistoryItem(object):
    """
    Object representation of history record.
    """
    ACTION_CREATE = 'Create'
    ACTION_ADD = 'Add'
    ACTION_INSERT = 'Insert'
    ACTION_DELETE = 'Delete'
    ACTION_PEND = 'Pend'
    ACTION_UPDATE = 'Update'
    ACTION_RELEASED = 'Released'

    def __init__(self, comment='', action=ACTION_CREATE, documentId=0, documentTypeId=0,
                 documentType='', pageId=0, createdBy='', createdDate=None):
        self.comment = comment
        self.action = action
        self.documentId = documentId
        self.documentTypeId = documentTypeId
        self.documentType = documentType
        self.pageId = pageId
        self.createdBy = createdBy
        self.createdDate = createdDate  # type: datetime.datetime


class CaseQCIdentity(object):
    """
    Class to acquire (reserve) new id values for any CaseQC table in
    the CaseQC schema.
    """
    TBL_CASEMASTER = 'tblCaseMaster'
    TBL_CASEHISTORY = 'tblCaseHistory'

    def __init__(self):
        self._xmitConfig = None
        self.__cursor = None

    def getXmitConfig(self):
        """

        :rtype: ASAPTransmitConfig
        """
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

    def getNewIdValue(self, sTableName, idCount=1):
        """
        Reserve one or more ids for a table.  If more than one id reserved
        (idCount defaults to 1), the return value is the last id reserved.
        It is left up to the caller to determine which ids are available.
        Example:  pass 5 for idCount, and 25 is returned
                  this means ids 21-25 have been reserved
        Returns 0 if no ids reserved.
        """
        newId = 0
        sSqlGetIdentity = '''
                select idvalue from esubidentity with (updlock)
                where tablename = '{sTableName!s:s}'
                '''
        sSqlSetIdentity = '''
                update esubidentity set idvalue = {newId:d}
                where tablename = '{sTableName!s:s}'
                and idvalue = {oldId:d}
                '''
        if idCount > 0:
            # Get identity, add idCount to table
            cursor = self._getCursor()
            cursor.execute(sSqlGetIdentity.format(sTableName=sTableName))
            rec = cursor.fetch(True)
            if rec:
                oldId = rec[0]
                newId = oldId + idCount
                cursor.execute(sSqlSetIdentity.format(newId=newId,
                                                      sTableName=sTableName,
                                                      oldId=oldId))
                cursor.commit()
        return newId
