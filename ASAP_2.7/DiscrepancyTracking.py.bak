"""

  Facility:         ILS

  Module Name:      DiscrepancyTracking

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains classes for tracking and reviewing ASAP
      discrepancies.

  Author:
      Jarrod Wild

  Creation Date:
      26-Nov-2007

  Modification History:
      13-Aug-2015   amw Ticket 61720
        Updated for usage on new and/or old apphub

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""
from __future__ import division, absolute_import, with_statement, print_function
from .Utility import ASAP_UTILITY
import os


class ASAPDiscrepancy(object):
    """
    Object representation of row from asap_discrepancies table and
    asap_discrepancy_types table.
    """
    def __init__(self, discrepancyId=0, sid='', trackingId='', discrepancyTypeId=0,
                 discrepancyTypeDesc='', discrepancyDate=None, resolvedDate=None,
                 resolvedBy='', comment='', closedDate=None):
        self.discrepancyId = discrepancyId
        self.sid = sid
        self.trackingId = trackingId
        self.discrepancyTypeId = discrepancyTypeId
        self.discrepancyTypeDesc = discrepancyTypeDesc
        self.discrepancyDate = discrepancyDate  # this is *never* null in table
        self.resolvedDate = resolvedDate
        self.resolvedBy = resolvedBy
        self.comment = comment
        self.closedDate = closedDate

    def copy(self, disc):
        """

        :param ASAPDiscrepancy disc:
        """
        self.discrepancyId       = disc.discrepancyId
        self.sid                 = disc.sid
        self.trackingId          = disc.trackingId
        self.discrepancyTypeId   = disc.discrepancyTypeId
        self.discrepancyTypeDesc = disc.discrepancyTypeDesc
        self.discrepancyDate     = disc.discrepancyDate
        self.resolvedDate        = disc.resolvedDate
        self.resolvedBy          = disc.resolvedBy
        self.comment             = disc.comment
        self.closedDate          = disc.closedDate


class ASAPDiscrepancyTypes(object):
    """
    Constants for discrepancy type ids that should mirror the
    SQL table asap_discrepancy_types.
    """
    ORDER_NO_SAMPLE = 1
    ORDER_SAMPLE_NO_DOCS = 2
    ORDER_NO_DOCS = 3


class ASAPDiscrepancyHandler(object):
    """
    Handles the 'dirty' work of pulling discrepancies, checking for closed
    or resolved discrepancies, and closing/resolving discrepancies.
    """
    SQL_SELECT_CLAUSE = '''
        select d.discrepancyid, d.sid, d.trackingid, d.discrepancytypeid,
        dt.discrepancytype, d.discrepancydate, d.resolveddate, d.resolvedby,
        d.comment, d.closeddate from asap_discrepancies d with (nolock)
        inner join asap_discrepancy_types dt with (nolock) on
        d.discrepancytypeid = dt.discrepancytypeid
        '''

    def __init__(self):
        self.__cursor = ASAP_UTILITY.xmitConfig.getCursor(ASAP_UTILITY.xmitConfig.DB_NAME_XMIT)

    @staticmethod
    def __mapDiscrepancy(rec):
        """
        Map record returned from SQL to ASAPDiscrepancy and return the new object.

        :param list rec:
        :rtype: ASAPDiscrepancy
        """
        (discrepancyId, sid, trackingId, discrepancyTypeId,
         discrepancyTypeDesc, discrepancyDate, resolvedDate,
         resolvedBy, comment, closedDate) = rec

        if not sid:
            sid = ''
        if not trackingId:
            trackingId = ''
        if not resolvedBy:
            resolvedBy = ''
        if not comment:
            comment = ''
        disc = ASAPDiscrepancy(discrepancyId, sid, trackingId, discrepancyTypeId,
                               discrepancyTypeDesc, discrepancyDate, resolvedDate,
                               resolvedBy, comment, closedDate)
        return disc

    def __refreshDiscrepancy(self, disc):
        """
        Reload discrepancy data from database.

        :param ASAPDiscrepancy disc:
        """
        self.__cursor.execute('''
                {selectClause!s:s} where d.discrepancyid = {discrepancyId:d}
                '''.format(selectClause=self.SQL_SELECT_CLAUSE,
                           discrepancyId=disc.discrepancyId))
        rec = self.__cursor.fetch(True)
        self.__cursor.rollback()
        if rec:
            updatedDisc = self.__mapDiscrepancy(rec)
            disc.copy(updatedDisc)

    def getDiscrepancies(self, sid, trackingId, discrepancyTypeId):
        """ Given a sid and/or trackingId and a discrepancy type, check for
        any existing discrepancies and return them as a list.

        :param discrepancyTypeId:
        :param trackingId:
        :param sid:
        :rtype: list[ASAPDiscrepancy]
        """
        if not discrepancyTypeId or not (sid or trackingId):
            return []
        whereClause = "d.discrepancytypeid = {discrepancyTypeId:d}".format(discrepancyTypeId=discrepancyTypeId)
        if sid:
            whereClause += " and d.sid = '{sid!s:s}'".format(sid=sid)
        if trackingId:
            whereClause += " and d.trackingid = '{trackingId!s:s}'".format(trackingId=trackingId)
        self.__cursor.execute('''
            {selectClause!s:s} where {whereClause!s:s}
            '''.format(selectClause=self.SQL_SELECT_CLAUSE, whereClause=whereClause))
        recs = self.__cursor.fetch()
        self.__cursor.rollback()
        if recs:
            return [self.__mapDiscrepancy(rec) for rec in recs]
        return []

    def getOpenDiscrepancies(self, discrepancyTypeId=0):
        """
        Get all discrepancies that are not either auto-closed
        or manually resolved.  Filter by discrepancy type if desired.
        :param int discrepancyTypeId:
        """
        subClause = ''
        if discrepancyTypeId > 0:
            subClause = "and d.discrepancytypeid = {discrepancyTypeId:d}".format(discrepancyTypeId=discrepancyTypeId)
        self.__cursor.execute('''
                {selectClause!s:s} where d.resolveddate is null
                and d.closeddate is null
                {subClause!s:s}
                '''.format(selectClause=self.SQL_SELECT_CLAUSE, subClause=subClause))
        recs = self.__cursor.fetch()
        self.__cursor.rollback()
        if recs:
            return [self.__mapDiscrepancy(rec) for rec in recs]
        return []

    def addNewDiscrepancy(self, newDisc):
        """
        Given an ASAPDiscrepancy object, insert into asap_discrepancy
        table, if the discrepancy doesn't already exist.  If the discrepancy
        *does* exist and is closed (but not manually resolved), a new
        discrepancy will be added.

        :param ASAPDiscrepancy newDisc:
        """
        fSuccess = False
        if newDisc.discrepancyTypeId and (newDisc.sid or newDisc.trackingId):
            discs = self.getDiscrepancies(newDisc.sid,
                                          newDisc.trackingId,
                                          newDisc.discrepancyTypeId)
            fOpenNew = True
            for disc in discs:
                if not disc.closedDate or disc.resolvedDate:
                    fOpenNew = False
            if not discs or fOpenNew:
                self.__cursor.execute('''
                        insert into asap_discrepancies
                        (sid, trackingid, discrepancytypeid, discrepancydate)
                        values ('{sid!s:s}', '{trackingId!s:s}', {discrepancyTypeId:d}, current_timestamp)
                        '''.format(sid=newDisc.sid,
                                   trackingId=newDisc.trackingId,
                                   discrepancyTypeId=newDisc.discrepancyTypeId))
                self.__cursor.commit()
                fSuccess = True
        return fSuccess

    def closeDiscrepancy(self, disc):
        """
        DO NOT USE unless the ASAPDiscrepancy has been resolved
        without manual intervention, i.e. a sample was missing an order
        and the order was received and matched and therefore the discrepancy
        should be closed.

        :param ASAPDiscrepancy disc:
        """
        self.__cursor.execute('''
                update asap_discrepancies set closeddate = current_timestamp
                where discrepancyid = {discrepancyId:d} and closeddate is null
                '''.format(discrepancyId=disc.discrepancyId))
        self.__cursor.commit()
        self.__refreshDiscrepancy(disc)
        return True

    def resolveDiscrepancy(self, disc, sComment):
        """ For manual resolution of an ASAPDiscrepancy, pass a brief comment
        that describes the reason for manual resolution.

        :param str sComment:
        :param ASAPDiscrepancy disc:
        """
        fSuccess = False
        if sComment:
            self.__cursor.execute('''
                update asap_discrepancies set resolveddate = current_timestamp,
                resolvedby = '{user!s:s}', comment = '{comment!s:s}'
                where discrepancyid = {discrepancyId:d} and closeddate is null
                '''.format(user=os.getenv('USERNAME'),
                           comment=sComment,
                           discrepancyId=disc.discrepancyId))
            self.__cursor.commit()
            self.__refreshDiscrepancy(disc)
            if not disc.closedDate:
                fSuccess = True
        return fSuccess
