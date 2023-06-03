"""

  Facility:         ILS

  Module Name:      DocumentFactory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPDocumentFactory class.

  Author:
      Jarrod Wild

  Creation Date:
      02-Nov-2006

  Modification History:
      19-JUN-2012   rsu
        Add fromtimestamp check to allow new apphub runs in the future

      26-Oct-2015   amw
        Fixed pagefilename size to 8.3

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
from .Document import ASAPDocument
from .TransmitConfig import ASAPTransmitConfig


class ASAPDocumentFactory(object):
    """
    This is a factory class for building ASAPDocument objects from
    key values.
    """
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

    def fromDocumentId(self, docId):
        """
        Given a docid, build an ASAPDocument object.

        :param int|str|unicode docId: the documentid
        :rtype: ASAPDocument|None
        """
        document = None
        config = self.getXmitConfig()
        cursor = config.getCursor(config.DB_NAME_DELTA_QC)
        if cursor:
            tempDoc = ASAPDocument()
            try:
                tempDoc.setDocumentId(docId)
            except:
                self.__logger.warn(
                    'Docid {docid:d} cannot be converted to int.'.format(docid=docId))
                return None
            sQuery = '''
                select p.pagefilename, d.documentdatecreated, dt.documenttypename
                from tblpages p with (nolock)
                inner join tbldocuments d with (nolock) on p.documentid = d.documentid
                inner join tbldocumenttypes dt with (nolock) on d.documenttypeid = dt.documenttypeid
                where p.documentid = {docid:d}
                and p.pagesequence = (select min(p2.pagesequence)
                                      from tblpages p2 with (nolock)
                                      where p2.documentid = p.documentid)
                '''.format(docid=tempDoc.getDocumentId())
            cursor.execute(sQuery)
            docRec = cursor.fetch(True)
            cursor.rollback()
            if docRec:
                filename, datecreated, typename = docRec
                tempDoc.fileName = filename.lstrip('0')
                tempDoc.setDocTypeName(typename)
                if datecreated:
                    tempDoc.setDateCreated(datecreated)
                sCountQuery = '''
                    select count(*) from tblpages with (nolock)
                    where documentid = {docid:d}
                    '''.format(docid=tempDoc.getDocumentId())
                cursor.execute(sCountQuery)
                rec = cursor.fetch(True)
                cursor.rollback()
                if rec:
                    tempDoc.pageCount, = rec
                document = tempDoc
            else:
                self.__logger.warn(
                    'Docid {docid:d} does not exist in ILS_QC drawer.'.format(docid=tempDoc.getDocumentId()))
        else:
            self.__logger.warn('No cursor for ILS_QC available.')
        return document

    def fromFileName(self, fileName):
        """
        Given an image document file name (<pageid>.TIF where <pageid> is left
        padded with zeros to make the file 8.3 format), build an ASAPDocument
        object.

        :rtype: ASAPDocument|None
        """
        document = None
        config = self.getXmitConfig()
        cursor = config.getCursor(config.DB_NAME_DELTA_QC)
        if cursor:
            tempDoc = ASAPDocument()
            tempDoc.fileName = fileName
            try:
                pageId = int(fileName.split('.')[0])
            except:
                self.__logger.warn(
                    'File name %s is not in proper format.' % fileName)
                return None
            sQuery = '''
                select d.documentid, d.documentdatecreated, dt.documenttypename
                from tblpages p with (nolock)
                inner join tbldocuments d with (nolock)
                on p.documentid = d.documentid
                inner join tbldocumenttypes dt with (nolock)
                on d.documenttypeid = dt.documenttypeid
                where p.pageid = {pageId:d} and p.pagesequence =
                (select min(p2.pagesequence) from tblpages p2 with (nolock)
                where p2.documentid = p.documentid)
                '''.format(pageId=pageId)
            cursor.execute(sQuery)
            docRec = cursor.fetch(True)
            cursor.rollback()
            if docRec:
                docid, datecreated, typename = docRec
                tempDoc.setDocumentId(docid)
                tempDoc.setDocTypeName(typename)
                if datecreated:
                    tempDoc.setDateCreated(datecreated)
                sCountQuery = '''
                    select count(*) from tblpages with (nolock)
                    where documentid = {docid:d}
                    '''.format(docid=tempDoc.getDocumentId())
                cursor.execute(sCountQuery)
                rec = cursor.fetch(True)
                cursor.rollback()
                if rec:
                    tempDoc.pageCount, = rec
                document = tempDoc
            else:
                self.__logger.warn(
                    'Document for file {fileName!s:s} does not exist in ILS_QC drawer.'.format(fileName=fileName))
        else:
            self.__logger.warn('No cursor for ILS_QC available.')
        return document

    def documentsFromSid(self, sid):
        """
        Given a sid, return a list of ASAPDocument objects for that sid.

        :rtype: list[ASAPDocument]
        """
        documents = []
        config = self.getXmitConfig()
        cursor = config.getCursor(config.DB_NAME_DELTA_QC)
        if cursor:
            sQuery = '''
                select d.documentid
                from tbldocuments d with (nolock)
                inner join tblfolders f with (nolock)
                on d.folderid = f.folderid
                where f.{sidField!s:s} = '{sid!s:s}'
                '''.format(sidField=config.getSetting(config.SETTING_DELTA_SID_FIELD), sid=sid)
            cursor.execute(sQuery)
            docIds = cursor.fetch()
            cursor.rollback()
            if docIds:
                for docId, in docIds:
                    document = self.fromDocumentId(docId)
                    if document:
                        documents.append(document)
            else:
                self.__logger.warn('No documents found in Delta for sid {sid!s:s}.'.format(sid=sid))
        else:
            self.__logger.warn('No cursor for Delta available.')
        return documents

    def documentsFromTrackingId(self, trackingId):
        """ Given a tracking ID, return a list of ASAPDocuments for that tracking ID.

        :rtype: list[ASAPDocument]
        """
        documents = []
        config = self.getXmitConfig()
        cursor = config.getCursor(config.DB_NAME_CASE_QC)
        if cursor:
            sQuery = '''
                select sampleid from casemaster with (nolock)
                where trackingid = '{trackingId!s:s}'
                '''.format(trackingId=trackingId)
            cursor.execute(sQuery)
            rec = cursor.fetch(True)
            cursor.rollback()
            if rec:
                sid, = rec
                documents = self.documentsFromSid(sid)
            else:
                self.__logger.warn(
                    'No case found in case QC tables for tracking ID {trackingId!s:s}.'
                    .format(trackingId=trackingId))
        else:
            self.__logger.warn('No cursor for case QC tables available.')
        return documents
