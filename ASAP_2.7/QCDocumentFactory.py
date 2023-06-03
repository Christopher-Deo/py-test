"""
  Facility:         ILS

  Module Name:      QCDocumentFactory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the QCDocumentFactory class.
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

from .QCDocument import QCDocument
from CRL.DeltaModel import DeltaDocument, DeltaDocumentFactory


class QCDocumentFactory(DeltaDocumentFactory):
    """
    Retrieve document records by sid or documentid.
    """

    def __init__(self):
        DeltaDocumentFactory.__init__(self, DeltaDocument.DB_ILS_QC)

    def _getDocuments(self, whereClause):
        documents = DeltaDocumentFactory._getDocuments(self, whereClause)
        qcDocuments = []
        for doc in documents:
            qcDoc = QCDocument()
            qcDoc.dateCreated = doc.dateCreated
            qcDoc.deltaDb = doc.deltaDb
            qcDoc.documentId = doc.documentId
            qcDoc.documentType = doc.documentType
            qcDoc.indexes = doc.indexes
            qcDoc.pages = doc.pages
            qcDocuments.append(qcDoc)
        return qcDocuments

    def getUnmatchedDocuments(self):
        """
        This is currently exclusive to the QC drawer, but could be allowed for others
        if implementing the same behavior.
        """
        sidField = ''
        matchedField = ''
        exportedField = ''
        for fieldid in list(self._indexMap.keys()):
            if self._indexMap[fieldid] == DeltaDocument.IDX_MATCHED:
                matchedField = fieldid
            elif self._indexMap[fieldid] == DeltaDocument.IDX_SID:
                sidField = fieldid
            elif self._indexMap[fieldid] == DeltaDocument.IDX_EXPORTED:
                exportedField = fieldid
        whereClause = '''
            ({matchedField!s:s} is null or {matchedField!s:s} <> 'Y') and not exists
            (select f2.folderid from tblfolders f2 with (nolock)
            where f2.{splitSidField!s:s} = {sidField!s:s} and {exportedField!s:s} <> 'N') and not exists
            (select sampleid from casemaster with (nolock)
            where sampleid = {sidField!s:s})
            '''.format(matchedField=matchedField,
                       splitSidField=sidField.split('.')[1],
                       sidField=sidField,
                       exportedField=exportedField)
        docs = self._getDocuments(whereClause)
        return docs
