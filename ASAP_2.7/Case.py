"""

  Facility:         ILS

  Module Name:      Case

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPCase class.

  Author:
      Jarrod Wild

  Creation Date:
      31-Oct-2006

  Modification History:
      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7

"""

import os
import CRLUtility
from .Contact import ASAPContact
from .Document import ASAPDocument


class ASAPCase(object):
    """
    This is not necessarily a representation of a full case, but of
    what is to be transmitted at a given instance.  It is basically
    a wrapper around some key fields that are helpful in building
    index information, along with a collection of imaged document
    information (ASAPDocument objects).
    """
    def __init__(self):
        # LIMS sample key (also stored in ACORD and QC casemaster)
        self.sid = ''
        # ACORD order key, trackingid + source_code (also stored in QC casemaster)
        self.trackingId = ''
        self.source_code = ''
        # ASAPContact for case
        # noinspection PyTypeChecker
        self.contact = None  # type: ASAPContact
        # dictionary of ASAPDocument objects using documentid as key
        self.__documents = {}

    def addDocument(self, document):
        """
        Returns True if document added.  If False is returned, check
        document's fBill and fSend flags to see if they have been turned
        off, in which case the document is to be ignored.  If they are
        not turned off, it means the document didn't have a billing code
        for its type.

        :param ASAPDocument document:
        :returns: a boolean indicating success
        :rtype: bool
        """
        fSuccess = False
        document.fBill = True
        document.fSend = True
        if self.contact:
            billingCode = self.contact.docTypeBillingMap.get(document.getDocTypeName())
            if billingCode == self.contact.no_bill_no_send_code:
                document.fBill = False
                document.fSend = False
            elif billingCode:
                self.__documents[document.getDocumentId()] = document
                document.case = self
                if billingCode == self.contact.no_bill_code:
                    document.fBill = False
                fSuccess = True
        return fSuccess

    def getDocument(self, docid):
        """ Get a document object for a specified documentid associated with the case

        :param str|int docid: The DocumentId of the document to retrieve
        :return: the document with the specified DocumentId or None
        :rtype: ASAPDocument|None
        """
        return self.__documents.get(int(docid))

    def getDocuments(self):
        """ Get all documents associated with the case

        :return:
        :rtype: dict[int, ASAPDocument]
        """
        return self.__documents

    def moveToError(self, errorSubfolder='error'):
        """ Move all case files to related error subfolders.

        :param str errorSubfolder:
        :returns: True
        :rtype: bool
        """
        idxFileNames = []
        if self.contact.index.type == self.contact.index.IDX_TYPE_CASE:
            idxFileNames.append('{trackingId!s:s}.IDX'.format(trackingId=self.trackingId))
        else:
            for doc in list(self.__documents.values()):
                idxFileNames.append('{filename!s:s}.IDX'.format(filename=doc.fileName.split('.')[0]))
        for idxFileName in idxFileNames:
            idxPath = os.path.join(self.contact.index_dir, idxFileName)
            if os.path.isfile(idxPath):
                errorPath = os.path.join(self.contact.index_dir, errorSubfolder)
                if not os.path.exists(errorPath):
                    os.mkdir(errorPath)
                CRLUtility.CRLCopyFile(idxPath, os.path.join(errorPath, idxFileName), True, 5)
        for doc in list(self.__documents.values()):
            docPath = os.path.join(self.contact.document_dir, doc.fileName)
            if os.path.isfile(docPath):
                errorPath = os.path.join(self.contact.document_dir, errorSubfolder)
                if not os.path.exists(errorPath):
                    os.mkdir(errorPath)
                CRLUtility.CRLCopyFile(docPath, os.path.join(errorPath, doc.fileName), True, 5)
        return True

    def dbgPrint(self):
        """ Prints the current case instance's data

        :return: None
        """
        print('\nCase:\n{casedata!s:s}'.format(casedata=str((self.sid, self.trackingId))))
        print('Contact:')
        self.contact.dbgPrint()
        print('Documents ({doc_count:d}):'.format(doc_count=len(self.__documents)))
        for document in list(self.__documents.values()):
            document.dbgPrint()
