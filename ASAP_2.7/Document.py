"""

  Facility:         ILS

  Module Name:      Document

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPDocument class.

  Author:
      Jarrod Wild

  Creation Date:
      31-Oct-2006

  Modification History:
      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import datetime


class ASAPDocument(object):
    """
    This is a representation of an imaged document's information.
    This will typically be in the 'documents' dictionary for the
    related ASAPCase.
    """
    def __init__(self):
        # documentId as stored in tbldocuments in the ILS_QC database
        self.__documentId = 0
        # document type name used to map to client name in LIMS table
        # document_service_map
        self.__docTypeName = ''
        # number of pages
        self.pageCount = 0
        # file name (the name given to the image for the first page)
        self.fileName = ''
        # date the file was indexed
        self.__dateCreated = '01/01/1900'
        # flag for billing (defaults to True)
        self.fBill = True
        # flag for sending (defaults to True)
        self.fSend = True
        # case this document is a part of
        self.case = None

    def setDocumentId(self, docid):
        """

        :param int|str|unicode docid: the documentid
        """
        self.__documentId = int(docid)

    def getDocumentId(self):
        return self.__documentId

    def setDocTypeName(self, docTypeName):
        """

        :param str|unicode docTypeName: the doc type
        """
        self.__docTypeName = docTypeName.strip().upper()

    def getDocTypeName(self):
        return self.__docTypeName

    def setDateCreated(self, dateVal):
        """

        :param datetime.datetime dateVal: a datetime object representing the document's create date
        """
        fSuccess = False
        try:
            self.__dateCreated = dateVal.strftime('%m/%d/%Y')
            fSuccess = True
        except:
            pass
        return fSuccess

    def getDateCreated(self):
        return self.__dateCreated

    def dbgPrint(self):
        print('{documentdata!s:s}'.format(documentdata=str((self.__documentId,
                                                            self.__docTypeName,
                                                            self.pageCount,
                                                            self.fileName,
                                                            self.__dateCreated,
                                                            self.case))))
