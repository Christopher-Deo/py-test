"""

  Facility:         ILS

  Module Name:      Contact

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPContact class.

  Author:
      Jarrod Wild

  Creation Date:
      24-Oct-2006

  Modification History:
      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""
from __future__ import division, absolute_import, with_statement, print_function
from .Index import ASAPIndex


class ASAPContact(object):
    """
    Wrapper for all information about a contact (including the index
    object).
    """

    # noinspection PyUnresolvedReferences
    def __init__(self, logger=None):
        self.contact_id = ''
        self.client_id = ''
        self.region_id = ''
        self.examiner = ''
        self.source_code = ''
        # staging paths for processing files for ASAP
        self.document_dir = ''
        self.index_dir = ''
        self.acord103_dir = ''
        self.xmit_dir = ''
        # billing code for no-bill no-send
        self.no_bill_no_send_code = ''
        # billing code for no-bill send
        self.no_bill_code = ''
        # these two maps hold data from LIMS document_service_map table:
        # maps doctypename from Delta to client document name
        self.docTypeNameMap = {}  # type: dict[str, str]
        # maps doctypename from Delta to billing code
        self.docTypeBillingMap = {}  # type: dict[str, str]
        # maps base class name to a tuple of (custom module, custom class)
        self.customClasses = {}  # type: dict[str, tuple[str, str]]
        self.index = ASAPIndex(logger)
        # list of carrier full names pulled from ACORD tables
        self.acordCarrierNames = []

    def dbgPrint(self):
        """ Prints the current Contact instance's data

        :return: None
        """
        print('{contactdata!s:s}'
              .format(contactdata=str((self.contact_id,
                                       self.client_id,
                                       self.region_id,
                                       self.examiner,
                                       self.source_code))))
        docRowFmt = '{docType!s:s}: {docTypeName!s:s} ({docTypeBilling!s:s})'
        for docType in self.docTypeNameMap.keys():
            print(docRowFmt.format(docType=docType,
                                   docTypeName=self.docTypeNameMap[docType],
                                   docTypeBilling=self.docTypeBillingMap[docType]))
        classRowFmt = '{baseClass!s:s}: {customClass!s:s}'
        for baseClass in self.customClasses.keys():
            print(classRowFmt.format(baseClass=baseClass, customClass=str(self.customClasses[baseClass])))
        print('{acordCarrierNames!s:s}'.format(acordCarrierNames=str(self.acordCarrierNames)))
