"""
  Facility:         ILS

  Module Name:      QCDocument

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the QCDocument class.
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
from CRL.DeltaModel import DeltaDocument


class QCDocument(DeltaDocument):
    """
    Object representation of ILS_QC document record (tbldocuments, tblfolders, tblindexes).
    """

    # noinspection PyUnresolvedReferences
    def __init__(self):
        DeltaDocument.__init__(self)
        self.transmitHistory = []   # type: list[tuple[str, str]]
