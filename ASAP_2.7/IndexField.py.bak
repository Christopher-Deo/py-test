"""

  Facility:         ILS

  Module Name:      IndexField

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPIndexField class.

  Author:
      Jarrod Wild

  Creation Date:
      24-Oct-2006

  Modification History:
      19-JUN-2012   rsu     Ticket # 33016
          ASAP for ING PHI and DBS.
          Allow index field configurations to pull from the ACORD 121 xml.

      02-JUN-2015   rsu     Ticket # 57129
          Modify setValue to attempt to normalize a unicode string to ascii using NFKD (normal form canonical decomposition).

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""
from __future__ import division, absolute_import, with_statement, print_function
import CRLUtility
import unicodedata


class ASAPIndexField(object):
    """
    Wrapper for an index field.
    """
    # source names
    SRC_LIMS = 'lims'
    SRC_ACORD103 = 'acord103'
    SRC_ACORD121 = 'acord121'
    SRC_DELTA_QC = 'deltaqc'
    SRC_CASE_QC = 'caseqc'
    SRC_CONSTANT = 'constant'
    SRC_DERIVED = 'derived'
    # reference object names
    REF_ASAPCASE = 'asapcase'
    REF_ASAPDOCUMENT = 'asapdocument'
    REF_TRACKINGID = 'trackingID'

    # reference object fields
    REF_DATECREATED = 'datecreated'
    REF_PAGECOUNT = 'pagecount'
    REF_DOCTYPENAME = 'doctypename'
    REF_CLIENTDOCNAME = 'clientdocname'
    REF_DOCCOUNT = 'doccount'
    # field types
    TYPE_DATE = 'date'
    TYPE_STRING = 'string'
    TYPE_NUMBER = 'number'
    # special formats
    FMT_SSN = '999-99-9999'

    def __init__(self, logger=None):
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger
        # name/value pair
        self.__name = ''
        self.__value = ''
        # all the metadata about the field
        self.__type = self.TYPE_STRING
        self.__format = ''
        self.__required = False
        self.__maxlength = 0
        self.__source = self.SRC_DERIVED
        self.__reference = ''

    def reset(self):
        """
        If source is a constant, reset field value to original value, else reset to empty string
        """
        if self.__source == self.SRC_CONSTANT:
            self.__value = self.__reference
        else:
            self.__value = ''

    def __formatDate(self, sValue):
        dateValue = CRLUtility.ParseStrDate(sValue, True)
        return dateValue.strftime(str(self.__format))

    def __formatNumber(self, sValue):
        newVal = sValue
        if len(newVal) > 1:
            # remove common special characters like dashes, dollar signs, commas
            newVal = newVal[0] + newVal[1:].replace('-', '')
            newVal = newVal.replace('$', '')
            newVal = newVal.replace(',', '')
        if (self.__format == self.FMT_SSN):
            if len(newVal) == 9:
                return '{s1!s:s}-{s2!s:s}-{s3!s:s}'.format(s1=newVal[:3], s2=newVal[3:5], s3=newVal[5:])
            else:
                raise Exception('Value {sValue!s:s} cannot be converted to SSN format.'
                                .format(sValue=sValue))
        else:
            numFormat = '{{floatVal:{numFmt!s:s}f}}'.format(numFmt=self.__format)
            try:
                floatVal = float(newVal)
                return numFormat.format(floatVal=floatVal)
            except Exception:
                self.__logger.info(
                    'Value {newVal!s:s} cannot be converted to float, returning empty string.'
                    .format(newVal=newVal))
                return ''

    def __formatValue(self, sValue):
        newVal = sValue
        if self.__format:
            if self.__type == self.TYPE_DATE:
                newVal = self.__formatDate(sValue)
            elif self.__type == self.TYPE_NUMBER:
                newVal = self.__formatNumber(sValue)
        return newVal.strip()

    def setFieldMeta(self, sName, sType, fRequired,
                     iMaxLength, sFormat, sSource, sRef):
        """
        Store metadata in field object.
        """
        self.__name = sName
        self.__type = sType
        if fRequired in ('Y', True):
            self.__required = True
        else:
            self.__required = False
        self.__maxlength = int(iMaxLength)
        self.__format = sFormat
        # use the carat in the tables as a placeholder for the percent
        if sType == self.TYPE_DATE and sFormat:
            self.__format = sFormat.replace('^', '%')
        self.__source = sSource
        self.__reference = sRef
        self.reset()

    def getName(self):
        return self.__name

    def getSource(self):
        return self.__source

    def getReference(self):
        return self.__reference

    def isRequired(self):
        return self.__required

    def getValue(self):
        return self.__value

    def setValue(self, sValue):
        """
        Returns True if successful.  If False is returned, a log message
        is written that explains the reason.

        :param str|unicode sValue:
        """
        fSuccess = False
        if isinstance(sValue, (str, unicode)):
            sValue = sValue.strip()
            if sValue:
                try:
                    sValue = str(sValue)  # string case fails on some unicode values
                except:
                    # attempt to normalize unicode characters to ascii using NFKD (normal form canonical decomposition)
                    sValue = unicodedata.normalize('NFKD', sValue).encode('ascii', 'ignore')
                    sValue = str(sValue)
                try:
                    newVal = self.__formatValue(sValue)
                    if (self.__maxlength == 0 or len(newVal) <= self.__maxlength):
                        self.__value = newVal
                    else:
                        # truncate for now (maybe make truncation an option later)
                        self.__value = newVal[:self.__maxlength]

                    fSuccess = True
                except Exception as exc:
                    self.__logger.warn("Unable to set value '{sValue!s:s}' for field {name!s:s}:"
                                       .format(sValue=sValue, name=self.__name))
                    self.__logger.warn(exc)
            else:
                self.__value = ''
                fSuccess = True
        else:
            self.__logger.warn(
                'Non-string value passed to field {name!s:s}.'.format(name=self.__name))
        return fSuccess

    def dbgPrint(self):
        print('{vals!s:s}'
              .format(vals=str((self.__name, self.__type, self.__required, self.__maxlength,
                                self.__format, self.__source, self.__reference, self.__value))))
