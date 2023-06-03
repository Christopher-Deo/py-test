"""

  Facility:         ILS

  Module Name:      Index

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPIndex class.

  Author:
      Jarrod Wild

  Creation Date:
      24-Oct-2006

  Modification History:
      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import os
import CRLUtility
from .IndexField import ASAPIndexField


class ASAPIndex(object):
    """
    Wrapper for an index with support for file read/write.  The index
    includes mapping of field name to ASAPIndexField object and mapping
    of ordinal position (field order) to field name.
    Typically one instance would be created and stored under the related
    ASAPContact for use in building index files.
    """
    # index types used in the configuration tables
    IDX_TYPE_CASE = 'case'
    IDX_TYPE_DOCUMENT = 'document'
    # maps special characters from identifiers used in table entries
    __ESCAPE_MAP = {
        '<LF>': '\n',
        '<CR>': '\r',
        '<T>': '\t',
        '<SP>': ' '
    }

    # noinspection PyUnresolvedReferences
    def __init__(self, logger=None):
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger
        self.type = self.IDX_TYPE_CASE

        self.__fieldMap = {}  # type: dict[str, ASAPIndexField]
        self.__orderMap = {}  # type: dict[int, str]
        # delim is delimiter between field/value pairs
        # subdelim is delimiter between field and value
        self.__delim = '\n'
        self.__subdelim = '='

    def __escapeMap(self, strVal):
        """ Convert all identifiers to their special character counterparts
        <LF> -> "\n"

        :param str strVal:
        """
        if strVal:
            for escSeq in list(self.__ESCAPE_MAP.keys()):
                strVal = strVal.replace(escSeq, self.__ESCAPE_MAP[escSeq])
        return strVal

    def reset(self):
        """
        Reset all fields
        """
        for field in list(self.__fieldMap.values()):
            field.reset()

    def setDelim(self, delim):
        self.__delim = self.__escapeMap(delim)

    def setSubdelim(self, subdelim):
        self.__subdelim = self.__escapeMap(subdelim)

    def addField(self, field, iOrder):
        """

        :param ASAPIndexField field:
        :param int iOrder:
        """
        fSuccess = False
        if isinstance(field, ASAPIndexField):
            fieldName = field.getName()
            self.__fieldMap[fieldName] = field
            self.__orderMap[iOrder] = fieldName
            fSuccess = True
        else:
            self.__logger.warn('Field is not an instance of ASAPIndexField.')
        return fSuccess

    def getFieldMap(self):
        return self.__fieldMap

    def getOrderedFieldNames(self):
        """
        Return list of field names ordered by sequence in index.
        """
        fieldList = []
        orderVals = list(self.__orderMap.keys())
        orderVals.sort()
        for order in orderVals:
            fieldList.append(self.__orderMap[order])
        return fieldList

    def getField(self, sField):
        """ Get the ASAPIndexField instance with the specified field name

        :param str|unicode sField:
        :return: the ASAPIndexField specified
        :rtype: None|ASAPIndexField
        """
        return self.__fieldMap.get(sField)

    def getValue(self, sField):
        """ Get value for a specific index field

        :param str sField:
        :return: the specified field's value
        :rtype: str
        """
        field = self.__fieldMap.get(sField)
        if field:
            return field.getValue()
        else:
            return ''

    def setValue(self, sField, sValue):
        """ Set the value on a specified ASAPIndexField. If not successful, a log
        message is written indicating the reason.

        :param str sField: The index field for which to set the value
        :param str sValue: The value to set
        :returns: boolean value indicating success
        :rtype: bool
        """
        fSuccess = False
        field = self.__fieldMap.get(sField)
        if field:
            fSuccess = field.setValue(sValue)
        else:
            self.__logger.warn("Field {sField!s:s} does not exist.".format(sField=sField))
        return fSuccess

    def readFile(self, fileName):
        """ Read in index file and store values in index.

        :param str fileName: path to file from which to read in values
        """
        fSuccess = False
        if os.path.isfile(fileName):
            try:
                idxFile = open(fileName, 'r')
                rawData = idxFile.read()
                idxFile.close()
                pairs = rawData.strip().split(self.__delim)
                for pair in pairs:
                    field, value = pair.split(self.__subdelim)
                    if not self.setValue(field, value):
                        raise Exception("Failed to set field {field!s:s} to value '{value!s:s}'.".format(field=field, value=value))
                fSuccess = True
            except Exception as exc:
                self.__logger.warn('Unable to process index file {fileName!s:s}:'.format(fileName=fileName))
                self.__logger.warn(exc)
        else:
            self.__logger.warn('File {fileName!s:s} does not exist.'.format(fileName=fileName))
        return fSuccess

    def writeFile(self, fileName):
        """ Write index out to file specified by fileName.

        :param str|unicode fileName: path to file which will be created
        """
        fSuccess = False
        try:
            fieldNames = self.getOrderedFieldNames()
            # print('{fieldNames!s:s}'.format(fieldNames=str(fieldNames)))
            pairs = []
            for fieldName in fieldNames:
                field = self.getField(fieldName)
                value = field.getValue()
                if not value and field.isRequired():
                    raise Exception('Required field {fieldName!s:s} missing value.'.format(fieldName=fieldName))
                # print('{fieldName!s:s}, {subdelim!s:s}, {value!s:s}'
                #       .format(fieldName=fieldName, subdelim=self.__subdelim, value=value))
                pair = fieldName + self.__subdelim + value
                pairs.append(pair)
            rawData = self.__delim.join(pairs) + '\n'
            idxFile = open(fileName, 'w')
            idxFile.write(rawData)
            idxFile.close()
            fSuccess = True
        except Exception:
            self.__logger.warn('Unable to write index file {fileName!s:s}:'.format(fileName=fileName), exc_info=True)
        return fSuccess

    def dbgPrint(self):
        print('{vals!s:s}'
              .format(vals=str((self.type,
                                self.__delim,
                                self.__subdelim))))
        print('Index fields:')
        fieldlist = self.getOrderedFieldNames()
        for field in fieldlist:
            self.__fieldMap[field].dbgPrint()
