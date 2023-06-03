"""

  Facility:         ILS

  Module Name:      IndexHandler

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPIndexHandler class.

  Author:
      Jarrod Wild

  Creation Date:
      03-Nov-2006

  Modification History:
      19-JUN-2012   rsu     Ticket # 33016
          ASAP for ING PHI and DBS.
          Allow index field configurations to pull from the ACORD 121 xml.

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
import datetime
import os
from .Utility import ASAP_UTILITY
from .Case import ASAPCase
from .. import AcordXML


class ASAPIndexHandler(object):
    """
    This is a helper class for building index files for individual
    cases.  The files are generated and placed in the index folder
    as defined by the case's contact.  If needed, this class can
    be inherited to override methods that do nothing in this base
    class, for custom processing.
    """

    def __init__(self):
        # print 'in base init'
        self.__logger = CRLUtility.CRLGetLogger()
        self.__case = ASAPCase()
        self.__currentDocument = None
        self.__acord103Fields = []
        self.__acord121Fields = []
        self.__deltaFields = []
        self.__limsFields = []
        self.__acordHandler = None
        self.__idxPaths = []

    def __reset(self):
        self.__currentDocument = None
        self.__acord103Fields = []
        self.__acord121Fields = []
        self.__deltaFields = []
        self.__limsFields = []
        self.__acordHandler = None
        self.__idxPaths = []

    def _isReadyToIndex(self):
        """
        Derived class should override this method if index shouldn't
        be created but error state shouldn't occur either (i.e. carrier
        wants lab report bundled with case and lab report isn't ready).
        """
        return True

    def _isFirstTransmit(self):
        """
        Check if current case is being transmitted for first time.
        """
        fFirst = True
        hist = ASAP_UTILITY.getDocumentHistory()
        if hist.getTrackedDocidsForCase(self.__case, hist.ACTION_TRANSMIT):
            fFirst = False
        return fFirst

    def _isFullTransmit(self):
        """
        Check if entire current case is being transmitted.
        """
        fFull = True
        hist = ASAP_UTILITY.getDocumentHistory()
        xmitRecs = hist.getTrackedDocidsForCase(self.__case, hist.ACTION_TRANSMIT)
        xmitDocids = [docid for docid, auditstamp in xmitRecs]
        docids = list(self.__case.getDocuments().keys())
        for docid in docids:
            if docid in xmitDocids:
                xmitDocids.remove(docid)
        if xmitDocids:
            fFull = False
        return fFull

    def _preProcessIndex(self):
        """
        Derived class should override this method to perform any
        preprocessing before indexes are being created.  Return
        True to allow process to continue, False to halt processing.
        """
        # print 'base class preprocess does nothing'
        return True

    def _processDerivedFields(self):
        """
        Derived class should override this method to perform any
        processing of derived fields (and other custom processing) for
        a particular index.  Return True to allow process to continue,
        False to halt processing.
        """
        # print 'base class process derived does nothing'
        return True

    def _postProcessIndex(self):
        """
        Derived class should override this method to perform any
        postprocessing after indexes have been written to files.
        Return True to allow process to continue, False to halt
        processing.
        """
        # print 'base class postprocess does nothing'
        return True

    def _getLogger(self):
        """
        This accessor is so a derived class can get the logger.
        """
        return self.__logger

    def _getCase(self):
        """
        This accessor is so a derived class can get the case.
        """
        return self.__case

    def _getCurrentDocument(self):
        """
        This accessor is so a derived class can get the current document
        (if building document indexes and not a case index).
        """
        return self.__currentDocument

    def _getAcordHandler(self):
        """
        This accessor is so a derived class can get the handler (if available).
        """
        return self.__acordHandler

    def _getIndexPaths(self):
        """
        This accessor is so a derived class can get a list of index files written
        for a case.  This would be useful during postprocessing.
        """
        return self.__idxPaths

    def __processAcord103Fields(self):
        """
        If there are 103 fields to process, and the case's contact
        has a 103 path configured, and the 103 file in question is
        present, then attempt to pull the necessary fields.  Return
        True if okay to continue, False if processing should be halted.
        """
        if self.__acord103Fields:
            contact = self.__case.contact
            if contact.acord103_dir:
                xmlPath = os.path.join(contact.acord103_dir,
                                       '{trackingid!s:s}.XML'.format(trackingid=self.__case.trackingId))
                parser = AcordXML.AcordXMLParser()
                handler = parser.parse(xmlPath)
                if parser.getException():
                    # exception email should be generated for this, so just return False
                    return False
                errorHandler = parser.getErrorHandler()
                if errorHandler.warnings:
                    self.__logger.info('Warnings parsing {xmlPath!s:s}:'.format(xmlPath=xmlPath))
                    for warning in errorHandler.warnings:
                        self.__logger.info(warning)
                if errorHandler.errors:
                    self.__logger.warn('Errors parsing {xmlPath!s:s}:'.format(xmlPath=xmlPath))
                    for error in errorHandler.errors:
                        self.__logger.warn(error)
                    return False
                if handler:
                    # process acord 103 fields now
                    txLifeElement = handler.txList[0]
                    fError = False
                    # try to process all fields, so all problems that occur can be logged
                    # before deciding to return False
                    for field in self.__acord103Fields:
                        elem = txLifeElement.getElement(field.getReference())
                        if elem:
                            if not field.setValue(elem.value):
                                fError = True
                        else:
                            self.__logger.warn('Field {name!s:s} not found in {xmlPath!s:s}.'
                                               .format(name=field.getReference(), xmlPath=xmlPath))
                            # don't error, let the required check take care of this later
                            # fError = True
                    if fError:
                        return False
                    # there were no problems, so assign the handler for access by
                    # a derived class, if needed
                    self.__acordHandler = handler
                else:
                    self.__logger.warn('No handler was returned from parsing {xmlPath!s:s}.'.format(xmlPath=xmlPath))
                    return False
            else:
                self.__logger.warn(
                    'ACORD 103 fields present in index, but contact ' +
                    '{contact_id!s:s} not configured to process ACORD 103 files.'
                    .format(contact_id=contact.contact_id))
                return False
        return True

    def __processAcord121Fields(self):
        """
        If there are 121 fields to process, retrieve the original 121 from the blob,
        then attempt to pull the necessary fields.
        Return True if okay to continue, False if processing should be halted.
        """
        if self.__acord121Fields:
            # Retrieve ACORD 121 order from blob
            sQuery = """
                select blobhandle
                from rh_blobs
                where blobid = (select max(blobid)
                                from acord_order
                                where source_code = '{source_code!s:s}'
                                and trackingid = '{trackingid!s:s}')
                """.format(source_code=self.__case.contact.source_code,
                           trackingid=self.__case.trackingId)
            xmitConfig = ASAP_UTILITY.getXmitConfig()
            acordCursor = xmitConfig.getCursor(xmitConfig.DB_NAME_ACORD)
            acordCursor.execute(sQuery)
            rec = acordCursor.fetch(True)
            if not rec:
                self.__logger.warn('Unable to locate acord 121 blob for tracking id {trackingid!s:s}'
                                   .format(trackingid=self.__case.trackingId))
                return False

            data = rec[0].read()
            acordCursor.rollback()

            parser = AcordXML.AcordXMLParser()
            handler = parser.parseString(data)
            if parser.getException():
                # exception email should be generated for this, so just return False
                return False
            errorHandler = parser.getErrorHandler()
            if errorHandler.warnings:
                self.__logger.info('Warnings parsing 121 for {trackingid!s:s}:'.format(trackingid=self.__case.trackingId))
                for warning in errorHandler.warnings:
                    self.__logger.info(warning)
            if errorHandler.errors:
                self.__logger.warn('Errors parsing 121 for {trackingid!s:s}:'.format(trackingid=self.__case.trackingId))
                for error in errorHandler.errors:
                    self.__logger.warn(error)
                return False

            if handler:
                # process acord 121 fields now
                txLifeElement = handler.txList[0]
                fError = False
                # try to process all fields, so all problems that occur can be logged
                # before deciding to return False
                for field in self.__acord121Fields:
                    elem = txLifeElement.getElement(field.getReference())
                    if elem:
                        if not field.setValue(elem.value):
                            fError = True
                    else:
                        self.__logger.warn('Field {name!s:s} not found in 121 for {trackingid!s:s}.'
                                           .format(name=field.getReference(), trackingid=self.__case.trackingId))
                        # don't error, let the required check take care of this later
                        # fError = True
                if fError:
                    return False
                # there were no problems, so assign the handler for access by
                # a derived class, if needed
                self.__acordHandler = handler
            else:
                self.__logger.warn('No handler was returned from parsing 121 for {trackingid!s:s}.'
                                   .format(trackingid=self.__case.trackingId))
                return False

        return True

    def __processDeltaQCFields(self):
        """
        If there are Delta fields to process, then attempt to pull the
        necessary fields.  The values should exist in either the ASAPCase
        object or the ASAPDocument object(s).  Return True if okay to
        continue, False if processing should be halted.
        """
        if self.__deltaFields:
            contact = self.__case.contact
            fError = False
            for field in self.__deltaFields:
                ref = field.getReference()
                tokens = ref.strip().split('.')
                if len(tokens) == 2:
                    fNotSupported = False
                    refObject, refValue = tokens
                    sValue = ''
                    if refObject == field.REF_ASAPCASE:
                        if refValue == field.REF_DOCCOUNT:
                            sValue = str(len(self.__case.getDocuments()))
                        elif refValue == field.REF_TRACKINGID:
                            sValue = self.__case.trackingId
                        else:
                            fNotSupported = True
                    elif refObject == field.REF_ASAPDOCUMENT:
                        if refValue == field.REF_DATECREATED:
                            sValue = self.__currentDocument.getDateCreated()
                        elif refValue == field.REF_PAGECOUNT:
                            sValue = str(self.__currentDocument.pageCount)
                        elif refValue == field.REF_DOCTYPENAME:
                            sValue = self.__currentDocument.getDocTypeName()
                        elif refValue == field.REF_CLIENTDOCNAME:
                            sValue = contact.docTypeNameMap.get(
                                self.__currentDocument.getDocTypeName())
                            if not sValue:
                                sValue = ''
                        else:
                            fNotSupported = True
                    else:
                        fNotSupported = True
                    if fNotSupported:
                        self.__logger.warn('Reference {ref!s:s} is not currently supported.'.format(ref=ref))
                        fError = True
                    elif not field.setValue(sValue):
                        fError = True
                else:
                    self.__logger.warn('Reference {ref!s:s} is not properly formed for field {field!s:s}.'
                                       .format(ref=ref, field=field.getName()))
                    fError = True
            if fError:
                return False
        return True

    def __processLIMSFields(self):
        """
        If there are LIMS fields to process, attempt to pull the fields.
        Check whether the case's sid is in sip or snip, and pull data.
        Return True if okay to continue, False if processing should be halted.
        """
        if self.__limsFields:
            fError = False
            tableFieldMap = {}
            for field in self.__limsFields:
                ref = field.getReference()
                tokens = ref.strip().split('.')
                if len(tokens) == 2:
                    refObject, refValue = tokens
                    fieldList = tableFieldMap.get(refObject)
                    if not fieldList:
                        fieldList = []
                        tableFieldMap[refObject] = fieldList
                    fieldList.append((refValue, field))
                else:
                    self.__logger.warn('Reference {ref!s:s} is not properly formed for field {field!s:s}.'
                                       .format(ref=ref, field=field.getName()))
                    fError = True
            sid = self.__case.sid
            cursor = ASAP_UTILITY.getLIMSCursorForSid(sid)
            if cursor:
                for tableName in list(tableFieldMap.keys()):
                    fieldList = tableFieldMap[tableName]
                    fieldNames = [refValue for refValue, field in fieldList]
                    sQuery = """
                        select {cols!s:s}
                        from {table!s:s}
                        where sid = '{sid!s:s}'
                        """.format(cols=','.join(fieldNames), table=tableName, sid=sid)
                    cursor.execute(sQuery)
                    rec = cursor.fetch(True)
                    cursor.rollback()
                    if rec:
                        index = 0
                        fields = [field for refValue, field in fieldList]
                        for field in fields:
                            if not rec[index]:
                                field.setValue('')
                            elif isinstance(rec[index], datetime.datetime):
                                field.setValue(rec[index].strftime('%Y-%m-%d %H:%M:%S'))
                            else:
                                field.setValue(str(rec[index]))
                            index += 1
                    else:
                        self.__logger.warn("Field values could not be found in LIMS for sid {sid!s:s}."
                                           .format(sid=sid))
                        fError = True
            else:
                self.__logger.warn("Sid {sid!s:s} cannot be found in LIMS.".format(sid=sid))
                fError = True
            if fError:
                return False
        return True

    def __writeIndex(self):
        """
        Write index to file.  Format based on document or case index.
        """
        contact = self.__case.contact
        if contact.index.type == contact.index.IDX_TYPE_CASE:
            idxBase = self.__case.trackingId
        else:
            idxBase = self.__currentDocument.fileName.split('.')[0]
        idxPath = os.path.join(contact.index_dir, '{idxBase!s:s}.IDX'.format(idxBase=idxBase))
        self.__idxPaths.append(idxPath)
        return contact.index.writeFile(idxPath)

    def __moveImagesToProcessed(self):
        """
        Move indexed images to processed subfolder
        """
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in list(self.__case.getDocuments().values()):
            docPath = os.path.join(self.__case.contact.document_dir,
                                   doc.fileName)
            processedPath = os.path.join(self.__case.contact.document_dir,
                                         processedSubdir,
                                         doc.fileName)
            if os.path.isfile(docPath):
                CRLUtility.CRLCopyFile(docPath, processedPath, True, 5)

    def buildIndexesForCase(self, asapCase):
        """
        This is the only public method for this class.  Call this to generate index files
        for a case and its documents.  Index files are generated and stored based upon
        contact specifications tied to case object.

        :param ASAPCase asapCase:
        """
        self.__reset()
        self.__case = asapCase
        self.__case.contact.index.reset()
        # call check to build indexes
        if not self._isReadyToIndex():
            return False
        # call custom preprocessing
        if not self._preProcessIndex():
            self.__logger.warn('Preprocess failed for case ({sid!s:s}/{trackingid!s:s}).'
                               .format(sid=asapCase.sid, trackingid=asapCase.trackingId))
            return False
        # gather fields in groups
        contact = self.__case.contact
        fieldNames = contact.index.getOrderedFieldNames()
        for name in fieldNames:
            field = contact.index.getField(name)
            if field.getSource() == field.SRC_ACORD103:
                self.__acord103Fields.append(field)
            elif field.getSource() == field.SRC_ACORD121:
                self.__acord121Fields.append(field)
            elif field.getSource() == field.SRC_DELTA_QC:
                self.__deltaFields.append(field)
            elif field.getSource() == field.SRC_LIMS:
                self.__limsFields.append(field)
        # process the 'lims' fields
        if not self.__processLIMSFields():
            asapCase.contact.index.reset()
            return False
        # process the 'acord121' fields
        if not self.__processAcord121Fields():
            asapCase.contact.index.reset()
            return False
        # process the 'acord103' fields
        if not self.__processAcord103Fields():
            asapCase.contact.index.reset()
            return False
        docList = list(asapCase.getDocuments().values())
        for asapDocument in docList:
            self.__currentDocument = asapDocument
            # process the 'deltaqc' fields
            if not self.__processDeltaQCFields():
                asapCase.moveToError()
                asapCase.contact.index.reset()
                return False
            # call custom processing for derived fields,
            # and for tweaking other fields as needed by
            # contact-specific class
            if not self._processDerivedFields():
                asapCase.moveToError()
                self.__logger.warn('Process derived fields failed for case ({sid!s:s}/{trackingid!s:s}).'
                                   .format(sid=asapCase.sid, trackingid=asapCase.trackingId))
                asapCase.contact.index.reset()
                return False
            # write index file to contact-specific staging location
            if not self.__writeIndex():
                asapCase.moveToError()
                return False
            # if case index type, break out of loop now
            if contact.index.type == contact.index.IDX_TYPE_CASE:
                break
        # call custom postprocessing
        if not self._postProcessIndex():
            asapCase.moveToError()
            self.__logger.warn('Postprocess failed for case ({sid!s:s}/{trackingid!s:s}).'
                               .format(sid=asapCase.sid, trackingid=asapCase.trackingId))
            return False
        # move indexed images to processed subfolder to prevent reindexing
        self.__moveImagesToProcessed()
        return True


class TestCustomHandler(ASAPIndexHandler):
    def _preProcessIndex(self):
        print('in custom class preprocess')
        case = self._getCase()
        print('case = ({sid!s:s}/{trackingid!s:s})'.format(sid=case.sid, trackingid=case.trackingId))
        return True

    def _processDerivedFields(self):
        print('in custom class process derived fields')
        doc = self._getCurrentDocument()
        print('Docid = {docid:d}, doctype = {doctype!s:s}'.format(docid=doc.getDocumentId(), doctype=doc.getDocTypeName()))
        return True

    def _postProcessIndex(self):
        print('in custom class postprocess')
        return False
