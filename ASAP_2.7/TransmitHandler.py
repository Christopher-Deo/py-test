"""

  Facility:         ILS

  Module Name:      TransmitHandler

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPTransmitHandler class.

  Author:
      Jarrod Wild

  Creation Date:
      03-Nov-2006

  Modification History:
      01-Dec-2010   rsu     Ticket # 18174
          Work-around to not restage failed _stageIndexedCase for troslqapps

      13-Jun-2017   Komal Gandhi
          Adding logging for AGI MTD

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
from .Case import ASAPCase
from .Contact import ASAPContact
from .FileManager import ASAPFile
from .Utility import ASAP_UTILITY


class ASAPTransmitHandler(object):
    """
    This is a helper class for overseeing the final staging and
    transmission of cases.  This class must be overridden to be
    of any use.
    """

    def __init__(self, logger=None):
        # print('in base init')
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger
        self.__contact = ASAPContact(logger)
        self.__currentCase = ASAPCase()
        self.__stagedCases = []

    def _preStage(self):
        """
        Derived class should override this method to perform any
        pre-processing.
        """
        return True

    def _isIndexedCaseReady(self):
        """
        Derived class should override this method if there are other
        conditions to be checked before transmission (is the LIMS transmit
        date set, for example).  Returns True by default.  Return False
        to hold case from transmission without causing an error.
        """
        # print('base class check for readiness does nothing')
        return True

    def _isFirstTransmit(self):
        """
        Check if current case is being transmitted for first time.
        """
        fFirst = True
        hist = ASAP_UTILITY.getDocumentHistory()
        if hist.getTrackedDocidsForCase(self.__currentCase, hist.ACTION_TRANSMIT):
            fFirst = False
        return fFirst

    def _isFullTransmit(self):
        """
        Check if entire current case is being transmitted.
        """
        fFull = True
        hist = ASAP_UTILITY.getDocumentHistory()
        xmitRecs = hist.getTrackedDocidsForCase(self.__currentCase, hist.ACTION_TRANSMIT)
        xmitDocids = [docid for docid, auditstamp in xmitRecs]
        docids = list(self.__currentCase.getDocuments().keys())
        for docid in docids:
            if docid in xmitDocids:
                xmitDocids.remove(docid)
        if xmitDocids:
            fFull = False
        return fFull

    def _stageIndexedCase(self):
        """
        Derived class should override this method to perform any
        processing for an indexed case prior to transmission.  Return
        True to allow process to continue, False to halt processing.
        """
        # print('base class stage indexed case does nothing')
        return True

    def _transmitStagedCases(self):
        """
        Derived class should override this method to perform any
        processing of derived fields (and other custom processing) for
        a particular index.  Return True to allow process to continue,
        False to halt processing.
        """
        # print('base class transmit staged cases does nothing')
        return True

    # noinspection PyMethodMayBeStatic
    def _postTransmit(self):
        """
        Derived class should override this method to perform any
        post-processing.
        """
        return True

    def _getLogger(self):
        """
        This accessor is so a derived class can get the logger.
        """
        return self.__logger

    def _getContact(self):
        """
        This accessor is so a derived class can get the contact.
        """
        return self.__contact

    def _getCurrentCase(self):
        """
        This accessor is so a derived class can get the current case
        (when staging or transmitting).
        """
        return self.__currentCase

    def _getStagedCases(self):
        """
        This accessor is so a derived class can get the list of staged
        cases.
        """
        return self.__stagedCases

    def stageAndTransmitCases(self, asapCases, asapContact, stagedCases):
        """
        This method returns True if there were no errors in staging
        and/or transmitting the list of cases.  If the method returns
        False, it only means that there was a failure in the process, not
        necessarily that the transmission itself failed.  The details of
        the particular failure(s) would be found in the log.  Successfully
        staged cases are placed in the stagedCases list (which should be
        initially empty when passed in).
        """
        fSuccess = True
        self.__contact = asapContact
        # delete all files marked for deletion by file manager
        fm = ASAP_UTILITY.getASAPFileManager(self.__contact)
        asapFiles = fm.getFilesByState(ASAPFile.STATE_MARKED_FOR_DELETION)
        for asapFile in asapFiles:
            fm.deleteFile(asapFile)
        if not self._preStage():
            self.__logger.warn('Pre-stage process failed.')
            return False
        for asapCase in asapCases:
            self.__currentCase = asapCase
            if self._isIndexedCaseReady():
                # derived class will do work using overridden method
                fExc = False
                fStaged = False
                try:
                    fStaged = self._stageIndexedCase()
                except Exception:
                    if (self.__contact.contact_id == 'troslqapps'):
                        self.__logger.warn('Staging of troslqapps indexed case ({sid!s:s}/{trackingId!s:s}) caused an exception, do not restage:'
                                           .format(sid=asapCase.sid, trackingId=asapCase.trackingId), exc_info=True)
                    elif (self.__contact.contact_id == 'agimtdapps'):
                        self.__logger.warn('Staging of agimtdapps indexed case ({sid!s:s}/{trackingId!s:s}) caused an exception'
                                           .format(sid=asapCase.sid, trackingId=asapCase.trackingId), exc_info=True)
                    else:
                        self.__logger.warn('Staging of indexed case ({sid!s:s}/{trackingId!s:s}) caused an exception, restaging:'
                                           .format(sid=asapCase.sid, trackingId=asapCase.trackingId), exc_info=True)
                        ASAP_UTILITY.reStageToTransmit(asapCase)
                        fExc = True

                if not fExc:
                    if fStaged:
                        # add case object to list of staged cases
                        self.__stagedCases.append(self.__currentCase)
                        stagedCases.append(self.__currentCase)
                    else:
                        asapCase.moveToError()
                        self.__logger.warn('Staging of indexed case ({sid!s:s}/{trackingId!s:s}) failed.'
                                           .format(sid=asapCase.sid, trackingId=asapCase.trackingId))
                    fSuccess = fSuccess and fStaged
        # track all documents for all staged cases as ready to transmit
        docHistory = ASAP_UTILITY.getDocumentHistory()
        for asapCase in self.__stagedCases:
            docs = list(asapCase.getDocuments().values())
            for asapDoc in docs:
                docHistory.trackDocument(asapDoc, docHistory.ACTION_TRANSMIT)
            if (self.__contact.contact_id == 'agimtdapps'):
                self.__logger.info('Transmit Info added for indexed case ({sid!s:s}/{trackingId!s:s})'
                                   .format(sid=asapCase.sid, trackingId=asapCase.trackingId))
        # derived class will do the work, and can choose not to actually
        # transmit (if transmission should occur at another time)
        fTransmitted = self._transmitStagedCases()
        if not fTransmitted:
            self.__logger.warn('Transmitting staged cases failed.')
        if not self._postTransmit():
            self.__logger.warn('Post-transmit process failed.')
            fSuccess = False
        fSuccess = fSuccess and fTransmitted
        return fSuccess
