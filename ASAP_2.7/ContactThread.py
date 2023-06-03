"""

  Facility:         ILS

  Module Name:      ContactThread

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPContactThread class.

  Author:
      Jarrod Wild

  Creation Date:
      20-Nov-2006

  Modification History:
      02-JUN-2015     rsu     Ticket # 57129
          Add try/except to prevent thread death due to invalid index data. An error email will identify the problem case.

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
import threading
from .MainHandler import ASAPMainHandler


class ASAPContactThread(threading.Thread):
    """
    This class is for a contact-level processing thread, and
    allows for concurrent processing of all contacts from indexing
    to transmission.
    """
    def __init__(self, contact, logger=None):
        threading.Thread.__init__(self)
        self.__contact = contact
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger
        self.__handler = ASAPMainHandler()

    def run(self):
        """
        Execution method for this thread.
        """
        # import time
        # print('In thread for contact {contact_id!s:s}.'.format(contact_id=self.__contact.contact_id))
        # time.sleep(20)
        fError = False
        try:
            # process indexes for contact
            self.__logger.info("Thread {tname!s:s} started.".format(tname=threading.currentThread().getName()))
            self.__logger.info('Building indexes for contact {contact!s:s}...'.format(contact=self.__contact.contact_id))
            exportedCases = self.__handler.getExportedCasesForContact(self.__contact)
            fError = self.__handler.getErrorState() or fError
            if exportedCases:
                self.__logger.info('For contact {contact_id!s:s}, there are {numCases:d} cases to index...'
                                   .format(contact_id=self.__contact.contact_id, numCases=len(exportedCases)))
                for exportedCase in exportedCases:
                    try:
                        if self.__handler.buildIndexesForCase(exportedCase) and self.__handler.billCase(exportedCase):
                            # add the 477 if indexing and billing are successful
                            self.__handler.addLIMSMessage(exportedCase, self.__handler.MSG_IMAGES_AVAILABLE)
                    except:
                        fError = True
                        self.__logger.error('Exception in thread building indexes for {contact_id!s:s}. Please correct so transmission can continue for case {sid!s:s}/{trackingId!s:s}.'
                                            .format(contact_id=exportedCase.contact.contact_id, sid=exportedCase.sid, trackingId=exportedCase.trackingId))

                        fError = self.__handler.getErrorState() or fError
            # get indexed cases for contact and do transmit processing
            indexedCases = self.__handler.getIndexedCasesForContact(self.__contact)
            fError = self.__handler.getErrorState() or fError
            if indexedCases:
                self.__logger.info('For contact {contact_id!s:s}, there are {numCases:d} cases to stage for transmission...'
                                   .format(contact_id=self.__contact.contact_id, numCases=len(indexedCases)))
                stagedCases = []
                self.__handler.stageAndTransmitCases(indexedCases, self.__contact, stagedCases)
                for asapCase in stagedCases:
                    # add 377 message once case has been staged to transmit
                    # also send ACORD status
                    self.__handler.addLIMSMessage(asapCase, self.__handler.MSG_IMAGES_RELEASED)
                    fError = self.__handler.getErrorState() or fError
                    self.__handler.pushAcordStatus(asapCase, self.__handler.STATUS_SENT_TO_CLIENT)
                    fError = self.__handler.getErrorState() or fError
                fError = self.__handler.getErrorState() or fError
            else:
                # in case there are images that failed to transmit previously,
                # run this without passing any indexed cases
                self.__handler.stageAndTransmitCases([], self.__contact, [])
                fError = self.__handler.getErrorState() or fError
        except Exception:
            self.__logger.exception('Error')
            self.__logger.warn('Exception in thread for contact {contact_id!s:s} (see above).'.format(contact_id=self.__contact.contact_id))
        if fError:
            self.__logger.error('There was at least one error processing ASAP cases for contact {contact_id!s:s}.'
                                .format(contact_id=self.__contact.contact_id))
        self.__logger.info('Thread-based case processing for contact {contact_id!s:s} complete.'
                           .format(contact_id=self.__contact.contact_id))
