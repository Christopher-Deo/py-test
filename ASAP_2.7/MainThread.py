"""

  Facility:         ILS

  Module Name:      MainThread

  Version:
      Software Version:          Python version 2.3

      Copyright 2007, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script is the main script to run for ASAP index/transmit
      processing.

  Author:
      Jarrod Wild

  Creation Date:
      20-Jan-2007

  Modification History:

"""
# import sets

import time

from ILS.ASAP.Utility import ASAP_UTILITY
from ILS.ASAP.ContactThread import ASAPContactThread
from ILS.ASAP.MainHandler import ASAPMainHandler


def run():
    print('In run()')
    fError = False
    begintime = time.time()
    logger = ASAP_UTILITY.asapLogger
    logger.info('\n\n{}'.format('-' * 100))
    config = ASAP_UTILITY.getXmitConfig()
    try:
        fm = ASAP_UTILITY.getASAPFileManager()
        fm.purgeNullFiles()
        contacts = config.getContacts()
        mainHandler = ASAPMainHandler(logger)
        # export cases
        releasedCases = mainHandler.getReleasedCases()
        fError = mainHandler.getErrorState() or fError
        for case in releasedCases:
            mainHandler.exportCase(case)
            fError = mainHandler.getErrorState() or fError
        mainHandler.reportPreExportedCases()
        # process all configured ASAP contacts, then do a join on each thread
        contactThreads = []
        for contact in list(contacts.values()):
            contactThread = ASAPContactThread(contact, logger)
            contactThreads.append(contactThread)
        # use a thread pool of size 5
        runningThreads = []
        completeThreads = []
        maxThreads = 1
        fComplete = False
        while not fComplete:
            for contactThread in contactThreads:
                if contactThread in runningThreads:
                    if contactThread.isAlive():
                        try:
                            contactThread.join(1.0)
                        except Exception:
                            logger.warn("A thread may have ended uncleanly.")
                    if not contactThread.isAlive():
                        runningThreads.remove(contactThread)
                        completeThreads.append(contactThread)
                elif contactThread not in completeThreads and len(runningThreads) < maxThreads:
                    contactThread.start()
                    runningThreads.append(contactThread)
            if len(runningThreads) == 0:
                fComplete = True
    except Exception:
        logger.exception('Error')
    if fError:
        logger.error('There was at least one error exporting released ASAP cases.')
    logger.info('ASAP processing complete.')
    logger.info('Time to execute this task was {elapsed:.03f} seconds.'.format(elapsed=(time.time() - begintime)))


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print('Exception: {}'.format(str(e)))
