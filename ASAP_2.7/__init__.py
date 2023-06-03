__author__ = 'Jarrod Wild'
__version__ = '20190917.01'

#
# The following lines are used to determine in this module should run as a development
# or production instance
#
import DevInstance
# devState = DevInstance.devInstance(True)
devState = DevInstance.devInstance()

import CRLUtility
import os
import sys

if devState.isDevInstance():
    logDir = r'\\Ntsys1\ils_appl\log\test'
    logEmail = 'nelsonj@crlcorp.com'
else:
    # log directory using E drive on ntilsapphub-pvl1
    logDir = r'e:\log\ILS'
    if not os.path.exists(logDir):
        logDir = r'\\ilsdfs\apphub$\log\ILS'
    logEmail = ('ilsprod@crlcorp.com',)

logFileName = os.path.join(logDir, 'ASAP_{scriptname!s:s}.log'.format(scriptname=os.path.splitext(os.path.basename(sys.argv[0]))[0]))

#
# Log file handling.
#
ASAP_LOG_EMAIL_SUBJECT = 'ASAP System Process Error'

try:
    # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
    asapLogger
except NameError:
    asapLogger = CRLUtility.CRLGetLogger()
    if not asapLogger.handlers:
        asapLogger = CRLUtility.CRLGetLogger(sLogFile=logFileName,
                                             sEMailSubject=ASAP_LOG_EMAIL_SUBJECT,
                                             eMailToAddress=logEmail,
                                             iRotatingFileSize=2 ** 24)
