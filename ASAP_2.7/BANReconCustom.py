"""

  Facility:         ILS

  Module Name:      BANReconCustom

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      NOTE: This is temporary script used to handle Banner ASAP and non-ASAP reconciliation.
      It was copied from BANCustom.py, and is expected to be merged back into BANCustom.py after
      Banner provides new reconciliation requirements.

  Author:
      Jarrod Wild

  Creation Date:
      01-Nov-2007

  Modification History:
      5-Dec-2012    kg
           Banner Reconciliation changes

      01-JUL-2015   rsu     Ticket # 60581
          Use ASAPUtility to push an ACORD approved-by-client status (5).

      13-Jul-2015   venkatj Ticket 48263
          Imaging system upgrade project. Master ticket 45490.
          Use a common tool ILS.Utils.DocServConnSettings.py to connect to the database

      01-May-2018   amw     SCTASK0019411
          Update banner recon.  Change file names to be more clean, attempt to fix an issue
          that caused 17,000 images to get resent.   Remove extraneous coding and separate the
          functions into smaller chunks to help with readability and self documentation.
          simplified the process to be used on new apphub.  Added logging to help when troubleshooting
          and rerunning.

      01-Jun-2018   amw     CHG0032589
          Removed extra letter before checking in ILSDocumentBundling for pageId

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""
from __future__ import division, absolute_import, with_statement, print_function
import DevInstance

# devState = DevInstance.devInstance(True)
devState = DevInstance.devInstance()

import os
import re
import sys
import time
import glob
import ftplib
import datetime
import CRLUtility
from ILS import ILSIndexCreation
from ILS import ILSDocumentBundling as idb

if devState.isDevInstance():
    logger = CRLUtility.CRLGetLogger(r'\\ilsdfs\apphub$\Test\log\ILS\BANReconCustom.log', 'nelsonj@crlcorp.com', iLogLevel=20)
else:
    logger = CRLUtility.CRLGetLogger(r'\\ilsdfs\apphub$\log\ILS\BANReconCustom.log', 'ilsprod@crlcorp.com', iLogLevel=20)

from ILS.ASAP.Utility import ASAP_UTILITY

ASAP_BASE_DIR = r'\\ilsdfs\sys$\xmit\ftp\BAN0003'
NON_ASAP_BASE_DIR = r'\\ilsdfs\sys$\xmit\ftp\BAN0004'
OUTBOX_BASE_DIR = r'\\ilsdfs\sys$\xmit\email\CertifiedMail\BAN2'

if ASAP_UTILITY.devState.isDevInstance():
    ASAP_BASE_DIR = os.path.join(ASAP_BASE_DIR, 'test')
    NON_ASAP_BASE_DIR = os.path.join(NON_ASAP_BASE_DIR, 'test')
    OUTBOX_BASE_DIR = os.path.join(OUTBOX_BASE_DIR, 'test')
    RECIPIENTS = 'nelsonj@crlcorp.com'
else:
    RECIPIENTS = "ilsprod@crlcorp.com"

COLOR1 = 'aqua'
COLOR2 = 'white'

SENTLIST = 'sentlist_{}.txt'

OUTBOX_DIR = os.path.join(OUTBOX_BASE_DIR, 'work')

TRACKING_DIR = os.path.join(NON_ASAP_BASE_DIR, 'imaging', 'recon', 'tracking')
PROCESSED_DIR = os.path.join(TRACKING_DIR, 'Processed')

RECON_DIR = os.path.join(ASAP_BASE_DIR, 'imaging', 'recon')
BACKUP_DIR = os.path.join(RECON_DIR, 'Recon_From_Client')
WORK_DIR = os.path.join(RECON_DIR, 'Work_Recon')

DIRS_TO_CLEAN = [BACKUP_DIR, PROCESSED_DIR, OUTBOX_DIR]

TODAY = datetime.datetime.today()

R_TYPE_GET_RECON_ONLY = 'GET_RECON_ONLY'
R_TYPE_REMOVE_RECONCILED = 'REMOVE_RECONCILED'
R_TYPE_REMOVE_RECONCILED_NOASAP = 'REMOVE_RECONCILED_NOASAP'
R_TYPE_EXCLUDE_NON_ASAP_AND_MAX = 'EXCLUDE_NON_ASAP_AND_MAX'
R_TYPE_EXCLUDE_NON_ASAP = 'EXCLUDE_NON_ASAP'
R_TYPE_IGNORE_MAX = 'IGNORE_MAX'
R_TYPE_CREATE_SENT_ONLY = 'CREATE_SENT_ONLY'
R_TYPE_NORMAL = 'NORMAL'

R_TYPES_AVAILABLE = (R_TYPE_NORMAL, R_TYPE_CREATE_SENT_ONLY, R_TYPE_IGNORE_MAX, R_TYPE_EXCLUDE_NON_ASAP, R_TYPE_EXCLUDE_NON_ASAP_AND_MAX, R_TYPE_REMOVE_RECONCILED, R_TYPE_GET_RECON_ONLY, R_TYPE_REMOVE_RECONCILED_NOASAP)


def writeHTMLHeader(filePtr, strTitle):
    strhtml = ""
    strhtml += "<table border=1 cellspacing=0>\n"
    strhtml += "   <caption>{strTitle!s:s}</caption>\n".format(strTitle=strTitle)
    strhtml += "   <tr style=\"background-color:black;color:{color!s:s};\">\n".format(color=COLOR1)
    strhtml += "      <th>Carrier</th>\n"
    strhtml += "      <th>File Name</th>\n"
    strhtml += "      <th>CRL ID (SID)</th>\n"
    strhtml += "      <th>Last Name</th>\n"
    strhtml += "      <th>First Name</th>\n"
    strhtml += "      <th>Last Transmit Date/Time</th>\n"
    strhtml += "   </tr>\n"
    filePtr.write(strhtml)


def writeHTMLBody(filePtr, dataRows):
    htmlrow = 0
    for carrier, xmitDate, fileName, sid, lname, fname in dataRows:
        bgcolor = COLOR1
        if htmlrow % 2 == 0:
            bgcolor = COLOR2
        strhtml = ""
        strhtml += "   <tr style=\"background-color:{bgcolor!s:s};\">\n".format(bgcolor=bgcolor)
        strhtml += "        <td>{carrier!s:s}</td>\n".format(carrier=carrier)
        strhtml += "        <td>{fileName!s:s}</td>\n".format(fileName=fileName)
        strhtml += "        <td>{sid!s:s}</td>\n".format(sid=sid)
        strhtml += "        <td>{lname!s:s}</td>\n".format(lname=lname)
        strhtml += "        <td>{fname!s:s}</td>\n".format(fname=fname)
        strhtml += "        <td>{xmit:%d-%b-%Y %H:%M:%S}</td>\n".format(xmit=xmitDate)
        strhtml += "   </tr>\n"
        filePtr.write(strhtml)
        htmlrow += 1


def writeHTMLFooter(filePtr):
    strhtml = "</table>"
    filePtr.write(strhtml)


def build_BAN_ASAP_report(detailList, reportFile, sTitle):
    dataRows = []
    for contact_id, xmitDate, fileName, sid, lname, fname in detailList:
        carrier = 'Banner Life'
        if contact_id.upper().startswith('WMP'):
            carrier = 'William Penn'
        dataRows.append((carrier, xmitDate, fileName, sid, lname, fname))
    dataRows.sort()
    filePtr = open(reportFile, 'w')
    try:
        writeHTMLHeader(filePtr, sTitle)
        writeHTMLBody(filePtr, dataRows)
        writeHTMLFooter(filePtr)
    finally:
        filePtr.close()


def getReconFile(logger):
    """
    Download the recon recon_file received from Banner.
    """
    sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo('CRLCORP2')
    ftpServerBAN = ftplib.FTP(sServer, sUser, sPassword)
    recon_files = CRLUtility.CRLFTPList(ftpServerBAN, os.path.join(r'/banftp1/Recon', "*.*"))
    logger.info('found: {count:d} files to process: {files!s:s}'.format(count=len(recon_files), files=','.join(recon_files)))
    for recon_file in recon_files:
        localFilePath = os.path.join(WORK_DIR, recon_file)
        origFilepath = os.path.join(BACKUP_DIR, recon_file)
        CRLUtility.CRLFTPGet(ftpServerBAN, os.path.join(r'/banftp1/Recon', recon_file), localFilePath)
        # Keep copy the files from Banner in another ORIGINAL folder in case something fails
        CRLUtility.CRLFTPGet(ftpServerBAN, os.path.join(r'/banftp1/Recon', recon_file), origFilepath)

    # Delete after copying files, there was problem when the statement was put with above loop
    # so doing separately
    for recon_file in recon_files:
        logger.info('File full delete path {path!s:s}'.format(path=(r'/banftp1/Recon/' + recon_file)))
        CRLUtility.CRLFTPDelete(ftpServerBAN, r'/banftp1/Recon/' + recon_file)
    zipfilename = os.path.join(BACKUP_DIR, TODAY.strftime('%Y%m%d%H%M%S.zip'))
    CRLUtility.CRLZIPFiles(os.path.join(BACKUP_DIR, 'CRL*.TXT'), zipfilename)
    CRLUtility.CRLAddToZIPFile(os.path.join(WORK_DIR, SENTLIST.format('*')), zipfilename, False)


def setReconDateForNonASAPDocs(tifName):
    """
    If a DSBI entry exists for tifName, set its dsbi_reconcile_date to current_timestamp.
    """
    query = """
        select count(*)
        from doc_serv_bundle_image
        where dsbi_image_file = '{tifName!s:s}'
        and dsbi_facility_id in ('bas','bad','bac','bag')
    """.format(tifName=tifName)
    upd = """
        update doc_serv_bundle_image
        set dsbi_reconcile_date = current_timestamp
        where dsbi_image_file = '{tifName!s:s}'
        and dsbi_facility_id in ('bas','bad','bac','bag')
    """.format(tifName=tifName)
    cursor = idb.ilsConnection.cursor()
    cursor.execute(query)
    data = cursor.fetchone()
    count = 0
    if data:
        count = data[0]
    if count > 0:
        logger.debug('Updating existing DSBI entry for {tifName!s:s}'.format(tifName=tifName))
        cursor.execute(upd)
    cursor.close()


def findDocType(tifName):
    """
    Find the document type based on the facility id.
    bas, bad, bac, bag
    """
    query = """
        SELECT dsbi_facility_id
        FROM doc_serv_bundle_image
        WHERE dsbi_image_file = '{tifName!s:s}'
        and dsbi_facility_id in ('bas','bad','bac','bag')
        """.format(tifName=tifName)  # sql server is case insensitive
    cursor = idb.ilsConnection.cursor()
    cursor.execute(query)
    data = cursor.fetchone()
    cursor.close()
    if data:
        facility, = data
        if facility.lower() == 'bad':
            return 'clientdoc'
        elif facility.lower() == 'bac':
            return 'consent'
        else:
            return 'paramed'


def createFile(filename, lValues):
    if not isinstance(lValues, list):
        lValues = list(lValues)
    lValues.sort()
    with open(filename, 'w') as fn:
        fn.write('\n'.join(lValues))


def runReconciliation(logger, limitToMax, retransNonAsap, bProcessNonTransmitted, bResetAccordStatus, reconDate):
    """
    Compare the reconciliation files and find the images that have not been reconciled.
    """
    logger.info('running reconciliation for {reconDate!s:s}'.format(reconDate=reconDate))
    xmitConfig = ASAP_UTILITY.getXmitConfig()
    asapcursor = xmitConfig.getCursor(xmitConfig.DB_NAME_XMIT)
    docFactory = ASAP_UTILITY.getDocumentFactory()

    banReconList = processBanReconFiles(logger, asapcursor, docFactory, bResetAccordStatus)

    banExtraFiles, nonTransmittedFiles, crlSentfiles = getIncongruencies(logger, banReconList)

    if bProcessNonTransmitted:
        caseFactory = ASAP_UTILITY.getCaseFactory()
        processNonTransmittedFiles(logger, asapcursor, caseFactory, docFactory, banExtraFiles, nonTransmittedFiles, limitToMax, retransNonAsap, reconDate)

    # Delete files in work_recon folder
    workreconfiles = glob.glob(os.path.join(WORK_DIR, 'CRL*.TXT')) + crlSentfiles
    for workreconfile in workreconfiles:
        CRLUtility.CRLDeleteFile(workreconfile)

    if not bProcessNonTransmitted:
        createFile(os.path.join(WORK_DIR, TODAY.strftime('CRL_MANUAL_%Y%m%d%H%M%S.TXT')), banExtraFiles)
        createFile(os.path.join(TRACKING_DIR, reconDate.strftime('MANUAL_%Y%m%d%H%M%S.dat')), nonTransmittedFiles)


def processNonTransmittedFiles(logger, asapcursor, caseFactory, docFactory, banExtraFiles, nonTransmittedFiles, limitToMax, retransNonAsap, reconDate):
    paramedFiles = []
    nonexistingFiles = []
    NonTransmittedFilesASAP = []
    NonTransDocAndConsent = []
    # Set the files to retransmit either through ASAP or NON ASAP route.
    if banExtraFiles:
        logger.warn('Found {count:d} Files that are missing in CRL\'s sent file'.format(count=len(banExtraFiles)))
    if nonTransmittedFiles:
        logger.warn('Found {count:d} unique Files that did not transmit'.format(count=len(nonTransmittedFiles)))
        if limitToMax and len(nonTransmittedFiles) >= 3000:
            logger.warn('following files possibly needing to resend: {files!s:s}'.format(files=','.join(nonTransmittedFiles)))
            raise ValueError('excessive files to retransmit, potential issue with Banner Recon Process: {count:d} files found'
                             .format(count=len(nonTransmittedFiles)))
        for nonTransmittedFile in nonTransmittedFiles:
            if nonTransmittedFile[0].isdigit():
                doc = docFactory.fromFileName(nonTransmittedFile)
            else:
                doc = None
            if doc:  # ASAP
                retransAsapImage(logger, asapcursor, idb.qcConnection, caseFactory, nonTransmittedFile, doc.getDocumentId())
                NonTransmittedFilesASAP.append(nonTransmittedFile)
            else:
                # Find what kind of document
                doctype = findDocType(nonTransmittedFile)
                logger.debug('processing {nonTransmittedFile!s:s} - {doctype!s:s}'.format(nonTransmittedFile=nonTransmittedFile, doctype=doctype))
                if doctype == 'paramed':
                    paramedFiles.append(nonTransmittedFile)
                    continue
                if doctype == 'clientdoc':
                    if retransmitImage(logger, idb.clientDocConnection, nonTransmittedFile, retransNonAsap):
                        NonTransDocAndConsent.append(nonTransmittedFile)
                    else:
                        nonexistingFiles.append(nonTransmittedFile)
                elif doctype == 'consent':
                    if retransmitImage(logger, idb.consentConnection, nonTransmittedFile, retransNonAsap):
                        NonTransDocAndConsent.append(nonTransmittedFile)
                    else:
                        nonexistingFiles.append(nonTransmittedFile)
                else:
                    nonexistingFiles.append(nonTransmittedFile)
    else:
        logger.info('all files transmitted and reconciled successfully')

    # Send emails about the non transmitted files
    msg = ["The following files were not reconciled and have been setup to transmit again:"]
    msg.append("ASAP Files")
    msg.extend(NonTransmittedFilesASAP)
    msg.append('')
    msg.append("Client Documents and Consents:")
    msg.extend(NonTransDocAndConsent)
    msg.append('')
    msg.append("Smart paramed / GoParamed Image Files, Manually Retransmit:")
    msg.extend(paramedFiles)
    msg.append('')
    msg.append("The following files were not found in our ASAP/ Non-ASAP system:")
    msg.extend(nonexistingFiles)
    msg.append('')
    msg.append("The following files are included in the Banner recon file but not in CRL recon File, ignore as these are extra documents for previously transmitted cases:")
    msg.extend(banExtraFiles)
    sSubject = " CRL - Banner / William Penn Reconciliation for all documents on {reconDate:%d-%b-%Y}".format(reconDate=reconDate)
    CRLUtility.CRLSendEMail(RECIPIENTS, '\n'.join(msg), sSubject,
                            RECIPIENTS, '',
                            RECIPIENTS)


def processBanReconFiles(logger, asapcursor, docFactory, bResetAccordStatus):
    # Make a list of files from each recon and compare them
    BANReconList = set()
    ASAPReconSids = []  # ASAP sids already reconciled this run
    reconFromBAN = glob.glob(os.path.join(WORK_DIR, 'CRL*.txt'))
    dataLines = []
    for banReconFile in reconFromBAN:
        data = getFileData(banReconFile, True)
        if not data:
            logger.info('Empty Recon File received: {banReconFile!s:s}'.format(banReconFile=banReconFile))
            continue
        else:
            logger.info('Processing File {banReconFile!s:s} with {count:d} records'
                        .format(banReconFile=banReconFile, count=len(data)))
        dataLines.extend(data)
    dataLines = list(set(dataLines))
    logger.debug('processing {count:d} unique files'.format(count=len(dataLines)))
    for line in dataLines:
        try:
            if 'TIF' in line:
                filename = line.split('.')[0] + '.TIF'
                BANReconList.add(filename)
                # Add the Reconcile record in the asap_document_history table
                if filename[0].isdigit():
                    doc = docFactory.fromFileName(filename)
                else:
                    doc = None
                # If ASAP Case
                if doc:
                    # Reconcile this document in asap_document_history
                    sid = markASAPReconciled(logger, asapcursor, doc.getDocumentId())

                    # send ACORD approved-by-client status
                    if sid not in ASAPReconSids:
                        if bResetAccordStatus:
                            if setAcordStatusToApprovedByClient(logger, sid):
                                ASAPReconSids.append(sid)
                else:
                    # This document not found in ASAP then it must be non ASAP
                    # Add the reconcile date for this document in dbo.Doc_Serv_Bundle_Image
                    setReconDateForNonASAPDocs(filename)
        except Exception:
            logger.warn('Error occured while Reconciling Images received from the Banner')
    return BANReconList


def getIncongruencies(logger, BANReconList):
    CRLReconList = set()

    CRLfiles = glob.glob(os.path.join(WORK_DIR, SENTLIST.format('*')))
    for crlReconFile in CRLfiles:
        data = getFileData(crlReconFile, True)
        if data:
            logger.info('processing {count:d} records in CRL SentList: {crlReconFile!s:s}'.format(count=len(data), crlReconFile=crlReconFile))
            CRLReconList.update(data)
        else:
            logger.warn('Found Empty CRL SentList: {crlReconFile!s:s}'.format(crlReconFile=crlReconFile))
    return BANReconList - CRLReconList, CRLReconList - BANReconList, CRLfiles


def setAcordStatusToApprovedByClient(logger, sid):
    caseFact = ASAP_UTILITY.getCaseFactory()
    case = caseFact.fromSid(sid)
    if case:
        ASAP_UTILITY.pushAcordStatus(case.trackingId, case.source_code, ASAP_UTILITY.STATUS_APPROVED_BY_CLIENT)
        return True
    else:
        logger.warn('Unable to retrieve case trackingId while reconciling for Banner')
    return False


def markASAPReconciled(logger, asapcursor, docId):
    # Get the sid, contact_id
    sGetValues = """
        select sid, contact_id
        from asap_document_history
        where actionitem = 'transmit'
        and documentid = {docId:d}
        """.format(docId=docId)
    asapcursor.execute(sGetValues)
    recs = asapcursor.fetch()
    asapcursor.rollback()
    sid, contact_id = recs[0]
    logger.info('Reconciling sid: {sid!s:s}'.format(sid=sid))
    sInsert = """
        insert into asap_document_history (sid, documentid, contact_id, actionitem, actiondate)
        values ('{sid!s:s}', {docId:d}, '{contact_id!s:s}', 'reconcile', current_timestamp)""".format(sid=sid, docId=docId, contact_id=contact_id)
    asapcursor.execute(sInsert)
    asapcursor.commit()
    return sid


def retransAsapImage(logger, asapcursor, connection, caseFactory, nonTransmittedFile, docId):
    logger.debug('processing as ASAP Image: {nonTransmittedFile!s:s}'.format(nonTransmittedFile=nonTransmittedFile))
    cursor = connection.cursor()
    # Uncomment Later
    ILSIndexCreation.setExportFieldForDocId(logger,
                                            cursor,
                                            docId,
                                            'value_5',
                                            ILSIndexCreation.ILS_K_EXPORTED_NO)
    # Set the ACORD 103 to retrieve
    sGetValues = """
        select sid, contact_id
        from asap_document_history
        where actionitem = 'transmit'
        and documentid = {docId:d}""".format(docId=docId)
    asapcursor.execute(sGetValues)
    recs = asapcursor.fetch()
    asapcursor.rollback()
    sid, contact_id = recs[0]
    logger.info('Retransmit Sid {sid!s:s} and case'.format(sid=sid))
    case = caseFactory.fromSid(sid)
    # !!!!!!!New Change
    # As Banner has all the documents combined into one file so we need to retransmit the case
    # and not individual Documents.

    # Uncommented 3-jun
    if case:
        ASAP_UTILITY.reReleaseCase(case, True)
        logger.info('Retransmitting Banner Document id {docId:d} for tracking ID: {trackingId!s:s}'
                    .format(docId=docId, trackingId=case.trackingId))


def retransmitImage(logger, connection, nonTransmittedFile, retransNonAsap):
    cursor = connection.cursor()
    try:
        nonTransmittedFile = os.path.basename(nonTransmittedFile)
        nonTransmittedFile = re.compile('\d{8}\..*', re.I).findall(nonTransmittedFile)
        if nonTransmittedFile:
            docId = idb.getDocumentIdFromFileName(nonTransmittedFile[0], connection)
        else:
            docId = None
        if not docId:
            return False
        else:
            # Uncommented later
            if retransNonAsap:
                logger.warn('resetting client doc {docId:d} to resend'.format(docId=docId))
                ILSIndexCreation.setExportFieldForDocId(logger,
                                                        cursor,
                                                        docId,
                                                        'value_4',
                                                        ILSIndexCreation.ILS_K_EXPORTED_NO)
            else:
                logger.warn('skipping {docId:d} from resending'.format(docId=docId))
    finally:
        cursor.close()
    return True


def getMethodology(reconType, reconDate=TODAY):
    bGetRecon = bRunRecon = bConsolidate = bCreateAsapSent = bSendEmail = bLimitMax = bRetransNonAsap = bProcessNonTransmitted = bResetAccordStatus = True,
    if reconType == R_TYPE_NORMAL:
        pass
    elif reconType == R_TYPE_CREATE_SENT_ONLY:
        bGetRecon = bRunRecon = bCreateAsapSent = bSendEmail = bLimitMax = bRetransNonAsap = False
    elif reconType == R_TYPE_IGNORE_MAX:
        bLimitMax = False
    elif reconType == R_TYPE_EXCLUDE_NON_ASAP:
        bRetransNonAsap = False
    elif reconType == R_TYPE_EXCLUDE_NON_ASAP_AND_MAX:
        bLimitMax = bRetransNonAsap = False
    elif reconType == R_TYPE_REMOVE_RECONCILED:
        # marks matching as reconciled in ASAP and Non ASAP and creates a new recon and sent list with the missing values
        # that way we can rerun the prior days without breaking today's files
        bConsolidate = bCreateAsapSent = bSendEmail = bLimitMax = bRetransNonAsap = bProcessNonTransmitted = False
    elif reconType == R_TYPE_GET_RECON_ONLY:
        bRunRecon = bConsolidate = bCreateAsapSent = bSendEmail = bLimitMax = bRetransNonAsap = bProcessNonTransmitted = bResetAccordStatus = False
    elif reconType == R_TYPE_REMOVE_RECONCILED_NOASAP:
        bConsolidate = bCreateAsapSent = bSendEmail = bLimitMax = bRetransNonAsap = bProcessNonTransmitted = bResetAccordStatus = False

    if reconDate != TODAY:
        bGetRecon = bRunRecon = bConsolidate = False

    if devState.isDevInstance():
        bGetRecon = False

    return bGetRecon, bRunRecon, bConsolidate, bCreateAsapSent, bSendEmail, bLimitMax, bRetransNonAsap, bProcessNonTransmitted, bResetAccordStatus


def BANRecon(logger, reconDate=TODAY, reconType=R_TYPE_NORMAL, logLevel=20):
    """
    Build recon file to transmit to Banner.
    """
    reconFiles = []
    if reconType != R_TYPE_NORMAL:
        logger.info('running special process type: {reconType!s:s}'.format(reconType=reconType))
    bGetRecon, bRunRecon, bConsolidate, bCreateAsapSent, bSendEmail, bLimitMax, bRetransNonAsap, bProcessNonTransmitted, bResetAccordStatus = getMethodology(reconType, reconDate)

    logger.setLevel(logLevel)

    # In production, download the recon file that we have received from Banner
    nonAsapData = []
    moveSentToProc = False
    if bGetRecon:
        getReconFile(logger)
    if bRunRecon:
        # if we need to run the process including files from a prior day then we only want to build the sent list from that day
        # we still need to rerun after that day for reconciliation to occur
        runReconciliation(logger, bLimitMax, bRetransNonAsap, bProcessNonTransmitted, bResetAccordStatus, reconDate)

    if bConsolidate:
        reconFiles = glob.glob(os.path.join(TRACKING_DIR, '*.dat'))
        for reconFile in reconFiles:
            nonAsapData.extend(getFileData(reconFile))
        moveSentToProc = True
    # allow for us to create a sent list without the asap (as that will be generated later at normal run time
    # need to manually remove any records from the recon that did not originate from before today.

    if bCreateAsapSent:
        detailList, sentData = getAsapNeedingReconciliation(logger, reconDate)
        sentData.extend(nonAsapData)

        if detailList:
            build_BAN_ASAP_report(detailList,
                                  reportFile=os.path.join(OUTBOX_DIR, 'Banner_WPenn_ASAPDailyList_{reconDate:%d%b%Y}.xls'.format(reconDate=reconDate)),
                                  sTitle="CRL - Banner Life and William Penn ASAP Transmissions for {reconDate:%d-%b-%Y}".format(reconDate=reconDate))

        reconFile = createReconFile(reconDate, sentData)
        if bSendEmail:
            SendReconEmail(logger, reconFile, reconDate)
    elif nonAsapData:
        createReconFile(reconDate, nonAsapData)

    if moveSentToProc:
        moveToProc(reconFiles)

    if not devState.isDevInstance():
        cleanup(logger)


def getFileData(filename, upper=False):
    if os.path.exists(filename):
        fn = open(filename, 'r')
        try:
            data = [elem.strip().replace('"', '') for elem in fn.readlines() if elem.strip() and elem.strip().replace('"', '')]
            if upper:
                data = [elem.upper() for elem in data]
            return data
        finally:
            fn.close()
    else:
        return []


def createReconFile(reconDate, rawdata):
    if rawdata:
        reconFile = os.path.join(WORK_DIR, SENTLIST.format(reconDate.strftime('%Y%m%d%H%M%S')))
        with open(reconFile, 'w') as reconPtr:
            reconPtr.write('\n'.join(rawdata))
        logger.info('Recon file {reconFile!s:s} written.'.format(reconFile=reconFile))
        return reconFile
    else:
        return None


def SendReconEmail(logger, reconFile, reconDate):
    if reconFile:
        logger.info('sending recon email')
        # send email to Johnny Maddox at Banner with attached recon file
        sMessage = ""
        sMessage += "Please find attached today's reconciliation file listing the "
        sMessage += "document image files transmitted by CRL to Banner Life on this day."
        sSubject = "CRL - Banner Life Reconciliation File for {reconDate:%d-%b-%Y}".format(reconDate=reconDate)
        CRLUtility.CRLSendEMail(RECIPIENTS, sMessage, sSubject,
                                RECIPIENTS, '',
                                RECIPIENTS, reconFile)
        # Copy the recon File in the CRL_Recon folder


def getAsapNeedingReconciliation(logger, reconDate):
    logger.info('getting ASAP Needing Reconciliation for: {reconDate!s:s}'.format(reconDate=reconDate))
    config = ASAP_UTILITY.getXmitConfig()
    docFactory = ASAP_UTILITY.getDocumentFactory()
    docHistory = ASAP_UTILITY.getDocumentHistory()
    reconContact = config.getContact('BAN', 'SLQ', 'APPS')
    if reconContact:
        rawdata = []
        sQuery = '''
            select contact_id, max(actiondate), sid, last_name, first_name, documentid
            from {table!s:s} with (nolock)
            inner join ils_qc..casemaster with (nolock) on sid = sampleid
            where contact_id in ('banslqapps','wmpslqapps','banreinsmm','wmpreinsmm')
            and actionitem = '{action_xmit!s:s}'
            and actiondate between '{reconDate:%d-%b-%Y} 00:00' and '{reconDate:%d-%b-%Y} 15:00'
            group by contact_id, sid, last_name, first_name, documentid
            order by contact_id, max(actiondate)
            '''.format(table=docHistory.TABLE_DOCUMENT_HISTORY,
                       action_xmit=docHistory.ACTION_TRANSMIT,
                       reconDate=reconDate)
        cursor = config.getCursor(config.DB_NAME_XMIT)
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        detailList = []
        if recs:
            logger.info('found {count:d} asap records needing reconciliation'.format(count=len(recs)))
            docList = []
            detailMap = {}
            for contact_id, xmitDate, sid, lname, fname, docid in recs:
                doc = docFactory.fromDocumentId(docid)
                if doc:
                    docList.append(doc)
                    detailMap[(sid, docid)] = (contact_id, xmitDate, lname, fname)
            caseFact = ASAP_UTILITY.getCaseFactory()
            cases = caseFact.casesForDocuments(docList)
            logger.info('found {count:d} distinct cases'.format(count=len(cases)))
            for case in cases:
                docList = case.getDocuments().values()
                appDocList = docList
                for doc in docList:
                    if reconContact.docTypeNameMap[doc.getDocTypeName()] == 'APPI':
                        appDocList = [doc]
                        break
                for doc in appDocList:
                    rawdata.append(doc.fileName)
                    logger.info('asap missing reconciliation: {fileName!s:s}'.format(fileName=doc.fileName))
                    contact_id, xmitDate, lname, fname = detailMap[(case.sid,
                                                                    doc.getDocumentId())]
                    detailList.append((contact_id, xmitDate, doc.fileName,
                                       case.sid, lname, fname))
        else:
            logger.info('no asap records found needing reconciliation')
        return detailList, rawdata
    else:
        logger.error('Recon contact not configured.')
        return None, []


def moveToProc(filenames, today=TODAY):
    CRLUtility.CRLZIPFiles(filenames, os.path.join(PROCESSED_DIR, today.strftime('%Y%m%d%H%M%S.zip')))
    # for filename in filenames:
    #     if os.path.exists(filename):
    #         CRLUtility.CRLMoveFile(filename, os.path.join(PROCESSED_DIR, os.path.basename(filename) + today.strftime('_%')))


def getArgs(args=sys.argv[1:]):
    retArgs = {
        'reconDate': TODAY,
        'reconType': R_TYPE_NORMAL,
        'logLevel': 20
    }
    try:
        for arg in args:
            if arg == 'recon':
                pass
            elif arg.upper() in R_TYPES_AVAILABLE:
                retArgs['reconType'] = arg.upper()
            elif arg.upper() == 'DEBUG':
                retArgs['logLevel'] = 10
            else:
                dateTuple = re.compile('(\d{4})[-/](\d{2})[-/](\d{2})').findall(arg)
                if dateTuple:
                    retArgs['reconDate'] = datetime.datetime(*[int(elem) for elem in dateTuple[0]])
                else:
                    raise ValueError('invalid option')
        return retArgs
    except Exception:
        msg = ''
        msg += 'error parsing arguments, valid arguments (in any order) are as follows: recon [option type below] [YYYY-MM-DD, YYYY/MM/DD] DEBUG'
        msg += 'DEBUG - set log level to debug (be careful as this adds a large amount of logging and can fill up more then 1.5 log files for each run)'
        msg += 'recon - ignored but left in to allow for normal custom.py script methodology of passing in recon for recon processes'
        msg += 'date in format YYYY-MM-DD or YYYY/MM/DD re re-recon an older date'
        msg += ' the date applies to the sent list created for ASAP based on '
        msg += '---------------option_types------------'
        msg += 'GET_RECON_ONLY - retrieves recon file only'
        msg += 'REMOVE_RECONCILED - compares sent list and recon files then creates manual sent list and recon with the extra files and missing, this is as close to a rerun as we can do'
        msg += 'REMOVE_RECONCILED_NOASAP - same as REMOVE_RECONCILED except for it does not update acord statuses (this should be used if you plan on rerunning the process for these files)'
        msg += 'EXCLUDE_NON_ASAP_AND_MAX - processes sent and recon excluding the maximum unmatching and not restaging non asap work'
        msg += 'EXCLUDE_NON_ASAP - process sent and recon for asap processes only'
        msg += 'IGNORE_MAX - processes set and recon ignoring the maximum unmatched restriction'
        msg += 'CREATE_SENT_ONLY - Consolidates the .dat files and creates a sent file without asap transmission lists'
        msg += 'NORMAL - processes sent and recon with all restrictions and restransmissions'
        logger.exception(msg)
        raise


def cleanup(logger):
    for cleanDir in DIRS_TO_CLEAN:
        try:
            CRLUtility.CRLDeleteOldFiles(os.path.join(cleanDir, '*.*'))
        except Exception:
            logger.warn('unable to remove old files in {cleanDir!s:s}'.format(cleanDir=cleanDir))


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        args = getArgs()
        BANRecon(logger, **args)
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except Exception:
        logger.exception('Error')
