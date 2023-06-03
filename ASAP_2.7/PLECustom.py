"""

  Facility:         ILS

  Module Name:      PLECustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2017, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for PLE for ASAP processing.

      - there are 103's for PLE, so index fields will be pulled from the 103
      - PLE wants NDX as extension

  Author:
      Komal Gandhi

  Creation Date:
      29-Aug-2017

  Modification History:
      14-MAY-2021 nelsonj
          Migration to new apphub and updating to python 2.7

"""
from __future__ import division, absolute_import, with_statement, print_function

from ILS.ASAP.Utility import ASAP_UTILITY
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler

import CRLUtility
import glob
import datetime
import ftplib
import os
import sys
import time

if ASAP_UTILITY.devState.isDevInstance():
    EMAIL_ADDRESS = 'nelsonj@crlcorp.com'
    PLE_BASE_DIR = r'\\ilsdfs\sys$\XMIT\FTP\PLE_SLQ\Test\Imaging'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'
    PLE_BASE_DIR = r'\\ilsdfs\sys$\XMIT\FTP\PLE_SLQ\Imaging'


class PLETransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for PLE transmission.
    """

    def _preStage(self):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        reviewPath = os.path.join(xmitStagingPath, 'review')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)

        try:
            toXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
            self._getLogger().info(
                'Files in PLE transmit folder: {toXmitFiles!s:s}'
                .format(toXmitFiles=toXmitFiles))
            for xmitFile in toXmitFiles:
                CRLUtility.CRLCopyFile(xmitFile.getFullPath(),
                                       os.path.join(reviewPath, xmitFile.fileName),
                                       True, 5)
            if len(toXmitFiles) > 0:
                self._getLogger().error(
                    'Files were left behind in ' +
                    '{xmitStagingPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitStagingPath=xmitStagingPath))

            # move any zip files to reviewPath folder
            zipFiles = fm.glob(os.path.join(xmitZipPath, '*.*'))
            for zipFile in zipFiles:
                CRLUtility.CRLCopyFile(zipFile.getFullPath(),
                                       os.path.join(reviewPath, zipFile.fileName),
                                       True, 5)
            if len(zipFiles) > 0:
                self._getLogger().error(
                    'Zip files were left behind in ' +
                    '{xmitZipPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitZipPath=xmitZipPath))
            # now move files from the retrans folder to the xmit staging folder
            toRetransFiles = glob.glob(os.path.join(retransPath, '*.*'))
            for retransFile in toRetransFiles:
                baseFileName = os.path.basename(retransFile)
                CRLUtility.CRLCopyFile(retransFile,
                                       os.path.join(xmitStagingPath, baseFileName),
                                       True, 5)
        except:
            self._getLogger().warn(
                'Pre-stage failed with exception: ', exc_info=True)
            fSuccess = False
        return fSuccess

    def _stageIndexedCase(self):
        case = self._getCurrentCase()
        fSuccess = True
        fromToMoves = []

        # get 103 only if first transmit and acord 103 directory is configured
        if self._isFirstTransmit() and case.contact.acord103_dir:
            acord103Path = os.path.join(case.contact.acord103_dir, '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
            if os.path.isfile(acord103Path):
                pleFileName = os.path.basename(acord103Path)
                proc103Path = os.path.join(case.contact.acord103_dir, 'processed', '{trackingId!s:s}.XML'.format(trackingId=case.trackingId))
                xmit103Path = os.path.join(case.contact.xmit_dir, pleFileName)
                # Copy Acord103 to processed
                CRLUtility.CRLCopyFile(acord103Path, proc103Path, False, 5)
                # Move the Acord103 to to_ple
                CRLUtility.CRLCopyFile(acord103Path, xmit103Path, True, 5)
            else:
                fSuccess = False
                self._getLogger().warn('Failed to find ACORD 103 for case ({sid!s:s}/{trackingId!s:s}).'.format(sid=case.sid, trackingId=case.trackingId))

        # now try to get doc/index pairs
        documents = case.getDocuments().values()
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            if doc.getDocTypeName() == 'LAB RECEIPT/URINE/BLOOD TEST':
                PLEDocPrefix = 'C{docPrefix!s:s}'.format(docPrefix=docPrefix)  # labslip
            else:
                PLEDocPrefix = 'D{docPrefix!s:s}'.format(docPrefix=docPrefix)  # qc docs

            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '{docPrefix!s:s}.IDX'.format(docPrefix=docPrefix))
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir,
                                           '{PLEDocPrefix!s:s}.TIF'.format(PLEDocPrefix=PLEDocPrefix))
                xmitIdxPath = os.path.join(case.contact.xmit_dir,
                                           '{PLEDocPrefix!s:s}.NDX'.format(PLEDocPrefix=PLEDocPrefix))
                fromToMoves.append((docPath, xmitDocPath))
                fromToMoves.append((idxPath, xmitIdxPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid {documentId!s:s} (sid {sid!s:s}).'
                    .format(documentId=doc.getDocumentId(), sid=case.sid))

        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)
        return fSuccess

    def _transmitStagedCases(self):
        fSuccess = True
        today = datetime.datetime.today()
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join(xmitStagingPath, 'zip')
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')

        asapToXmitFiles = glob.glob(os.path.join(xmitStagingPath, '*.*'))
        if len(asapToXmitFiles) > 0:
            try:
                self._getLogger().info(
                    'There are {asapToXmitFiles:d} files in the transmit staging folder to process...'
                    .format(asapToXmitFiles=len(asapToXmitFiles)))
                zipFileName = 'CRLPLE{today:%Y%m%d%H%M%S}.zip'.format(today=today)
                CRLUtility.CRLZIPFiles(os.path.join(xmitStagingPath, '*.*'),
                                       os.path.join(xmitZipPath, zipFileName),
                                       True)
            except:
                fSuccess = False
        # FTP the zip file
        toFTPFiles = glob.glob(os.path.join(xmitZipPath, '*.*'))
        ftpServer = None
        for zipFile in toFTPFiles:
            fileName = os.path.basename(zipFile)
            try:
                if not ftpServer:
                    sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo('CRLCORP2')
                    ftpServer = ftplib.FTP(sServer, sUser, sPassword)
                if ASAP_UTILITY.devState.isDevInstance():
                    serverPath = '/pleftp1/test/ASAP/{fileName!s:s}'.format(fileName=fileName)
                else:
                    serverPath = '/pleftp1/prod/ASAP/{fileName!s:s}'.format(fileName=fileName)
                CRLUtility.CRLFTPPut(ftpServer, zipFile, serverPath, 'b')
                self._getLogger().info('File {zipFile!s:s} successfully uploaded to PacLife East'.format(zipFile=zipFile))
            except:
                fSuccess = False
                self._getLogger().exception('Failed to FTP file {zipFile!s:s} to PacLife East:'.format(zipFile=zipFile))
            if ftpServer:
                ftpServer.close()
        for zipFile in toFTPFiles:
            CRLUtility.CRLCopyFile(zipFile, os.path.join(xmitSentPath, os.path.basename(zipFile)), True, 5)
        # Write all the asapToXmitFiles names in a file that would be sent to PLE for reconciliation
        PLE_Recon_DIR = os.path.join(PLE_BASE_DIR, 'recon')
        reconFile = 'Recon{today:%Y%m%d}.txt'.format(today=today)
        reconFilePath = os.path.join(PLE_Recon_DIR, reconFile)
        if os.path.exists(reconFilePath):
            # Open file and append lines
            ptr = open(reconFilePath, 'a')
        else:
            ptr = open(reconFilePath, 'w')
        # list of files for recon files
        fileList = []
        for eachFile in asapToXmitFiles:
            rec = os.path.basename(eachFile) + ',' + today.strftime('%b %d %Y %H:%M:%S') + '\n'
            fileList.append(rec)
        ptr.writelines(fileList)
        ptr.close()
        return fSuccess


def PLERecon():
    """
    Reconciliation of PLE documents.
    """
    # Transmit today's recon file to PLE, to /pleftp1/prod/ASAP/Recon
    PLE_Recon_DIR = os.path.join(PLE_BASE_DIR, 'recon')
    reconProcessedFolder = os.path.join(PLE_Recon_DIR, 'processed')

    sServer, sUser, sPassword = CRLUtility.CRLGetFTPHostInfo('CRLCORP2')
    ftpServer = ftplib.FTP(sServer, sUser, sPassword)
    crlReconFiles = glob.glob(os.path.join(PLE_Recon_DIR, 'Recon*.txt'))
    for eachFile in crlReconFiles:
        if ASAP_UTILITY.devState.isDevInstance():
            serverPath = '/pleftp1/test/ASAP/Recon/{eachFile!s:s}'.format(eachFile=os.path.basename(eachFile))
        else:
            serverPath = '/pleftp1/prod/ASAP/Recon/{eachFile!s:s}'.format(eachFile=os.path.basename(eachFile))
        CRLUtility.CRLFTPPut(ftpServer, eachFile, serverPath, 'b')
        logger.info('Recon File {eachFile!s:s} successfully uploaded for PacLife East'.format(eachFile=os.path.basename(eachFile)))

    # Process the recon file from PLE
    docFactory = ASAP_UTILITY.getASAPDocumentFactory()
    caseFactory = ASAP_UTILITY.getASAPCaseFactory()
    pleReconFiles = glob.glob(os.path.join(PLE_Recon_DIR, 'CRLPLE_Recon*.txt'))
    for reconFile in pleReconFiles:
        baseFileName = os.path.basename(reconFile)
        filePtr = open(reconFile, 'r')
        dataLines = filePtr.readlines()
        filePtr.close()
        fError = False
        documents = []
        for line in dataLines:
            reconFields = []
            try:
                if line.strip():
                    imageFileName = line.strip().split('.')[0]
                    if not imageFileName.startswith('APPS') and len(imageFileName) == 9:
                        doc = docFactory.fromFileName(imageFileName[1:])
                        if doc:
                            documents.append(doc)
                    else:
                        continue
            except:
                logger.exception('Exception occured')
                logger.warn('Invalid entry found in file {baseFileName!s:s}:\r\n{reconFields!s:s}'.format(baseFileName=baseFileName, reconFields=str(reconFields)))
                fError = True
        cases = caseFactory.casesForDocuments(documents)
        docHistory = ASAP_UTILITY.getASAPDocumentHistory()
        for case in cases:
            for doc in case.getDocuments().values():
                docHistory.trackDocument(doc, docHistory.ACTION_RECONCILE)
        if fError:
            logger.error('There were one or more errors processing file {baseFileName!s:s}.'
                         .format(baseFileName=baseFileName))
    # Move all the files to processed
    ReconFiles = glob.glob(os.path.join(PLE_Recon_DIR, '*.txt'))
    for reconFile in ReconFiles:
        CRLUtility.CRLCopyFile(reconFile, os.path.join(reconProcessedFolder, os.path.basename(reconFile)), True, 5)


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        arg = ''
        begintime = time.time()
        if len(sys.argv) > 1:
            arg = sys.argv[1]

        if arg == 'recon':
            PLERecon()
        else:
            logger.warn('Argument(s) not valid. Valid arguments:')
            logger.warn('recon')

        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error')
