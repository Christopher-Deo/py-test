"""

  Facility:         ILS

  Module Name:      MOOCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2012, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for MOO for ASAP processing.

  Author:
      Komal Gandhi

  Creation Date:
      5-Apr-2013

  Modification History:
      3-Jun-2013    Ticket 41238
      Changed [Image data] to [Image Data]

      14-MAY-2021 nelsonj
      Migration to new apphub and updating to python 2.7

"""
from __future__ import division, absolute_import, with_statement, print_function
from ILS.ASAP.Utility import ASAP_UTILITY
from ILS.ASAP.IndexHandler import ASAPIndexHandler
from ILS.ASAP.TransmitHandler import ASAPTransmitHandler
import CRLUtility
import datetime
import glob
import os
import time

if ASAP_UTILITY.devState.isDevInstance():
    EMAIL_ADDRESS = 'nelsonj@crlcorp.com'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'


class MOOIndexHandler(ASAPIndexHandler):
    """
    Custom handler for building indexes for MOO.
    """

    def _postProcessIndex(self):
        print('processing indexes')
        idxPaths = self._getIndexPaths()
        for idxPath in idxPaths:
            # Add [Image data] as the first line
            print('Adding Image Data')
            idxLines = ['[Image Data]\n']
            with open(idxPath, 'r') as ptr:
                lines = ptr.readlines()
            for line in lines:
                idxLines.append(line)

            with open(idxPath, 'w') as ptr:
                ptr.writelines(idxLines)
        return True


class MOOTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for MOO transmission.
    """

    def _preStage(self):
        fSuccess = True
        print('In MOO Module')
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitPgpPath = os.path.join(xmitStagingPath, 'pgp')
        reviewPath = os.path.join(xmitStagingPath, 'review')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        try:
            # if there are any files here, they were left behind when a previous
            # run failed to complete, so move them to the reviewPath folder
            toXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
            for xmitFile in toXmitFiles:
                CRLUtility.CRLCopyFile(xmitFile.getFullPath(),
                                       os.path.join(reviewPath, xmitFile.fileName),
                                       True, 5)
            if len(toXmitFiles) > 0:
                self._getLogger().error(
                    'Files were left behind in ' +
                    '{xmitStagingPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitStagingPath=xmitStagingPath))
            # move any PGP-encrypted zip files to reviewPath folder
            pgpFiles = fm.glob(os.path.join(xmitPgpPath, '*.*'))
            for pgpFile in pgpFiles:
                CRLUtility.CRLCopyFile(pgpFile.getFullPath(),
                                       os.path.join(reviewPath, pgpFile.fileName),
                                       True, 5)
            if len(pgpFiles) > 0:
                self._getLogger().error(
                    'PGP-encrypted zip files were left behind in ' +
                    '{xmitPgpPath!s:s} from a previous run and have been moved to the review subfolder.'
                    .format(xmitPgpPath=xmitPgpPath))
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
        # now try to get doc/index pairs
        documents = case.getDocuments().values()
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '{docPrefix!s:s}.IDX'.format(docPrefix=docPrefix))
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir,
                                           'D{docPrefix!s:s}.tif'.format(docPrefix=docPrefix))
                xmitIdxPath = os.path.join(case.contact.xmit_dir,
                                           'D{docPrefix!s:s}.ndx'.format(docPrefix=docPrefix))
                fromToMoves.append((docPath, xmitDocPath))
                fromToMoves.append((idxPath, xmitIdxPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid {docid:d} (sid {sid!s:s}).'
                    .format(docid=doc.getDocumentId(), sid=case.sid))
        if fSuccess:
            for fromPath, toPath in fromToMoves:
                CRLUtility.CRLCopyFile(fromPath, toPath, True, 5)
        return fSuccess

    def _transmitStagedCases(self):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitSentPath = os.path.join(xmitStagingPath, 'sent')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)
        asapToXmitFiles = fm.glob(os.path.join(xmitStagingPath, '*.*'))
        if (len(asapToXmitFiles) > 0):
            self._getLogger().info(
                'There are {count:d} files in the transmit staging folder to process...'
                .format(count=len(asapToXmitFiles)))
            today = datetime.datetime.today()
            # Move the files to the Genworth regular Image transmission
            if ASAP_UTILITY.devState.isDevInstance():
                work_path = r'\\ILSDFS\sys$\xmit\FTP\MOO_IPS\test\Work'
            else:
                work_path = r'\\ILSDFS\sys$\xmit\FTP\MOO_IPS\prod\Work'
            for asapFile in asapToXmitFiles:
                try:
                    destFile = os.path.join(work_path, os.path.basename(asapFile.getFullPath()).upper().replace('NDX', 'INI'))
                    print('DestFile' + destFile)
                    # Copy the files to the MOO IPS transmission
                    CRLUtility.CRLCopyFile(asapFile.getFullPath(), destFile)
                    self._getLogger().info('Copied {path!s:s} file to MOO_IPS Work Folder...'.format(path=asapFile.getFullPath()))

                    # Zip the files and store in sent location for records
                    filePath = asapFile.getFullPath()
                    print(filePath)
                    if filePath.find('MOO_SLQ') > -1:
                        zipFileName = 'CRLMOOSLQ{dt:%Y%m%d%H%M%S}.zip'.format(dt=today)
                    elif filePath.find('MOO_CTRL') > -1:
                        zipFileName = 'CRLMOOCTRL{dt:%Y%m%d%H%M%S}.zip'.format(dt=today)
                    else:
                        raise ValueError('Path {filePath!s:s} does not match expected value'
                                         .format(filePath=filePath))
                    zipFilePath = os.path.join(xmitSentPath, zipFileName)
                    CRLUtility.CRLAddToZIPFile(asapFile.getFullPath(), zipFilePath, True)
                except:
                    fSuccess = False
                    destFile = os.path.join(retransPath, os.path.basename(asapFile.getFullPath()))
                    CRLUtility.CRLCopyFile(asapFile.getFullPath(), destFile, True)
                    self._getLogger().info('Moved {path!s:s} file to Mutual Of Omaha Retrans Folder...'.format(path=asapFile.getFullPath()))
        return fSuccess


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        print('In MOO Module')
        arg = ''
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error')
