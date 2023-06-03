"""

  Facility:         ILS

  Module Name:      SVBCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2013, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for SVB for ASAP processing.
      For Phase I, allow APPS to QC and release case, but case will not
          be transmitted to SVB. Instead, documents would be viewed via WebOasis.

      For Phase II, build lab reports, images, and indexes in ASAP. But place files in
      folder \\ilsdfs\sys$\XMIT\FTP\SVB\Work so the files will be picked up by SVB_Imaging.py
      - there are no 103's for SVB, so index fields will be pulled from the 121 instead of the 103
      - include lab report image and index if LIMS sample has reported, and this is a full
        case transmit, or a partial case transmit with a labslip
      - script ASAP_AnalyzeUpdatedLabReports will be run daily outside of ASAP_Main to
        restage cases with updated lab reports so they can be transmitted via SVBCustom
      - add prefix to tif/idx files
        D (client docs)
        L (lab report)
        C (consent)
      - SUBJECT is TEST if dev

  Author:
      Robin Underwood

  Creation Date:
      25-JUN-2013

  Modification History:
      25-JUN-2013     rsu   Ticket 41520
         No transmission for phase 1
      16-AUG-2013     rsu   Ticket 41796
         For Phase II, process images, indexes, and lab report files in ASAP. Then place the
         files where they will get picked up by the SVB_Imaging.py feed.
      14-MAY-2021 nelsonj
         Migration to new apphub and updating to python 2.7
"""

from .IndexHandler import ASAPIndexHandler
from .TransmitHandler import ASAPTransmitHandler
from .Utility import ASAP_UTILITY
import CRLUtility
import datetime
import glob
import os
import sys
import time

if ASAP_UTILITY.devState.isDevInstance():
    EMAIL_ADDRESS = 'nelsonj@crlcorp.com'
    SVB_TRANSMISSION_DIR = r'\\ilsdfs\sys$\XMIT\FTP\SVB\Test\Work'
else:
    EMAIL_ADDRESS = 'ilsprod@crlcorp.com'
    SVB_TRANSMISSION_DIR = r'\\ilsdfs\sys$\XMIT\FTP\SVB\Work'


class SVBIndexHandler(ASAPIndexHandler):
    """
    Custom handler for building indexes for SVB.
    """

    def _isReadyToIndex(self):
        """
        For full transmits, do not build indexes until the lab report is ready.
        """
        fReady = False
        if self._isFullTransmit():
            case = self._getCase()
            if self.isReadyToBuildLabReport(case):
                fReady = True
        else:
            fReady = True

        return fReady

    def _processDerivedFields(self):
        if ASAP_UTILITY.devState.isDevInstance():
            case = self._getCase()
            case.contact.index.setValue('SUBJECT', 'TEST')

        return True

    def _postProcessIndex(self):
        """
        Build image and index for SVB lab report.
        Must be a full transmit or a partial transmit with a labslip. And the LIMS
        sample must have transmitted.
        """

        case = self._getCase()

        try:
            if not self.isReadyToBuildLabReport(case):
                # no need to build lab report and index
                return True

            # build image from text report
            xmitDir = case.contact.xmit_dir
            txtReport = os.path.join(os.path.dirname(xmitDir), 'reports', '{sid!s:s}.txt'.format(sid=case.sid))
            CRLUtility.CRLOasisTextReportForSid(case.sid, 'ILS', txtReport)
            s = glob.glob(txtReport[:-4] + '.*')
            if not s:
                self._getLogger().warn('Failed to build lab report image for sid {sid!s:s}'.format(sid=case.sid))
                return False

            svbFolderMap = {
                'svbslqapps': 'SVB_SLQ'
            }

            fError, pageCount = self.buildLabReport(case.sid, svbFolderMap.get(case.contact.contact_id))

            if fError:
                self._getLogger().warn('Unable to process lab report for {sid!s:s}: '.format(sid=case.sid), exc_info=True)

            self.buildLabReportIndex(case, pageCount)

            # remove txt lab report from reports/processed folder
            txtReport = os.path.join(os.path.join(os.path.dirname(xmitDir), 'reports'), 'processed', '{sid!s:s}.txt'.format(sid=case.sid))
            s = glob.glob(txtReport)
            if s:
                CRLUtility.CRLDeleteFile(txtReport)

        except:
            self._getLogger().warn(
                'PostProcessIndex failed with exception: ', exc_info=True)

        return True

    def isReadyToBuildLabReport(self, case):
        """
        Check whether lab report is needed for this case transmission.
        Must be full transmit or partial transmit with a labslip. And the LIMS
        sample must have a transmit date.
        """

        rec = ASAP_UTILITY.getLIMSSampleFieldsForSid(case.sid, ('transmit_date',))
        if not rec:
            # no sid in LIMS, so no lab report
            self._getLogger().info('SVB Sid not in LIMS {sid!s:s}'.format(sid=case.sid))
            return False
        else:
            sid, transmit_date = rec
            self._getLogger().info('SVB Sid {sid!s:s} Transmit date {transmit_date!s:s}'.format(sid=case.sid, transmit_date=str(transmit_date)))
            if not transmit_date:
                # sid has not transmitted yet
                self._getLogger().info('SVB No transmit date Sid {sid!s:s}'.format(sid=case.sid))
                return False

        if self._isFullTransmit():
            self._getLogger().info('SVB is full transmit')
            return True

        docList = list(case.getDocuments().values())
        for doc in docList:
            if doc.getDocTypeName() == 'LAB RECEIPT/URINE/BLOOD TEST':
                self._getLogger().info('SVB not full transmit but has lab report')
                # lab slip is getting transmitted, so also transmit lab report
                return True

        return False

    def buildLabReport(self, sid, SVBFolder):
        """
        Convert txt file to image file.
        """
        fError = False

        # constants that differ between prod and dev
        sTest = ''
        if ASAP_UTILITY.devState.isDevInstance():
            sTest = 'test'

        SVB_IMAGING_DIR = os.path.join(r'\\ilsdfs\sys$\xmit\ftp', SVBFolder, sTest, 'imaging')

        reportStagingPath = os.path.join(SVB_IMAGING_DIR, 'reports')
        processedPath = os.path.join(reportStagingPath, 'processed')
        errorPath = os.path.join(reportStagingPath, 'error')

        rptFiles = glob.glob(os.path.join(reportStagingPath, '{sid!s:s}.txt'.format(sid=sid)))
        pageCount = 0
        for rptFile in rptFiles:
            fMoveToError = False
            sidText = os.path.splitext(os.path.basename(rptFile))[0]
            tif = os.path.join(reportStagingPath, sidText + '.tif')

            # reduce font size to avoid blank pages in the lab report
            pageCount, exitCode = CRLUtility.CRLTextToTiffWithPageCount(rptFile, tif, sCanvas='8.5/11/200', sFontSize='10.7b')

            if os.path.isfile(tif):
                # now move files to processed folder
                CRLUtility.CRLCopyFile(rptFile,
                                       os.path.join(processedPath,
                                                    os.path.basename(rptFile)),
                                       True, 5)
            else:
                fError = True
                fMoveToError = True
                self._getLogger().warn('Failed to create TIF image for text report {rptFile!s:s}.'.format(rptFile=rptFile))

            if fMoveToError:
                CRLUtility.CRLCopyFile(rptFile,
                                       os.path.join(errorPath, os.path.basename(rptFile)),
                                       True, 5)

                # all report files should have been processed, so if any are left, they are unmatched
        rptFiles = glob.glob(os.path.join(reportStagingPath, '*.txt'))
        if rptFiles:
            fError = True
            self._getLogger().warn('There are {rptFiles:d} unconverted report files in {reportStagingPath!s:s}.'.format(rptFiles=len(rptFiles), reportStagingPath=reportStagingPath))

        return (fError, pageCount)

    def buildLabReportIndex(self, case, pageCount):
        """
        Build the index for the lab report using another index file for the case as a model
        """

        doctypefield = 'REQUIRE'
        pagenumfield = 'PAGES'
        labworkdoctype = 'HOSMAC'

        idxPaths = self._getIndexPaths()
        if idxPaths and os.path.isfile(idxPaths[0]):
            idxFile = idxPaths[0]
            ptr = open(idxFile, 'r')
            lines = ptr.readlines()
            ptr.close()

            # this section of code to build the fieldlist must be done because
            # some field values might have commas in them, and they need to be
            # identified and joined with their erroneously separated prior field
            fieldlist = []
            for line in lines:
                line = line.strip()
                if (line.find(doctypefield) < 0 and line.find(pagenumfield) < 0):
                    fieldlist.append(line)
                elif line.find(doctypefield) >= 0:
                    # change doctype to labwork
                    fieldlist.append(doctypefield + "=" + labworkdoctype)
                elif line.find(pagenumfield) >= 0:
                    # update correct number of pages
                    if not pageCount or pageCount < 1:
                        pageCount = "1"
                    else:
                        pageCount = str(pageCount)

                    fieldlist.append(pagenumfield + "=" + pageCount)

            # write index file for lab report
            idxFile = os.path.join(case.contact.index_dir, '{sid!s:s}.idx'.format(sid=case.sid))
            ptr = open(idxFile, 'w')
            for line in fieldlist:
                ptr.write("{line!s:s}\n".format(line=line))

            ptr.close()


class SVBTransmitHandler(ASAPTransmitHandler):
    """
    Custom handler for SVB transmission.
    """

    def _preStage(self):
        fSuccess = True
        contact = self._getContact()
        xmitStagingPath = contact.xmit_dir
        xmitPgpPath = os.path.join(xmitStagingPath, 'pgp')
        reviewPath = os.path.join(xmitStagingPath, 'review')
        retransPath = os.path.join(xmitStagingPath, 'retrans')
        fm = ASAP_UTILITY.getASAPFileManager(contact)

        try:
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

    def _isIndexedCaseReady(self):
        fReady = False

        fact = ASAP_UTILITY.getViableCaseFactory()
        case = fact.fromSid(self._getCurrentCase().sid)
        if case and case.sample and case.sample.transmitDate:
            # only transmit if lab report is ready
            fReady = True

        return fReady

    def _stageIndexedCase(self):
        case = self._getCurrentCase()
        fSuccess = True
        fromToMoves = []

        # now try to get doc/index pairs
        documents = list(case.getDocuments().values())
        xmitConfig = ASAP_UTILITY.getXmitConfig()
        processedSubdir = xmitConfig.getSetting(xmitConfig.SETTING_PROCESSED_SUBDIR)
        for doc in documents:
            docPrefix = doc.fileName.split('.')[0]
            if doc.getDocTypeName() == 'LAB RECEIPT/URINE/BLOOD TEST':
                svbDocPrefix = 'C{docPrefix!s:s}'.format(docPrefix=docPrefix)  # labslip
            else:
                svbDocPrefix = 'D{docPrefix!s:s}'.format(docPrefix=docPrefix)  # qc docs

            docPath = os.path.join(case.contact.document_dir,
                                   processedSubdir,
                                   doc.fileName)
            idxPath = os.path.join(case.contact.index_dir, '{docPrefix!s:s}.IDX'.format(docPrefix=docPrefix))
            if os.path.isfile(docPath) and os.path.isfile(idxPath):
                xmitDocPath = os.path.join(case.contact.xmit_dir,
                                           '{svbDocPrefix!s:s}.tif'.format(svbDocPrefix=svbDocPrefix))
                xmitIdxPath = os.path.join(case.contact.xmit_dir,
                                           '{svbDocPrefix!s:s}.idx'.format(svbDocPrefix=svbDocPrefix))
                fromToMoves.append((docPath, xmitDocPath))
                fromToMoves.append((idxPath, xmitIdxPath))
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Failed to find matching index/image pair for docid {documentId!s:s} (sid {sid!s:s}).'
                    .format(documentId=doc.getDocumentId(), sid=case.sid))

        # move lab report and index
        svbLabRptDocPrefix = 'L{sid!s:s}'.format(sid=case.sid)
        if os.path.isfile(os.path.join(os.path.dirname(case.contact.xmit_dir), 'reports', '{sid!s:s}.tif'.format(sid=case.sid))):
            fromToMoves.append((os.path.join(os.path.dirname(case.contact.xmit_dir),
                                             'reports', '{sid!s:s}.tif'.format(sid=case.sid)),
                                os.path.join(case.contact.xmit_dir, '{svbLabRptDocPrefix!s:s}.tif'.format(svbLabRptDocPrefix=svbLabRptDocPrefix))))
            fromToMoves.append((os.path.join(os.path.dirname(case.contact.xmit_dir),
                                             'indexes', '{sid!s:s}.idx'.format(sid=case.sid)),
                                os.path.join(case.contact.xmit_dir, '{svbLabRptDocPrefix!s:s}.idx'.format(svbLabRptDocPrefix=svbLabRptDocPrefix))))

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
                'There are {asapToXmitFiles:d} files in the transmit staging folder to process...'
                .format(asapToXmitFiles=len(asapToXmitFiles)))
            today = datetime.datetime.today()

            # Move the files to the SVB regular Image transmission
            for asapFile in asapToXmitFiles:
                try:
                    destFile = os.path.join(SVB_TRANSMISSION_DIR, os.path.basename(asapFile.getFullPath()).upper().replace('NDX', 'INI'))
                    logger.debug('DestFile' + destFile)
                    # Copy the files to the SVB imaging transmission
                    CRLUtility.CRLCopyFile(asapFile.getFullPath(), destFile)
                    self._getLogger().info('Copied {fullPath!s:s} file to SVB Work Folder...'.format(fullPath=asapFile.getFullPath()))

                    # Zip the files and store in sent location for records
                    filePath = asapFile.getFullPath()
                    logger.debug(filePath)
                    zipFileName = 'CRLSVBSLQ{today:%Y%m%d%H%M%S}.zip'.format(today=today)
                    zipFilePath = os.path.join(xmitSentPath, zipFileName)
                    CRLUtility.CRLAddToZIPFile(asapFile.getFullPath(), zipFilePath, True)
                except:
                    fSuccess = False
                    destFile = os.path.join(retransPath, os.path.basename(asapFile.getFullPath()))
                    CRLUtility.CRLCopyFile(asapFile.getFullPath(), destFile, True)
                    self._getLogger().info('Moved {fullPath!s:s} file to SVB Work Folder...'.format(fullPath=asapFile.getFullPath()))
        return fSuccess


def SVBRecon():
    """
    Reconciliation of SVB documents will be handled in SVB_Imaging.py
    """


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    begintime = time.time()
    try:
        arg = ''

        if len(sys.argv) > 1:
            arg = sys.argv[1]

        # Recon is perform in SVB_Imaging.py
        if arg == 'recon':
            SVBRecon()
        else:
            logger.warn('Argument(s) not valid. Valid arguments:')
            logger.warn('recon')
        logger.info('Time to process this pass was {elapsed:5.3f} seconds.'
                    .format(elapsed=(time.time() - begintime)))
    except:
        logger.exception('Error')
