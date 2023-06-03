"""

  Facility:         ILS

  Module Name:      ImageFactory

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPImageFactory class.

  Author:
      Jarrod Wild

  Creation Date:
      07-Nov-2006

  Modification History:

      21-Oct-2015   amw
        Updated for Imaging System Update

      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import CRLUtility
import os
import time
from ILS.ASAP.Utility import ASAP_UTILITY
from ILS.ASAP.Document import ASAPDocument
from ILS import imageaccess

TEMP_DIR = r'ImageFactoryTemp'


class ASAPImageFactory(object):
    """
    This is a factory class for pulling/building TIFF images from
    Delta for ASAP.
    """

    def __init__(self):
        self.__logger = CRLUtility.CRLGetLogger()
        self.__document = ASAPDocument()

    def _buildImage(self, pageFileName, pageValues, stagingDir, tempDir, outDir):
        fSuccess = False
        if pageFileName and pageValues:
            destPath = os.path.join(stagingDir, pageFileName)
            fError = False
            firstPage = True
            for pageBinary in pageValues:
                if firstPage:
                    fn = open(destPath, 'wb')
                    fn.write(pageBinary)
                    fn.close()
                    firstPage = False
                else:
                    temp_file = ''
                    try:
                        temp_file = os.path.join(tempDir, os.path.basename(destPath))
                        fn = open(temp_file, 'wb')
                        fn.write(pageBinary)
                        fn.close()
                        iRet = CRLUtility.CRLAppendTiff(temp_file, destPath)
                        if iRet != 0:
                            self.__logger.warn('Append tiff ({temp_file!s:s} to {destPath!s:s}) failed with code = {iRet:d}.'.format(temp_file=temp_file, destPath=destPath, iRet=iRet))
                            fError = True
                    except Exception as exc:
                        self.__logger.warn('Failed to run append tiff program:')
                        self.__logger.warn(exc)
                        fError = True
                    try:
                        if temp_file:
                            os.remove(temp_file)
                    except Exception:
                        self.__logger.warn('unable to remove temp file: {temp_file!s:s}'
                                           .format(temp_file=temp_file))
                if fError:  # or ASAP_UTILITY.devState.isDevInstance():  # add this in to run on local machine (no TIFFDLL)
                    break
            if not fError:
                CRLUtility.CRLCopyFile(destPath,
                                       os.path.join(outDir, pageFileName),
                                       True, 5)
                self.__document.fileName = pageFileName
                fSuccess = True
            elif os.path.exists(destPath):
                CRLUtility.CRLDeleteFile(destPath)

        return fSuccess

    def _getDirs(self):
        """
        returns buildDir: directory to hold image as it is being built
                tempBuildDir: directory to hold an individual page before adding it to the image being built
                contactDir: directory where the image will go if built successfully
        :rtype: tuple[str, str, str]
        """
        config = ASAP_UTILITY.getXmitConfig()
        contact = self.__document.case.contact
        buildSubdir = config.getSetting(config.SETTING_BUILD_SUBDIR)
        buildPath = os.path.join(contact.document_dir, buildSubdir)
        if not os.path.exists(buildPath):
            os.mkdir(buildPath)

        tempBuildDir = os.path.join(buildPath, TEMP_DIR)
        if not os.path.exists(tempBuildDir):
            os.mkdir(tempBuildDir)
        return buildPath, tempBuildDir, contact.document_dir

    def _getImages(self):
        """
        returns first page filename
                [list of file binaries]
        :rtype: tuple[str|None, list]
        """
        docList = imageaccess.getDocuments(documentId=str(self.__document.getDocumentId()),
                                           drawerName='ILS_QC',
                                           applicationId='ILSPYTHONIMAGEACCESS')
        # print(docList)
        if docList:
            return docList[0][2][0][0], [elem[1] for elem in docList[0][2] if elem]
        else:
            return None, []

    def _buildDocument(self):
        try:
            fileName, docBinaries = self._getImages()
        except Exception as e:
            self.__logger.warn('unable to retrieve docs from imageaccess: {err!s:s}'.format(err=str(e)))
            return False
        if fileName:
            buildPath, tempBuildDir, contactDir = self._getDirs()
            fSuccess = self._buildImage(pageFileName=fileName,
                                        pageValues=docBinaries,
                                        stagingDir=buildPath,
                                        tempDir=tempBuildDir,
                                        outDir=contactDir)
        else:
            self.__logger.warn('No page records found for document (docid {docid:d}).'
                               .format(docid=self.__document.getDocumentId()))
            fSuccess = False
        return fSuccess

    def fromDocument(self, asapDocument):
        """
        Given a fully qualified ASAPDocument object (with the related
        ASAPCase and ASAPContact linked), build the multi-page TIFF
        image and place in the contact-specific image staging location.

        :param ASAPDocument asapDocument:
        :returns: bool indicating success
        :rtype: bool
        """
        fSuccess = False
        self.__document = asapDocument
        if asapDocument.case:
            begintime = time.time()
            fSuccess = self._buildDocument()
            self.__logger.debug('Time to build image (sid/docid) {sid!s:s}/{docid:d} took {elapsed:0.3f} seconds.'
                                .format(sid=asapDocument.case.sid,
                                        docid=asapDocument.getDocumentId(),
                                        elapsed=(time.time() - begintime)))
        else:
            self.__logger.warn('Document object (docid {docid:d}) is not related to a case.'
                               .format(docid=asapDocument.getDocumentId()))
        return fSuccess


if __name__ == """__main__""":

    testDoc = '10458472'
    sid = '42847595'
    testDir = r'z:\clients\asap'
    logger = CRLUtility.CRLGetLogger(os.path.join(testDir, 'ImageFactory.log'))
    if not os.path.exists(testDir):
        os.mkdir(testDir)


    class TestImageFactory(ASAPImageFactory):
        def _getDirs(self):
            dirsToMake = [os.path.join(testDir, 'build'), os.path.join(testDir, 'temp'), os.path.join(testDir, 'dest')]
            for elem in dirsToMake:
                if not os.path.exists(elem):
                    os.mkdir(elem)
            return dirsToMake


    class tcase(object):
        def __init__(self, sid):
            self.sid = sid

    asapDoc = ASAPDocument()
    asapDoc.setDocumentId(testDoc)
    asapDoc.case = tcase(sid)
    if TestImageFactory().fromDocument(asapDoc):
        print('everything was successful')
    else:
        print('failure')
