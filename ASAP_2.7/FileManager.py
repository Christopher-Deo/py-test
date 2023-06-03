"""

  Facility:         ILS

  Module Name:      FileManager

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPFileManager class that wraps the asap_file_manager
      table for maintaining state.  There is also the ASAPFile class for wrapping any
      files used by the ASAP ITS.

  Author:
      Jarrod Wild

  Creation Date:
      24-Apr-2010

  Modification History:
      27-Sep-2019   jbn     SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
"""

import base64
import glob
import os

from .Contact import ASAPContact
import CRLUtility


class ASAPFile(object):

    STATE_NULL_STATE = "NULL_STATE"
    STATE_MARKED_FOR_DELETION = "MARKED_FOR_DELETION"

    def __init__(self, asapContact, fileName, fFullPath=False):
        """

        :param ASAPContact asapContact:
        :param str fileName:
        :param bool fFullPath:
        """
        self.fileId = 0
        self.asapContact = asapContact
        self.contactPath = ''
        if fFullPath:
            self.setFullPath(fileName)
        else:
            self.fileName = fileName
        self.state = self.STATE_NULL_STATE

    def setFullPath(self, sFullPath):
        """

        :param str|unicode sFullPath:
        """
        rootPath = "{dirname!s:s}\\".format(dirname=os.path.dirname(self.asapContact.document_dir).upper())
        fileDir = os.path.dirname(sFullPath)
        self.fileName = os.path.basename(sFullPath)
        if fileDir.upper().startswith(rootPath):
            self.contactPath = fileDir[len(rootPath):]

    def getFullPath(self):
        fullPath = ''
        if self.contactPath:
            rootPath = os.path.dirname(self.asapContact.document_dir)
            fullPath = os.path.join(rootPath, self.contactPath, self.fileName)
        return fullPath


class ASAPFileManager(object):

    def __init__(self, asapContact=None):
        """

        :param ASAPContact|None asapContact:
        """
        self.__asapContact = asapContact
        self.__xmitConfig = None
        self.__cursor = None
        # self.__xmitConfig = ASAP_UTILITY.getXmitConfig()
        # self.__cursor = self.__xmitConfig.getCursor(self.__xmitConfig.DB_NAME_XMIT)
        self.__stateMap = {}
        self.refreshStates()

    def getCursor(self):
        if not self.__cursor:
            xmitConfig = self.getXmitConfig()
            self.__cursor = xmitConfig.getCursor(xmitConfig.DB_NAME_XMIT)
        return self.__cursor

    def getXmitConfig(self):
        if not self.__xmitConfig:
            from .Utility import ASAP_UTILITY
            self.__xmitConfig = ASAP_UTILITY.getXmitConfig()
        return self.__xmitConfig

    def refreshStates(self):
        self.__stateMap.clear()
        cursor = self.getCursor()
        cursor.execute('''
            select state_id, state_value
            from asap_file_state with (nolock)
            ''')
        recs = cursor.fetch()
        cursor.rollback()
        for idVal, value in recs:
            self.__stateMap[value] = idVal
        self.__stateMap[ASAPFile.STATE_NULL_STATE] = 0

    def getStateId(self, stateValue):
        """

        :param str stateValue:
        :rtype: int
        """
        if not self.__stateMap:
            self.refreshStates()
        return self.__stateMap.get(stateValue)

    @staticmethod
    def uploadContent(asapFile):
        """

        :param ASAPFile asapFile:
        :return:
        """
        content = ''
        fullPath = asapFile.getFullPath()
        if fullPath and os.path.isfile(fullPath):
            ptr = open(fullPath, 'rb')
            content = ptr.read()
            ptr.close()
        return content

    def newFile(self, fileName, fFullPath=False):
        """

        :param str|unicode fileName:
        :param bool fFullPath:
        :rtype: ASAPFile
        """
        return ASAPFile(self.__asapContact, fileName, fFullPath)

    def addFile(self, asapFile, fUploadContent=False):
        """

        :param ASAPFile asapFile:
        :param bool fUploadContent:
        """
        fileContent = ''
        if fUploadContent:
            fileContent = self.uploadContent(asapFile)
        self.addFileWithContent(asapFile, fileContent)

    def addFileWithContent(self, asapFile, fileContent):
        """

        :param ASAPFile asapFile:
        :param str fileContent:
        """
        if asapFile.asapContact.contact_id == self.__asapContact.contact_id:
            stateId = self.getStateId(asapFile.state)
            if not stateId:
                stateId = 1
            stateId = "{stateId:d}".format(stateId=stateId)
            contact_id = "'{contact_id!s:s}'".format(contact_id=self.__asapContact.contact_id)
            fileName = "'{fileName!s:s}'".format(fileName=asapFile.fileName)
            contactPath = "null"
            if asapFile.contactPath:
                contactPath = "'{contactPath!s:s}'".format(contactPath=asapFile.contactPath)
            content64 = "null"
            if fileContent:
                content64 = "'{fileContent!s:s}'".format(fileContent=base64.encodestring(fileContent))
            fieldList = [stateId, contact_id, fileName, contactPath, content64]
            cursor = self.getCursor()
            cursor.execute('''
                insert into asap_file_manager
                (state_id, contact_id, file_name, contact_path, file_content)
                values ({fieldList!s:s})
                '''.format(fieldList=','.join(fieldList)))
            cursor.commit()
        else:
            raise Exception("ASAPFile contact doesn't match ASAPFileManager contact.")

    def setNullState(self, asapFile):
        """

        :param ASAPFile asapFile:
        """
        asapFile.state = ASAPFile.STATE_NULL_STATE
        cursor = self.getCursor()
        cursor.execute('''
            update asap_file_manager
            set state_id = {stateId:d}
            where id = {fileId:d}
            '''.format(stateId=self.getStateId(asapFile.state),
                       fileId=asapFile.fileId))
        cursor.commit()

    def purgeNullFiles(self):
        cursor = self.getCursor()
        cursor.execute('''
            delete from asap_file_manager
            where state_id = {stateId:d}
            '''.format(stateId=self.getStateId(ASAPFile.STATE_NULL_STATE)))
        cursor.commit()

    def getMarkedForDeletionId(self, asapFile):
        """

        :param ASAPFile asapFile:
        """
        fileId = 0
        if asapFile.asapContact.contact_id == self.__asapContact.contact_id:
            cursor = self.getCursor()
            cursor.execute('''
                select m.id
                from asap_file_manager m with (nolock),
                asap_file_state s with (nolock)
                where m.state_id = s.state_id
                and s.state_value = '{state!s:s}'
                and m.contact_id = '{contact_id!s:s}'
                and m.contact_path = '{contactPath!s:s}'
                and m.file_name = '{fileName!s:s}'
                '''.format(state=ASAPFile.STATE_MARKED_FOR_DELETION,
                           contact_id=self.__asapContact.contact_id,
                           contactPath=asapFile.contactPath,
                           fileName=asapFile.fileName))
            rec = cursor.fetch(True)
            cursor.rollback()
            if rec:
                fileId, = rec
        else:
            raise Exception("ASAPFile contact doesn't match ASAPFileManager contact.")
        return fileId

    def deleteFile(self, asapFile):
        """

        :param ASAPFile asapFile:
        """
        asapFile.state = ASAPFile.STATE_MARKED_FOR_DELETION
        fullPath = asapFile.getFullPath()
        if asapFile.fileId == 0:
            asapFile.fileId = self.getMarkedForDeletionId(asapFile)
        if asapFile.fileId:
            fDeleted = True
            if os.path.isfile(fullPath):
                try:
                    CRLUtility.CRLDeleteFile(fullPath)
                except Exception:
                    fDeleted = False
            if fDeleted:
                self.setNullState(asapFile)
        else:
            self.addFile(asapFile)

    def moveFile(self, asapFile, sFullDestPath):
        """

        :param ASAPFile asapFile:
        :param str|unicode sFullDestPath:
        """
        destAsapFile = self.newFile(sFullDestPath, True)
        CRLUtility.CRLCopyFile(asapFile.getFullPath(), destAsapFile.getFullPath(), False, 5)
        self.deleteFile(asapFile)
        return destAsapFile

    def getFilesByState(self, state):
        """

        :param state:
        :rtype: list[ASAPFile]
        """
        asapFiles = []
        cursor = self.getCursor()
        cursor.execute('''
            select m.id, m.file_name, m.contact_path
            from asap_file_manager m with (nolock),
            asap_file_state s with (nolock)
            where m.state_id = s.state_id
            and s.state_value = '{state!s:s}' and m.contact_id = '{contact_id!s:s}'
            '''.format(state=state, contact_id=self.__asapContact.contact_id))
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            for fileId, fileName, contactPath in recs:
                asapFile = self.newFile(fileName)
                asapFile.fileId = fileId
                if contactPath:
                    asapFile.contactPath = contactPath
                asapFile.state = state
                asapFiles.append(asapFile)
        return asapFiles

    def getFilesByName(self, fileName):
        """

        :param str fileName:
        :rtype: list[ASAPFile]
        """
        asapFiles = []
        cursor = self.getCursor()
        cursor.execute('''
            select m.id, m.file_name, m.contact_path, s.state_value
            from asap_file_manager m with (nolock),
            asap_file_state s with (nolock)
            where m.state_id = s.state_id
            and m.contact_id = '{contact_id!s:s}' and m.file_name = '{fileName!s:s}'
            '''.format(contact_id=self.__asapContact.contact_id, fileName=fileName))
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            for fileId, fileName, contactPath, state in recs:
                asapFile = self.newFile(fileName)
                asapFile.fileId = fileId
                if contactPath:
                    asapFile.contactPath = contactPath
                asapFile.state = state
                asapFiles.append(asapFile)
        return asapFiles

    def glob(self, filePattern):
        """

        :param filePattern:
        :rtype: list[ASAPFile]
        """
        asapFiles = []
        files = glob.glob(filePattern)
        for fullPath in files:
            asapFile = self.newFile(fullPath, True)
            asapFile.fileId = self.getMarkedForDeletionId(asapFile)
            if not asapFile.fileId:
                asapFiles.append(asapFile)
        return asapFiles

    def getContent(self, asapFile):
        """

        :param ASAPFile asapFile:
        :rtype: str
        """
        content = ''
        xmitConfig = self.getXmitConfig()
        cursor = xmitConfig.reconnect(xmitConfig.DB_NAME_XMIT)
        cursor.execute('''
            select id, file_content
            from asap_file_manager with (nolock)
            where id = {fileId:d}
            '''.format(fileId=asapFile.fileId))
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            fileId, content64 = recs[0]
            content = base64.decodestring(content64)
        return content

    def writeFile(self, asapFile):
        """

        :param ASAPFile asapFile:
        """
        fullPath = asapFile.getFullPath()
        content = self.getContent(asapFile)
        if fullPath and os.path.exists(os.path.dirname(fullPath)) and content:
            ptr = open(fullPath, 'wb')
            ptr.write(content)
            ptr.close()
