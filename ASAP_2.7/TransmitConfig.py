"""

  Facility:         ILS

  Module Name:      TransmitConfig

  Version:
      Software Version:          Python version 2.7

      Copyright 2019, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains the ASAPTransmitConfig class.

  Author:
      Jarrod Wild

  Creation Date:
      24-Oct-2006

  Modification History:
      08-Feb-2010   jmw
          Updated to use CRLDBCursor object.

      13-Jul-2015   venkatj  Ticket 48263
          Imaging system upgrade project. Master ticket 45490.
          Use a common tool ILS.Utils.DocServConnSettings.py to connect to the database

      20-mar-2017   gandhik      69036
            Added AG FTP info

      27-Sep-2019   jbn      SCTASK0021398
          Migrating ASAP to new apphub
          Upgrade to Python 2.7
          Moved AG specific FTP Info to the AIGCustom TransmitHandler class
"""
from __future__ import division, absolute_import, with_statement, print_function


import DevInstance
import CRLUtility
import os
from collections import defaultdict
from CRL.DBCursor import CRLDBCursor
from .Contact import ASAPContact
from .IndexField import ASAPIndexField
from ILS.Utils import DocServConnSettings as dsc

# Global variables
STR_ASAP_XMIT_DB_CONNECT = dsc.getConnString(dsc.DATABASES.ILS, DevInstance.devInstance().isDevInstance())


class ASAPTransmitConfig(object):
    """
    Wrapper for SQL Server config tables for indexing and transmission
    for ASAP contact IDs.
    """
    #
    # The following lines are used to determine if this module should
    # run as a development or production instance
    #
    # configuration table names (from the ASAP schema)
    TABLE_SETTINGS = 'asap_settings'
    TABLE_DB_SETTINGS = 'asap_db_settings'
    TABLE_CONTACT_SETTINGS = 'asap_contact_settings'
    TABLE_INDEX_FIELDS = 'asap_index_fields'
    TABLE_CONTACT_INDEX_MAP = 'asap_contact_index_map'
    TABLE_CONTACT_PATHS = 'asap_contact_paths'
    TABLE_CONTACT_CUSTOM_PY = 'asap_contact_custom_py'
    TABLE_CONTACT_CARRIER_MAP = 'asap_contact_carrier_map'
    # constants for the common settings in the asap_settings table
    SETTING_REPORT_ID = 'crr_report_id'
    SETTING_ACORD103_DIR = 'acord103_dir'
    SETTING_BUILD_SUBDIR = 'build_subdir'
    SETTING_ERROR_SUBDIR = 'error_subdir'
    SETTING_PROCESSED_SUBDIR = 'processed_subdir'
    SETTING_DELTA_SID_FIELD = 'delta_sid_field'
    SETTING_DELTA_EXPORT_FIELD = 'delta_export_field'
    SETTING_NO_BILL_NO_SEND_CODE = 'no_bill_no_send_code'
    SETTING_NO_BILL_CODE = 'no_bill_code'
    # constants for the common database names in the asap_db_settings table
    DB_NAME_XMIT = 'xmit'
    DB_NAME_SIP = 'sip'
    DB_NAME_SNIP = 'snip'
    DB_NAME_DELTA_QC = 'delta_qc'
    DB_NAME_ACORD = 'acord'
    DB_NAME_CASE_QC = 'case_qc'

    # a cache of actual database cursors
    __cursors = {}

    # a cache of database connection info loaded from asap_db_settings, passed to CRLDBCursor
    __cursor_info = {}
    __settings = {}
    __contacts = {}  # type: dict[tuple, ASAPContact]
    __initialized = False

    def __init__(self, logger=None):
        if not logger:
            self.__logger = CRLUtility.CRLGetLogger()
        else:
            self.__logger = logger

    def __initialize(self):
        self.__initialized = True
        if self.__cursor_info:
            self.__logger.info('cursor metadata already retrieved')
        else:
            self.__cursor_info[self.DB_NAME_XMIT] = (CRLDBCursor.DB_TYPE_ODBC, STR_ASAP_XMIT_DB_CONNECT)
        if self.__settings:
            self.__logger.info('setting metadata already retrieved')
        else:
            self.__loadSettings()
        if self.__contacts:
            self.__logger.info('contacts metadata already retrieved')
        else:
            self.__loadContacts()

    def __loadDbCursorInfo(self):
        sQuery = '''
            select db_name, db_type, db_connect_string
            from {table!s:s} with (nolock)
            '''.format(table=self.TABLE_DB_SETTINGS)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            for dbname, dbtype, connstr in recs:
                self.__cursor_info[dbname] = (dbtype, connstr)
        else:
            self.__logger.warn('Unable to retrieve cursor metadata')

    def __loadSettings(self):
        sQuery = '''
            select setting_name, setting_value from {table!s:s} with (nolock)
            '''.format(table=self.TABLE_SETTINGS)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            for name, value in recs:
                self.__settings[name] = value

    def __loadContactIndex(self, contact):
        """

        :param ASAPContact contact:
        """
        sQuery = '''
            select cim.contact_field_name, cim.field_order,
                   aif.field_type, aif.source_name, aif.field_ref,
                   cim.max_length, cim.format, cim.required
            from {contact_table!s:s} cim with (nolock)
            inner join {index_table!s:s} aif with (nolock)
                    on cim.field_name = aif.field_name
            where cim.contact_id = '{contact_id!s:s}'
            '''.format(contact_table=self.TABLE_CONTACT_INDEX_MAP,
                       index_table=self.TABLE_INDEX_FIELDS,
                       contact_id=contact.contact_id)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        idxRecs = cursor.fetch()
        cursor.rollback()
        if idxRecs:
            for (fieldname, fieldorder, fieldtype, sourcename,
                 fieldref, maxlength, fmt, required) in idxRecs:
                indexField = ASAPIndexField(self.__logger)
                indexField.setFieldMeta(fieldname, fieldtype, required,
                                        maxlength, fmt, sourcename, fieldref)
                contact.index.addField(indexField, fieldorder)

    def __loadContactPaths(self, contact):
        """

        :param ASAPContact contact:
        """
        sQuery = '''
            select staging_dir, document_subdir, acord103_subdir, index_subdir, xmit_subdir
            from {table!s:s} with (nolock)
            where contact_id = '{contact_id!s:s}'
            '''.format(table=self.TABLE_CONTACT_PATHS,
                       contact_id=contact.contact_id)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        pathRec = cursor.fetch(True)
        cursor.rollback()
        if pathRec:
            staging_dir, doc_subdir, acord103_subdir, idx_subdir, xmit_subdir = pathRec
            if doc_subdir and doc_subdir.strip():
                contact.document_dir = os.path.join(staging_dir, doc_subdir.strip())
            if acord103_subdir and acord103_subdir.strip():
                contact.acord103_dir = os.path.join(staging_dir, acord103_subdir.strip())
            if idx_subdir and idx_subdir.strip():
                contact.index_dir = os.path.join(staging_dir, idx_subdir.strip())
            if xmit_subdir and xmit_subdir.strip():
                contact.xmit_dir = os.path.join(staging_dir, xmit_subdir.strip())

    def __loadContactServiceMap(self, contact):
        """
        Queries SIP for doctype mappings to both client doc types and billing codes

        :param ASAPContact contact:
        """
        sQuery = '''
            select document_type_name, client_document_name, tp_requested
            from document_service_map
            where contact_id = (select contact_id
                                from client_region_reports
                                where client_id = '{client_id!s:s}'
                                and region_id = '{region_id!s:s}'
                                and report_id = '{report_id!s:s}')
            '''.format(client_id=contact.client_id,
                       region_id=contact.region_id,
                       report_id=self.getSetting(self.SETTING_REPORT_ID))
        cursor = self.getCursor(self.DB_NAME_SIP)
        if cursor:
            cursor.execute(sQuery)
            mapRecs = cursor.fetch()
            cursor.rollback()
            if mapRecs:
                for docTypeName, clientName, billingCode in mapRecs:
                    contact.docTypeNameMap[docTypeName] = clientName
                    contact.docTypeBillingMap[docTypeName] = billingCode

    def __getContactServiceMap(self, contact):
        """
        Populate the docTypeNameMap and docTypeBillingMap attributes of ASAPContact contact
        Attempt to load mappings from the cache, if that fails, fall back to direct query from lims

        :param ASAPContact contact:
        """
        try:
            mapRecs = self.__cli_reg_doc_map[(contact.client_id, contact.region_id)]
            if mapRecs:
                for docTypeName, clientName, billingCode in mapRecs:
                    contact.docTypeNameMap[docTypeName] = clientName
                    contact.docTypeBillingMap[docTypeName] = billingCode
        except:
            self.__loadContactServiceMap(contact)

    def __loadContactServices(self):
        """ Load all document type mappings from Lims
        Populate a dictionary that maps (client_id, region_id) to the docType mappings
        NOTE: Do not confuse contact_id's here. The contact_id used in the 'join' between
        client_region_reports and document_service_map doesn't have anything to do with ASAP
        """

        self.__cli_reg_doc_map = {}
        sContactQuery = '''
            select client_id, region_id, contact_id
            from client_region_reports
            where report_id = '{report_id!s:s}'
            '''.format(report_id=self.getSetting(self.SETTING_REPORT_ID))
        sDocServMapQuery = '''
            select contact_id, document_type_name, client_document_name, tp_requested
            from document_service_map
            where contact_id in ('{contact_str!s:s}')
            order by contact_id, document_type_name
            '''
        cursor = self.getCursor(self.DB_NAME_SIP)
        if cursor:

            cursor.execute(sContactQuery)
            contactRecs = cursor.fetch()
            cursor.rollback()

            if contactRecs:

                client_region_contact_map = {}
                distinct_contacts = set()

                # Poplate the mapping from (client, region) to contact_id (lims document map contact_id. Not related to asap)
                for client, region, contact_id in contactRecs:
                    client_region_contact_map[(client.strip(), region.strip())] = contact_id
                    distinct_contacts.add(contact_id)

                # Use the distinct contacts to fetch the doctype mappings
                cursor.execute(sDocServMapQuery.format(contact_str="','".join(distinct_contacts)))
                docServRecs = cursor.fetch()
                cursor.rollback()

                contact_doc_map = defaultdict(list)

                # Map the doctype mappings to the contact_id
                for contact_id, docType, client_docType, tp_id in docServRecs:
                    contact_doc_map[contact_id].append([docType, client_docType, tp_id])

                # Bridge the gap so our end result maps (client, region) to a list of doctype records
                for cli_reg_key in client_region_contact_map:
                    contact_id = client_region_contact_map[cli_reg_key]
                    self.__cli_reg_doc_map[cli_reg_key] = contact_doc_map[contact_id]

    def __loadContactCustom(self, contact):
        """

        :param ASAPContact contact:
        """
        sQuery = '''
            select base_class, custom_module, custom_class
            from {table!s:s} with (nolock)
            where contact_id = '{contact_id!s:s}'
            '''.format(table=self.TABLE_CONTACT_CUSTOM_PY,
                       contact_id=contact.contact_id)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        customRecs = cursor.fetch()
        cursor.rollback()
        if customRecs:
            for base_class, custom_module, custom_class in customRecs:
                contact.customClasses[base_class] = (custom_module, custom_class)

    def __loadContactCarrierMap(self, contact):
        """

        :param ASAPContact contact:
        """
        sQuery = '''
            select acord_carrier_name
            from {table!s:s} with (nolock)
            where contact_id = '{contact_id!s:s}'
            '''.format(table=self.TABLE_CONTACT_CARRIER_MAP,
                       contact_id=contact.contact_id)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        carrierRecs = cursor.fetch()
        cursor.rollback()
        if carrierRecs:
            contact.acordCarrierNames = [carrier for carrier, in carrierRecs]

    def __loadContacts(self):
        sQuery = '''
            select contact_id, client_id, region_id, examiner,
            idx_type, idx_delim, idx_subdelim, source_code from {table!s:s} with (nolock)
            where enabled = 1
            '''.format(table=self.TABLE_CONTACT_SETTINGS)
        cursor = self.getCursor(self.DB_NAME_XMIT)
        cursor.execute(sQuery)
        recs = cursor.fetch()
        cursor.rollback()
        if recs:
            self.__loadContactServices()
            no_bill_no_send_code = self.getSetting(self.SETTING_NO_BILL_NO_SEND_CODE)
            if not no_bill_no_send_code:
                no_bill_no_send_code = ''
            no_bill_code = self.getSetting(self.SETTING_NO_BILL_CODE)
            if not no_bill_code:
                no_bill_code = ''
            for (contact_id, client, region, examiner,
                 idx_type, idx_dlm, idx_subdlm, source_code) in recs:
                contact = ASAPContact(self.__logger)
                contact.contact_id = contact_id
                contact.client_id = client
                contact.region_id = region
                contact.examiner = examiner
                contact.source_code = source_code
                contact.no_bill_no_send_code = no_bill_no_send_code
                contact.no_bill_code = no_bill_code
                contact.index.type = idx_type
                contact.index.setDelim(idx_dlm)
                contact.index.setSubdelim(idx_subdlm)
                self.__loadContactIndex(contact)
                self.__loadContactPaths(contact)
                self.__getContactServiceMap(contact)
                self.__loadContactCustom(contact)
                self.__loadContactCarrierMap(contact)
                if examiner:
                    self.__contacts[(client, region, examiner)] = contact
                else:
                    self.__contacts[(client, region)] = contact
        else:
            self.__logger.warn('Unable to retrieve contact metadata')

    def getContacts(self):
        if not self.__contacts:
            self.__initialize()
        return self.__contacts

    def getContact(self, client_id, region_id, examiner=None):
        if region_id:
            region_id = region_id.strip()
        if examiner:
            examiner = examiner.strip()
        if not self.__initialized:
            self.__initialize()
        contact = self.__contacts.get((client_id, region_id, examiner))
        if not contact:
            contact = self.__contacts.get((client_id, region_id))
        return contact

    def reconnect(self, dbName):
        """ This function was created to resolve an issue querying Acord103 blobs
        Basically do everything to erase the existence of a cursor to dbName, then
        recreate and return that cursor

        :param dbName:
        :return:
        """
        curs = self.getCursor(dbName)
        if curs:
            try:
                curs.rollback()
            except:
                pass

            try:
                curs.close()
            except:
                pass

            try:
                del curs
            except:
                pass

            try:
                dbtype, connstr = self._getCursorInfo(dbName)
                del self.__cursors[(dbtype, connstr)]
            except:
                pass

        return self.getCursor(dbName)

    def getCursor(self, dbName):
        """

        :param dbName:
        :rtype: CRLDBCursor
        """
        if not self.__initialized:
            self.__initialize()
        try:
            dbtype, connstr = self._getCursorInfo(dbName)
            return self.__cursors[(dbtype, connstr)]
        except KeyError:
            dbtype, connstr = self._getCursorInfo(dbName)
            self.__logger.debug('attempting to connect to {db!s:s}'.format(db=dbName))
            try:
                curs = CRLDBCursor(dbName, dbtype, connstr)
                self.__cursors[(dbtype, connstr)] = curs
                return curs
            except Exception:
                self.__logger.warn('failed to connect to {db!s:s}'.format(db=connstr))
                raise

    def _getCursorInfo(self, dbName):
        try:
            return self.__cursor_info[dbName]
        except KeyError:
            self.__loadDbCursorInfo()
            try:
                return self.__cursor_info[dbName]
            except KeyError:
                raise ValueError('Unknown database name: {db!s:s}'.format(db=dbName))

    def getSetting(self, sName):
        if not self.__initialized:
            self.__initialize()
        return self.__settings.get(sName)

    def close(self):
        # for cursor in self.__cursors.values():
        #     cursor.close()
        pass
