"""

  Facility:         ILS

  Module Name:      AMPACCustom

  Version:
      Software Version:          Python version 2.3

      Copyright 2006, Clinical Reference Laboratory.  All Rights Reserved.

  Abstract:
      This script contains custom classes for AMPAC for ASAP processing.
      
  Author:
      Jarrod Wild

  Creation Date:
      01-Mar-2007

  Modification History:

"""
from ILS import ASAP
from ILS.ASAP.CaseFactory       import ASAPCaseFactory
from ILS.ASAP.DocumentFactory   import ASAPDocumentFactory
from ILS.ASAP.DocumentHistory   import ASAPDocumentHistory
from ILS.ASAP.MainHandler       import ASAPMainHandler
from ILS.ASAP.IndexHandler      import ASAPIndexHandler
from ILS.ASAP.TransmitHandler   import ASAPTransmitHandler
from ILS.ASAP.Utility           import ASAPUtility 
import CRLUtility
import datetime
import glob
import os
import sys
import time

class AMPACIndexHandler( ASAPIndexHandler ):
    """
    Custom handler for building indexes for AMPAC.
    """
    def _postProcessIndex( self ):
        # open index file for appending, and write
        # the client doc types and special name format for AMPAC:
        # APPII,APPIIAIG002999999.PDF
        case = self._getCase()
        docs = list(case.getDocuments().values())
        appendlines = []
        for doc in docs:
            clientDocType = case.contact.docTypeNameMap.get( doc.getDocTypeName() )
            appendlines.append( '%s,%s%s.PDF\n'
                                % (clientDocType,
                                   clientDocType,
                                   case.trackingId) )
        paths = self._getIndexPaths()
        if paths and os.path.isfile(paths[0]):
            filePtr = open( paths[0], 'a' )
            filePtr.writelines( appendlines )
            filePtr.close()
        return True


class AMPACTransmitHandler( ASAPTransmitHandler ):
    """
    Custom handler for AMPAC transmission.
    """
    def _stageIndexedCase( self ):
        case = self._getCurrentCase()
        fSuccess = True
        # get case file
        idxPath = os.path.join( case.contact.index_dir,
                                '%s.IDX' % case.trackingId )
        if os.path.isfile( idxPath ):
            xmitIdxPath = os.path.join( case.contact.xmit_dir,
                                        'cas%s.CAS' % case.trackingId.lower() )
            CRLUtility.CRLCopyFile( idxPath, xmitIdxPath, True, 5 )
        else:
            fSuccess = False
            self._getLogger().warn(
                'Failed to find index file for case (%s/%s).'
                % (case.sid, case.trackingId) )
        return fSuccess

    def _transmitStagedCases( self ):
        fSuccess = True
        # zip the files and use WS_FTP scripts to send to APPS' ftp server
        contact = self._getContact()
        devSuffix = ''
        if ASAP.devState.isDevInstance():
            devSuffix = 'TEST_'
        APPS_FTP_COMMAND = ( r'"E:\Exe\CRL\WS_FTP Pro\ftpscrpt" -f ' +
                             r'E:\Scripts\ws_ftp\ILS\ILS_ESUB_APPS_%s%s.scp'
                             % (devSuffix, contact.contact_id.upper()) )
        client = contact.contact_id[5:]
        xmitStagingPath = contact.xmit_dir
        xmitZipPath = os.path.join( xmitStagingPath, 'zip' )
        xmitSentPath = os.path.join( xmitStagingPath, 'sent' )
        toXmitFiles = glob.glob( os.path.join(xmitStagingPath, '*.*') )
        if ( len(toXmitFiles) > 0 ):
            self._getLogger().info(
                'There are %d files in the transmit staging folder to process for %s...'
                % (len(toXmitFiles), contact.contact_id) )
            today = datetime.datetime.today()
            zipFileName = 'cass%s%s.zip' % (client, today.strftime('%Y%m%d%H%M%S') )
            CRLUtility.CRLZIPFiles( os.path.join(xmitStagingPath, '*.*'),
                                    os.path.join(xmitZipPath, zipFileName),
                                    True )
        # now FTP any unsent zip files to APPS for AMPAC
        zipFiles = glob.glob( os.path.join(xmitZipPath, '*.*') )
        if zipFiles:
            # use WS_FTP script to secure FTP the zip file to APPS
            # make 5 attempts to send zip file
            iAttempts = 0
            fSent = False
            while ( (not fSent) and (iAttempts < 5) ):
                iAttempts += 1
                iRet = CRLUtility.CRLSystem( APPS_FTP_COMMAND,
                                             self._getLogger() )
                if iRet == 0:
                    fSent = True
            if fSent:
                for zipFile in zipFiles:
                    CRLUtility.CRLCopyFile( zipFile,
                                            os.path.join(xmitSentPath,
                                                         os.path.basename(zipFile)),
                                            True, 5 )
            else:
                fSuccess = False
                self._getLogger().warn(
                    'Error %s attempting to FTP files to APPS:\n%s'
                    % (iRet, str(zipFiles)) )
        return fSuccess
    

def SendAMPACCaseFiles( xmitdate ):
    fError = False
    config = ASAP.xmitConfig
    util = ASAPUtility()
    logger = CRLUtility.CRLGetLogger()
    handler = ASAPMainHandler()
    # get the AMPAC contacts, and map the AMPAC/APPS contacts to them
    ampacaig = config.getContact( '001', '0001' )
    ampacing = config.getContact( '001', '0002' )
    ampacmnm = config.getContact( '001', '0003' )
    ampactro = config.getContact( '001', '0004' )
    aigampcapps = config.getContact( 'AGI', 'AMPC', 'APPS' )
    ingampcapps = config.getContact( 'ING', 'AMPC', 'APPS' )
    mnmampcapps = config.getContact( 'MNM', 'AMPC', 'APPS' )
    troampcapps = config.getContact( 'TRO', 'AMPC', 'APPS' )
    contactDict = {
        aigampcapps: ampacaig,
        ingampcapps: ampacing,
        mnmampcapps: ampacmnm,
        troampcapps: ampactro
        }
    enddate = datetime.datetime( xmitdate.year, xmitdate.month, xmitdate.day,
                                 22, 0, 0 )
    days = 1
    if enddate.weekday() == CRLUtility.sunday:
        days += 1
    begindate = enddate - datetime.timedelta( days )
    if enddate >= datetime.datetime.today():
        fError = True
        logger.warn( 'Same-day transmit cannot be run until after 10pm.' )
    elif enddate.weekday() == CRLUtility.saturday:
        fError = True
        logger.warn( 'Transmission cannot be run for Saturday.' )
    if not fError:
        # do query to get transmitted cases for the time frame of
        # yesterday at 10pm up to but not including 10pm today
        sQuery = '''
            select sid, documentid, max(actiondate) lastdate
            from %s
            where contact_id in ('agiampcapps','ingampcapps','mnmampcapps','troampcapps')
            and actionitem = '%s'
            and actiondate >= '%s'
            and actiondate < '%s'
            group by sid, documentid
            order by sid, documentid
                 ''' % ( ASAPDocumentHistory.TABLE_DOCUMENT_HISTORY,
                         ASAPDocumentHistory.ACTION_TRANSMIT,
                         begindate.strftime('%d-%b-%Y %H:%M:%S.00'),
                         enddate.strftime('%d-%b-%Y %H:%M:%S.00') )
        cursor = config.getCursor( config.DB_NAME_XMIT )
        iRet = cursor.execute( sQuery )
        recs = cursor.fetch()
        cursor.rollback()
        sidDocDict = {}
        if recs:
            for sid, docid, actiondate in recs:
                doclist = sidDocDict.get( sid )
                if doclist and docid not in doclist:
                    doclist.append( docid )
                elif not doclist:
                    sidDocDict[sid] = [docid,]
        caseFactory = ASAPCaseFactory()
        docFactory = ASAPDocumentFactory()
        # map AMPAC contact to a list of related cases
        ampacCaseDict = {}
        for sid in list(sidDocDict.keys()):
            case = caseFactory.fromSid( sid )
            if case:
                # use proper AMPAC contact for this case
                case.contact = contactDict.get( case.contact )
                if case.contact:
                    # make 103 retrievable before exporting the case
                    # call MainHandler.exportCase() to get 103
                    if util.reReleaseCase( case ) and handler.exportCase( case ):
                        docs = []
                        for docid in sidDocDict[sid]:
                            doc = docFactory.fromDocumentId( docid )
                            if doc:
                                docs.append( doc )
                        for doc in docs:
                            case.addDocument( doc )
                        if case.getDocuments() and handler.buildIndexesForCase( case ):
                            cases = ampacCaseDict.get( case.contact )
                            if cases:
                                cases.append( case )
                            else:
                                ampacCaseDict[case.contact] = [case,]
                        else:
                            fError = True
                            logger.warn( 'No documents to send, or problems building ' +
                                         'index for case (%s/%s).'
                                         % (case.sid, case.trackingId) )
                    else:
                        fError = True
                        logger.warn( 'Failed to export case (%s/%s).'
                                     % (case.sid, case.trackingId) )
                else:
                    fError = True
                    logger.warn( 'Case (%s/%s) did not have valid contact.'
                                 % (case.sid, case.trackingId) )
        # once indexes are built, call MainHandler.stageAndTransmitCases() for
        # each list of cases
        # allow doc history to track when they are transmitted
        # (might be beneficial later)
        for contact in list(ampacCaseDict.keys()):
            if not handler.stageAndTransmitCases( ampacCaseDict[contact],
                                                  contact, [] ):
                fError = True
                logger.warn( 'Failed to stage and/or transmit all cases ' +
                             'for AMPAC contact %s.' % contact.contact_id )
    if fError:
        logger.error(
            'There were one or more errors transmitting case files for AMPAC.' )


if __name__ == '__main__':
    logger = CRLUtility.CRLGetLogger()
    try:
        begintime = time.time()
        arg = ''
        #arg = 'transmit=2007-03-01'
        if len( sys.argv ) > 1:
            arg = sys.argv[1]
        if arg.startswith( 'transmit' ):
            xmitdate = datetime.datetime.today()
            sep = arg.find( '=' )
            if sep > 0:
                xmitdate = CRLUtility.ParseStrDate( arg[sep+1:], True )
            SendAMPACCaseFiles( xmitdate )
        else:
            print('Argument(s) not valid. Valid arguments:')
            print('transmit[=yyyy-mm-dd]')
        logger.info( 'Time to process this pass was %s seconds.'
                     % (time.time() - begintime) )
    except:
        logger.exception( 'Error' )

