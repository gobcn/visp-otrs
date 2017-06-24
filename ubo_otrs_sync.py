#!/usr/bin/env python
import logging
import logging.handlers
import requests
import requests.auth
import urllib
import MySQLdb
import MySQLdb.cursors
import phonenumbers
import time
from posixpath import join as urljoin
import subprocess
CLIENT_ID = "someLonGGuidClientId" # Fill this in with your client ID
CLIENT_SECRET = "SomEGreaTSecreT" # Fill this in with your client secret
TENANT_ID = 9876 # Fill this in wiht your tenant ID
UBO_API_SERVER = "https://api.visp.net/"
COUNTRY_CODE = 'CA'

log = None

def syslog_logger(server, port):
    log = logging.getLogger('ubo.otrs.sync')
    log.setLevel(logging.DEBUG) # set logging level to syslog server
    handler = logging.handlers.SysLogHandler(address=(server, port))
    formatter = logging.Formatter(fmt = 'UBO-OTRS-Sync: %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.addHandler(logging.StreamHandler()) #also log to screen
    log.info("UBO OTRS Synchronization: logger initialized")
    return log

def get_token():
    headers = {"x-visp-client-id" : CLIENT_ID, "x-visp-client-secret" : CLIENT_SECRET, "x-visp-tenant-id": TENANT_ID}
    tokenurl = urljoin(UBO_API_SERVER, 'token')
    log.info('Requesting authentication token from UBO server')
    try:
        response = requests.get(tokenurl, headers=headers)
    except:
        log.error('Failed to connect to UBO token provider at ' + tokenurl + '). UBO to OTRS Synchronization failed!')
        raise
    try:
        token_json = response.json()
        return token_json["token"]
    except:
        log.error('UBO API returned unexpected result instead of authentication token. UBO to OTRS Synchronization failed!')
        raise

def get_data(token,url):
    headers = {"x-visp-access-token" : token}
    response = requests.get(url, headers=headers)
    data_json = response.json()
    return data_json

def get_ubo_customer_data(access_token):
    customerurl = urljoin(UBO_API_SERVER, 'v1/customers')
    log.info('Fetching latest customer list from UBO')
    try:
        return get_data(access_token, customerurl)
    except:
        log.error('Failed to retrieve list of customers from UBO. Synchronization cannot continue!')
        raise

def otrs_create_customer_company(customerRecord):
    try:
        subprocess.check_output(["/opt/otrs/bin/otrs.Console.pl","Admin::CustomerCompany::Add","--customer-id",customerRecord.customerId,"--name",customerRecord.company],stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, e:
        logmsg = "Failed to add customer company " + customerRecord.company + "  (Customer ID #" + customerRecord.customerId + ") to OTRS: "
        log.error(logmsg + 'FATAL ERROR: ' + e.output)
        raise
    log.info("Customer company "  + customerRecord.company + " (Customer #" + customerRecord.customerId + ") has been successfully added to OTRS")
    return True

def otrs_create_customer_user(customerRecord):
    try:
        subprocess.check_output(["/opt/otrs/bin/otrs.Console.pl","Admin::CustomerUser::Add","--user-name",customerRecord.username,"--first-name",customerRecord.firstName,"--last-name",customerRecord.lastName,"--email-address",customerRecord.email,"--customer-id",customerRecord.customerId,"--password",customerRecord.password],stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, e:
        logmsg = "Failed to add customer user " + customerRecord.username + " (Customer ID #" + customerRecord.customerId + ") to OTRS: "
        if "User already exists" in e.output:
            log.warning(logmsg + "A user with the same username, " + customerRecord.username + ", already exists")
        elif "Email already exists" in e.output:
            log.warning(logmsg + "A user with the same email, " + customerRecord.email + ", already exists!")
        elif "has no mail exchanger" in e.output:
            log.warning(logmsg + "Customer email address " + customerRecord.email + " is invalid, cannot find MX or A record for email domain")
        else:
            log.error(logmsg + "FATAL ERROR: " + e.output)
            raise
        return False
    log.info("Customer user "  + customerRecord.username + " (Customer #" + customerRecord.customerId + ") has been successfully added to OTRS")
    return True

class OTRSCustomerDB(object):
    """ Class for OTRSCustomerDB

    """

    def __init__(self, host, user, password, database):
        log.info("Establishing connection with OTRS MySQL database")
        try:
            self.db = MySQLdb.connect(host, user, password, database, cursorclass=MySQLdb.cursors.DictCursor)
        except:
            log.error("UBO-OTRS Sync has failed to connect to MySQL database! Verify that the MySQL database is available and that credentials are correct.")
            raise
        self.host=host
        self.user=user
        self.password=password
        self.database=database
        self.cursor = self.db.cursor()
        log.info("Connected to OTRS MySQL database")

    def get_custuser_record_from_id(self, customerId):
        self.cursor.execute("SELECT * from customer_user WHERE customer_id = " + customerId)
        return self.cursor.fetchone()

    def get_custcomp_record_from_id(self, customerId):
        self.cursor.execute("SELECT * from customer_company WHERE customer_id = " + customerId)
        return self.cursor.fetchone()

    def disable_custuser(self, customerId):
        sql = "UPDATE customer_user SET valid_id = 2 WHERE customer_id = " + customerId
        self.update_statement(sql)

    def disable_custcomp(self, customerId):
        sql = "UPDATE customer_company SET valid_id = 2 WHERE customer_id = " + customerId
        self.update_statement(sql)

    def enable_custuser(self, customerId):
        sql = "UPDATE customer_user SET valid_id = 1 WHERE customer_id = " + customerId
        self.update_statement(sql)

    def enable_custcomp(self, customerId):
        sql = "UPDATE customer_company SET valid_id = 1 WHERE customer_id = " + customerId
        self.update_statement(sql)


    @staticmethod
    def has_company_record_changed(ccrecord, ubocc):
        if ccrecord['name'] != ubocc.company:
            return True
        elif ccrecord['street'] != ubocc.address1:
            return True
        elif ccrecord['zip'] != ubocc.zip:
            return True
        elif ccrecord['city'] != ubocc.city:
            return True
        elif ccrecord['comments'] !=ubocc.status:
            return True
        else:
            return False

    def update_custcomp(self, ccrecord, ubocc):
        if self.has_company_record_changed(ccrecord, ubocc):
            log.info("Company " + ubocc.company + " has changed in UBO since last synchronization. Updating in OTRS.")
            sql = "UPDATE customer_company SET name='%s', street='%s', zip='%s', city='%s', comments='%s', change_time='%s' WHERE customer_id='%s'" % (self.db.escape_string(ubocc.company), self.db.escape_string(ubocc.address1), self.db.escape_string(ubocc.zip), self.db.escape_string(ubocc.city), ubocc.status, time.strftime('%Y-%m-%d %H:%M:%S'), ubocc.customerId)
            sql = sql.replace("'None'", "NULL")
            self.update_statement(sql)

    @staticmethod
    def has_customer_user_record_changed(curecord, ubocu):
        if curecord['first_name'] != ubocu.firstName:
            return True
        elif curecord['last_name'] != ubocu.lastName:
            return True
        elif curecord['login'] != ubocu.username:
            return True
        elif curecord['email'] != ubocu.email:
            return True
        elif curecord['phone'] != ubocu.homeOrWorkPhone:
            return True
        elif curecord['fax'] != ubocu.fax:
            return True
        elif curecord['mobile'] != ubocu.cellPhone:
            return True
        else:
            return False

    def update_custuser(self, curecord, ubocu):
        if self.has_customer_user_record_changed(curecord, ubocu):
            log.info("Customer user " + ubocu.company + " has changed in UBO since last synchronization. Updating in OTRS.")
            sql = "UPDATE customer_user SET first_name='%s', last_name='%s', login='%s', email='%s', phone='%s', fax='%s', mobile='%s', change_time='%s' WHERE customer_id='%s'" % (self.db.escape_string(ubocu.firstName), self.db.escape_string(ubocu.lastName), ubocu.username, ubocu.email, ubocu.homeOrWorkPhone, ubocu.fax, ubocu.cellPhone, time.strftime('%Y-%m-%d %H:%M:%S'), ubocu.customerId)
            sql = sql.replace("'None'", "NULL")
            self.update_statement(sql)

    def update_statement(self, sql):
        try:
            # Execute the SQL command
            self.cursor.execute(sql)
            # Commit your changes in the database
            self.db.commit()
        except:
            # Rollback in case there is any error
            self.db.rollback()

    def close_db(self):
        self.db.close()
        log.info("Disconnected from OTRS MySQL database")

class UBOCustomerDB(list):
    """ Class for UBOCustomerDB, extends list

    """

    @classmethod
    def generateCustDbFromUBOData(cls, data):
        """ Class method to create a new UBOCustomerDB object based on
            UBO data
        """
        log.info("Parsing UBO data to load into memory")
        localcustdb = cls()
        try:
            localcustdb.loadUBOData(data)
        except:
            log.error("Failed to parse or load the data downloaded from UBO. Synchronization failed!")
            raise
        log.info("UBO data parsed and loaded into memory for further processing")
        return localcustdb

    def loadUBOData(self, data):
        """ Method to load data from UBO into this UBOCustomerDB object

        """
        for record in data:
            custrecord = UBOCustomerRecord.createFromJson(record)
            self.append(custrecord)

    def print_db_contents(self):
        for custrec in self: custrec.print_record()

    def sync_to_otrs_db(self, otrsCustDb):
        """ Method to update the built in customer database in OTRS
            with the customer data in UBO
        """

        for record in self:
            record.sync_to_otrs_db(otrsCustDb)

class UBOCustomerRecord(object):
    """ Class for UBOCustomerRecord

    """

    @classmethod
    def createFromJson(cls, record):
        """ Class method to create a new UBOCustomerRecord object based on
            JSON data for a single customer from UBO
        """
        localcustrecord = cls()
        #print record
        for tab in record:
            tabObject = UBOCustomerTab()
            tabObject.__dict__.update(record[tab])
            tempDict = { tab: tabObject }
            localcustrecord.__dict__.update(tempDict)
        return localcustrecord

    @property
    def customerId(self):
        return str(self.primaryAccount.customerId)

    @property
    def username(self):
        if self.isDeleted:
            return self.primaryAccount.username + self.customerId
        else:
            return self.primaryAccount.username

    @property
    def password(self):
        return self.primaryAccount.password

    @property
    def company(self):
        # this ensures that the company name is formatted correctly to be acceptable to OTRS
        if (self.subscriber.company == '') or (self.subscriber.company is None):
            return self.firstName + " " + self.lastName + " | UBO #" + self.customerId
        else:
            return self.subscriber.company + " | UBO #" + self.customerId

    @property
    def firstName(self):
        return self.subscriber.firstName

    @property
    def lastName(self):
        return self.subscriber.lastName

    @property
    def status(self):
        return self.primaryAccount.status

    @property
    def homeOrWorkPhone(self):
        if self.workPhone is not None:
            return self.workPhone
        else:
            return self.homePhone

    @property
    def homePhone(self):
        return self.format_phone(self.subscriber.homePhone)

    @property
    def cellPhone(self):
        return self.format_phone(self.subscriber.cellPhone)

    @property
    def workPhone(self):
        return self.format_phone(self.subscriber.workPhone)

    @property
    def fax(self):
        return self.format_phone(self.subscriber.fax)

    @property
    def address1(self):
        return self.subscriber.address1

    @property
    def address2(self):
        return self.subscriber.address2

    @property
    def city(self):
        return self.subscriber.city

    @property
    def state(self):
        return self.subscriber.state

    @property
    def zip(self):
        return self.subscriber.zip

    @property
    def email(self):
        # this ensures that the email address is formatted correctly to be acceptable to OTRS
        if (self.billing.email == '') or (self.billing.email is None) or self.isDeleted:
            return self.username + self.customerId + '@noemail.net'
        elif "," in self.billing.email:
            return self.billing.email.split(',')[0]
        else:
            return self.billing.email

    @property
    def isDeleted(self):
        if self.status == 'Deleted':
            return True;
        else:
            return False;

    @staticmethod
    def format_phone(phonenumber):
        if phonenumber is None:
            return None
        elif phonenumber.replace('-', '') == '':
            return None
        else:
            return phonenumbers.format_number(phonenumbers.parse(phonenumber, COUNTRY_CODE), phonenumbers.PhoneNumberFormat.NATIONAL)

    def sync_to_otrs_db(self, otrsCustDb):
        ccrecord = otrsCustDb.get_custcomp_record_from_id(self.customerId)
        if ccrecord is None:
            if otrs_create_customer_company(self):
                ccrecord = otrsCustDb.get_custcomp_record_from_id(self.customerId)
        curecord = otrsCustDb.get_custuser_record_from_id(self.customerId)
        if curecord is None:
            if otrs_create_customer_user(self):
                curecord = otrsCustDb.get_custuser_record_from_id(self.customerId)
        if ccrecord is not None:
            otrsCustDb.update_custcomp(ccrecord, self)
            if self.isDeleted and (ccrecord['valid_id'] == 1):
                print "Company deleted in UBO but active in OTRS! (" + self.customerId + ") Updating OTRS..."
                otrsCustDb.disable_custcomp(self.customerId)
            elif not(self.isDeleted) and (ccrecord['valid_id'] == 2):
                print "Company disabled in OTRS but active in UBO! (" + self.customerId + ") Updating OTRS..."
                otrsCustDb.enable_custcomp(self.customerId)

        if curecord is not None:
            otrsCustDb.update_custuser(curecord, self)
            if self.isDeleted and (curecord['valid_id'] == 1):
                print "Customer user deleted in UBO but active in OTRS! (" + self.customerId + ") Updating OTRS..."
                otrsCustDb.disable_custuser(self.customerId)
            elif not(self.isDeleted) and (curecord['valid_id'] == 2):
                print "Customer user disabled in OTRS but active in UBO! (" + self.customerId + ") Updating OTRS..."
                otrsCustDb.enable_custuser(self.customerId)


    def print_record(self):
        print "========================================"
        if self.customerId is not None:
            print "Customer ID: " + self.customerId
        if self.username is not None:
            print "Username:    " + self.username
        if self.password is not None:
            print "Password:    " + self.password
        if self.company is not None:
            print "Company:     " + self.company
        if self.firstName is not None:
            print "First Name:  " + self.firstName
        if self.lastName is not None:
            print "Last Name:   " + self.lastName
        if self.status is not None:
            print "Status:      " + self.status
        if self.homePhone is not None:
            print "Home Phone:  " + self.homePhone
        if self.cellPhone is not None:
            print "Cell Phone:  " + self.cellPhone
        if self.workPhone is not None:
            print "Work Phone:  " + self.workPhone
        if self.fax is not None:
            print "Fax:         " + self.fax
        if self.address1 is not None:
            print "Address L1:  " + self.address1
        if self.address2 is not None:
            print "Address L2:  " + self.address2
        if self.city is not None:
            print "City:        " + self.city
        if self.state is not None:
            print "State:       " + self.state
        if self.zip is not None:
            print "Zip:         " + self.zip
        if self.email is not None:
            print "EMail:       " + self.email
        print "========================================"

class UBOCustomerTab(object):
    """ This class is for objects that that store data for one tab of one
        UBOCustomerRecord. A UBOCustomerRecord object is made up of
        multiple (at the moment, three) UBOCustomerTab objects
        (subscriber, billing, and primaryAccount) that correspond
        with those tabs in the UBO customer information.
    """

log = syslog_logger('mysyslogserver.mycorp.com', 514)
log.info('Connecting to UBO REST API...')
access_token = get_token()
log.info('Successfully received authentication token from UBO')
#print access_token
uboCustData = get_ubo_customer_data(access_token)
print uboCustData
log.info('Current customer list downloaded from UBO API successfully')
custDb = UBOCustomerDB.generateCustDbFromUBOData(uboCustData)
#custDb.print_db_contents()
otrsCustDb=OTRSCustomerDB("localhost", "otrs", "somesecretpassword", "otrs")
custDb.sync_to_otrs_db(otrsCustDb)
otrsCustDb.close_db()
log.info('Synchronization process finished')
