import pyodbc
from jproperties import Properties

from utils import gen_rand1, gen_rand_alpha
from mysql.connector import Error
import mysql.connector as msql
from faker import Faker
import sqlite3
import random

configs = Properties()
with open('input.properties', 'rb') as config_file:
    configs.load(config_file)

host = str(configs.get("host").data)
username = str(configs.get("username").data)
passwd = str(configs.get("passwd").data)
dbname = str(configs.get("dbname").data)
whichdb = str(configs.get("whichdb").data)
which_location = str(configs.get("whichlocation").data)
server_ms_stage = str(configs.get("server_ms_stage").data)
database_ms_stage = str(configs.get("database_ms_stage").data)
server_ms = str(configs.get("server_ms").data)
database_ms = str(configs.get("database_ms").data)
username_ms = str(configs.get("username_ms").data)
password_ms = str(configs.get("password_ms").data)
server_ms_loc = str(configs.get("server_ms_loc").data)
database_ms_loc = str(configs.get("database_ms_loc").data)
username_ms_loc = str(configs.get("username_ms_loc").data)
password_ms_loc = str(configs.get("password_ms_loc").data)
no_accounts_per_consignee = str(configs.get("no_accounts_per_consignee").data)


def get_db_connection():
    conn = None
    if whichdb == 'mysql':
        conn = msql.connect(host=host, database=dbname, user=username, password=passwd)
    elif whichdb == 'mssql' and which_location == 'remotestage':
        conn = pyodbc.connect(
            'Driver={ODBC Driver 18 for SQL Server};Server=tcp:' + server_ms_stage + ',1433;Database=' + database_ms_stage + ';Uid=' + username_ms + ';Pwd=' + password_ms + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword')
    elif whichdb == 'mssql' and which_location == 'remote':
        conn = pyodbc.connect(
            'Driver={ODBC Driver 18 for SQL Server};Server=tcp:' + server_ms + ',1433;Database=' + database_ms + ';Uid=' + username_ms + ';Pwd=' + password_ms + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword')
    elif whichdb == 'mssql' and which_location == 'local':
        conn = pyodbc.connect(
            'Driver={ODBC Driver 18 for SQL Server};Server=tcp:' + server_ms_loc + ',1433;Database=' + database_ms_loc + ';Uid=' + username_ms_loc + ';Pwd=' + password_ms_loc + ';Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30')

    return conn


def insert_one_record_accounts_table():
    try:
        fake = Faker()
        conn = get_db_connection()
        ucn = ""
        account_name = ""
        address = ""
        city = ""
        state = ""
        postalcode = ""

        """
        Connect to SQLite db and get random account number.
        """
        con = sqlite3.connect('account_names.db')
        sql = 'SELECT account_name FROM account_names ORDER BY RANDOM() LIMIT 1'
        cur = con.cursor()
        cur.execute(sql)
        record = cur.fetchall()
        account_name = record[0][0]

        """
        Generate random UCN and convert to string.
        """
        choice_is_alphabets = random.choice(["Yes", "No"])
        if choice_is_alphabets.lower() == 'yes':
            random_ucn_int = gen_rand1(1, 999999999999)
            ucn = f'{random_ucn_int:015d}'
        else:
            random_ucn_number_alpha = gen_rand1(5, 15)
            ucn = gen_rand_alpha(random_ucn_number_alpha)

        address = fake.address()
        city = fake.city()
        state = fake.state()
        yes_no = random.choice(["Yes", "No"])
        if yes_no == "Yes":
            postalcode = "%05d" % random.randint(0, 99999) + "-" + "%04d" % random.randint(0, 9999)
        else:
            postalcode = "%05d" % random.randint(0, 99999)
        country = fake.country()
        email = fake.company_email()

        if whichdb == 'mysql' and conn.is_connected():
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT individualUCN FROM accounts WHERE individualUCN = %s", (ucn,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount
            # print(\number of affected rows: {}\.format(row_count))
            if row_count == 0:
                # print(\------\)
                sql = "INSERT INTO accounts (individualUCN,hin,accountName,address,city,state,postalCode," \
                      "activeFlag,createdAt,updatedAt,country,email) VALUES (%s,NULL,%s,%s,%s,%s,%s,True,NOW(),NOW()," \
                      "%s,%s)"

                cursor1.execute(sql, (ucn, account_name, address, city, state, postalcode, country, email))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
                return ucn
            else:
                return ""
        else:
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT individualUCN FROM accounts WHERE individualUCN = ?", (ucn,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount
            # print(\number of affected rows: {}\.format(row_count))
            if row_count == 0:
                # print(\------\)
                sql = "INSERT INTO accounts (individualUCN,hin,accountName,address,city,state,postalCode," \
                      "activeFlag,createdAt,updatedAt,country,email) VALUES (?,NULL,?,?,?,?,?,1,getdate(),getdate()," \
                      "?,?)"

                cursor1.execute(sql, (ucn, account_name, address, city, state, postalcode, country, email))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
                return ucn
            else:
                return ""

    except Error as e:
        print("Error while connecting to MySQL", e)
        return ""


def insert_main_accounts_mapping(account_number):
    try:
        if whichdb == 'mysql':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute(
                "SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = %s and parentUCN is NULL",
                (account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (%s,NULL,NOW(),NOW())"

                cursor1.execute(sql_pr, (account_number,))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = ? and parentUCN is NULL",
                           (account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (?,NULL,getdate(),getdate())"

                cursor1.execute(sql_pr, (account_number,))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)


def insert_main_accounts_with_self_mapping(account_number):
    try:
        if whichdb == 'mysql':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = %s and parentUCN is "
                           "NOT NULL",
                           (account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (%s,%s,NOW(),NOW())"

                cursor1.execute(sql_pr, (account_number, account_number))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = ? and parentUCN is NOT "
                           "NULL",
                           (account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (?,?,getdate(),getdate())"

                cursor1.execute(sql_pr, (account_number, account_number))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)


def insert_sub_and_main_accounts_mapping(main_account_number, sub_account_number):
    try:
        if whichdb == 'mysql':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = %s",
                           (sub_account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (%s,%s,NOW(),NOW())"

                cursor1.execute(sql_pr, (sub_account_number, main_account_number))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = ?",
                           (sub_account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (?,?,getdate(),getdate())"

                cursor1.execute(sql_pr, (sub_account_number, main_account_number))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)


def insert_sub_accounts_mapping(sub_account_number):
    try:
        if whichdb == 'mysql':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE parentUCN is NULL")
            # gets the number of rows affected by the command executed
            main_accounts = list(cursor.fetchall())

            # print(len(main_accounts))

            random_main_int = gen_rand1(0, len(main_accounts))
            main_map_account = str(main_accounts[random_main_int][0])
            # print(main_map_account)
            # print( main_accounts)
            # print("------------------------------")

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = %s",
                           (sub_account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (%s,%s,NOW(),NOW())"

                cursor1.execute(sql_pr, (sub_account_number, main_map_account))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE parentUCN is NULL")
            # gets the number of rows affected by the command executed
            main_accounts = list(cursor.fetchall())

            # print(len(main_accounts))

            random_main_int = gen_rand1(0, len(main_accounts) - 1)
            main_map_account = str(main_accounts[random_main_int][0])
            # print(main_map_account)
            # print( main_accounts)
            # print("------------------------------")

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE accountUCN = ?",
                           (sub_account_number,))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount

            if row_count == 0:
                sql_pr = "INSERT INTO main_sub_account_mapping (accountUCN,parentUCN,createdAt,updatedAt) " \
                         "VALUES (?,?,getdate(),getdate())"

                cursor1.execute(sql_pr, (sub_account_number, main_map_account))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)


def get_all_sub_accounts_latest(number_req):
    try:
        if whichdb == 'mysql':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE parentUCN is NOT NULL")
            # gets the number of rows affected by the command executed
            main_accounts = list(cursor.fetchall())

            return main_accounts
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT TOP(" + str(
                number_req) + ") accountUCN FROM main_sub_account_mapping WHERE parentUCN is NOT NULL order by id DESC")
            # gets the number of rows affected by the command executed
            main_accounts = []
            for i in cursor:
                main_accounts.append(i)
            # main_accounts = list(cursor.fetchall())
            print(main_accounts)
            print(type(main_accounts))

            return main_accounts

    except Error as e:
        print("Error while connecting to MySQL", e)


def get_all_sub_accounts():
    try:
        if whichdb == 'mysql':
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE parentUCN is NOT NULL")
            # gets the number of rows affected by the command executed
            main_accounts = list(cursor.fetchall())

            return main_accounts
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("SELECT accountUCN FROM main_sub_account_mapping WHERE parentUCN is NOT NULL")
            # gets the number of rows affected by the command executed
            main_accounts = []
            for i in cursor:
                main_accounts.append(i)
            # main_accounts = list(cursor.fetchall())
            print(main_accounts)
            print(type(main_accounts))

            return main_accounts

    except Error as e:
        print("Error while connecting to MySQL", e)


def insert_one_consignee_to_one_account_mapping_table(account_number, consignee_details):
    try:
        print(consignee_details)
        print(account_number)

        conn = get_db_connection()

        if whichdb == 'mysql' and conn.is_connected():
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            cursor.execute("SELECT consigneeId FROM consignee_account_mapping WHERE consigneeId = %s",
                           (consignee_details["id"],))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount
            # print(\number of affected rows: {}\.format(row_count))
            if row_count == 0:
                # print(\------\)
                sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeName," \
                      "consigneeEmail," \
                      "createdAt,updatedAt) VALUES (%s,%s,%s,%s,NOW(),NOW())"

                print(sql)

                cursor1.execute(sql, (
                    account_number[0], consignee_details["id"], consignee_details["firstName"] + " "
                    + consignee_details["lastName"],
                    consignee_details["email"]))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
        else:
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            cursor.execute("SELECT consigneeId FROM consignee_account_mapping WHERE consigneeId = ?",
                           (consignee_details["id"],))
            # gets the number of rows affected by the command executed
            cursor.fetchall()
            row_count = cursor.rowcount
            # print(\number of affected rows: {}\.format(row_count))
            if row_count == 0:
                # print(\------\)
                sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeName," \
                      "consigneeEmail," \
                      "createdAt,updatedAt) VALUES (?,?,?,?,getdate(),getdate())"

                print(sql)

                cursor1.execute(sql, (
                    account_number[0], consignee_details["id"], consignee_details["firstName"] + " "
                    + consignee_details["lastName"],
                    consignee_details["email"]))
                print("Record inserted")
                # the connection is not autocommitted by default, so we
                # must commit to save our changes
                conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)
        return ""


def insert_consignee_account_mapping_table(account_numbers, consignee_details):
    try:
        print("inside method")
        print(consignee_details)
        print(account_numbers)

        conn = get_db_connection()

        if whichdb == 'mysql' and conn.is_connected():
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            for account in account_numbers:

                if not (consignee_details["email"] == 'VBabuGut@its.jnj.com'):
                    cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = %s",
                                   (consignee_details["email"],))
                    rw = cursor.fetchall()
                    nr = int(rw[0][0])

                    if nr >= int(no_accounts_per_consignee):
                        continue

                cursor.execute("SELECT consigneeId FROM consignee_account_mapping WHERE consigneeId = %s "
                               "AND subAccountUCN = %s",
                               (consignee_details["id"], account[0]))
                # gets the number of rows affected by the command executed
                cursor.fetchall()
                row_count = cursor.rowcount
                # print(\number of affected rows: {}\.format(row_count))
                if row_count == 0:
                    # print(\------\)
                    sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeName," \
                          "consigneeEmail," \
                          "createdAt,updatedAt, consigneePhone) VALUES (%s,%s,%s,%s,NOW(),NOW(),%s)"

                    print(sql)

                    cursor1.execute(sql, (
                        account[0], consignee_details["id"], consignee_details["firstName"] + " "
                        + consignee_details["lastName"],
                        consignee_details["email"], consignee_details["phone"]))
                    print("Record inserted")
                    # the connection is not autocommitted by default, so we
                    # must commit to save our changes
                    conn.commit()
        else:
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            for account in account_numbers:
                print(account)

                if not ((consignee_details["email"] == 'VBabuGut@its.jnj.com') or (
                        consignee_details["email"] == 'theautomationtester@hotmail.com') or (
                                consignee_details["email"] == 'theautomationtester@outlook.com') or (
                                consignee_details["email"] == 'shrikrishna.m@hcl.com')):
                    cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = ?",
                                   (consignee_details["email"],))
                    rw = cursor.fetchall()
                    nr = int(rw[0][0])

                    if nr >= int(no_accounts_per_consignee):
                        continue

                    cursor.execute("SELECT consigneeId FROM consignee_account_mapping WHERE consigneeId = ? "
                                   "AND subAccountUCN = ?",
                                   (consignee_details["id"], account[0]))
                    # gets the number of rows affected by the command executed
                    cursor.fetchall()
                    row_count = cursor.rowcount
                    # print(\number of affected rows: {}\.format(row_count))
                    if row_count == 0:
                        # print(\------\)
                        print("Mapping " + consignee_details["firstName"] + " to " + account[0])
                        sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeFirstName,consigneeLastName," \
                              "consigneeEmail," \
                              "createdAt,updatedAt,consigneePhone) VALUES (?,?,?,?,?,getdate(),getdate(),?)"

                        # print(sql)

                        cursor1.execute(sql, (
                            account[0], consignee_details["id"], consignee_details["firstName"],
                            consignee_details["lastName"],
                            consignee_details["email"], consignee_details["phone"]))
                        print("Record inserted")
                        # the connection is not autocommitted by default, so we
                        # must commit to save our changes
                        conn.commit()
                else:
                    cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = ?",
                                   (consignee_details["email"],))
                    rw = cursor.fetchall()
                    nr = int(rw[0][0])

                    if nr >= int(50):
                        continue

                    cursor.execute("SELECT consigneeId FROM consignee_account_mapping WHERE consigneeEmail = ? "
                                   "AND subAccountUCN = ?",
                                   (consignee_details["email"], account[0]))
                    # gets the number of rows affected by the command executed
                    cursor.fetchall()
                    row_count = cursor.rowcount
                    # print(\number of affected rows: {}\.format(row_count))
                    if row_count == 0:
                        # print(\------\)
                        print("Mapping " + consignee_details["firstName"] + " to " + account[0])
                        sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeFirstName,consigneeLastName," \
                              "consigneeEmail," \
                              "createdAt,updatedAt, consigneePhone) VALUES (?,?,?,?,?,getdate(),getdate(),?)"

                        # print(sql)

                        cursor1.execute(sql, (
                            account[0], "", consignee_details["firstName"], consignee_details["lastName"],
                            consignee_details["email"], consignee_details["phone"]))
                        print("Record inserted")
                        # the connection is not autocommitted by default, so we
                        # must commit to save our changes
                        conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)


def insert_ten_consignee_per_account(account_numbers):
    try:
        print("inside method")

        conn = get_db_connection()

        if whichdb == 'mysql' and conn.is_connected():
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            for account in account_numbers:

                for i in range(1, 11):
                    """
                        Connect to SQLite db and get random consignee name and email.
                    """
                    j = 0
                    k = 0
                    okta_email = ""
                    okta_user_name = ""
                    while j < 1:
                        con = sqlite3.connect('account_names.db')
                        sql = 'SELECT user, login FROM okta_users ORDER BY RANDOM() LIMIT 1'
                        cur = con.cursor()
                        cur.execute(sql)
                        record = cur.fetchall()
                        okta_user_name = record[0][0]
                        okta_email = record[0][1]
                        cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = %s",
                                       (okta_email,))
                        rw = cursor.fetchall()
                        nr = int(rw[0][0])
                        k = k + 1
                        if nr < 50 or k >= 40000:
                            j = 1

                    cursor.execute("SELECT consigneeEmail FROM consignee_account_mapping WHERE consigneeEmail = %s "
                                   "AND subAccountUCN = %s",
                                   (okta_email, account[0]))
                    # gets the number of rows affected by the command executed
                    cursor.fetchall()
                    row_count = cursor.rowcount
                    # print(\number of affected rows: {}\.format(row_count))
                    if row_count == 0:
                        # print(\------\)
                        sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeName," \
                              "consigneeEmail," \
                              "createdAt,updatedAt) VALUES (%s,%s,%s,%s,NOW(),NOW())"

                        print(sql)

                        cursor1.execute(sql, (account[0], "", okta_user_name, okta_email))
                        print("Record inserted")
                        # the connection is not autocommitted by default, so we
                        # must commit to save our changes
                        conn.commit()
        else:
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            for account in account_numbers:

                for i in range(1, 11):
                    """
                        Connect to SQLite db and get random consignee name and email.
                    """
                    j = 0
                    k = 0
                    okta_email = ""
                    okta_user_name = ""
                    while j < 1:
                        con = sqlite3.connect('account_names.db')
                        sql = 'SELECT user, login FROM okta_users ORDER BY RANDOM() LIMIT 1'
                        cur = con.cursor()
                        cur.execute(sql)
                        record = cur.fetchall()
                        okta_user_name = record[0][0]
                        okta_email = record[0][1]
                        cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = ?",
                                       (okta_email,))
                        rw = cursor.fetchall()
                        nr = int(rw[0][0])
                        k = k + 1
                        if nr < 50 or k >= 40000:
                            j = 1

                    # print(account)

                    cursor.execute("SELECT consigneeEmail FROM consignee_account_mapping WHERE consigneeEmail = ? "
                                   "AND subAccountUCN = ?",
                                   (okta_email, account[0]))
                    # gets the number of rows affected by the command executed
                    cursor.fetchall()
                    row_count = cursor.rowcount
                    # print(\number of affected rows: {}\.format(row_count))

                    cursor.execute("SELECT count(*) FROM consignee_account_mapping where subAccountUCN = ?",
                                   (account[0],))
                    # gets the number of rows affected by the command executed
                    result = cursor.fetchall()

                    nbr_mapped = int(result[0][0])

                    if row_count == 0 and nbr_mapped <= 20:
                        # print(\------\)
                        sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeName," \
                              "consigneeEmail," \
                              "createdAt,updatedAt) VALUES (?,?,?,?,getdate(),getdate())"

                        print(sql)

                        cursor1.execute(sql, (account[0], "", okta_user_name, okta_email))
                        print("Record inserted")
                        # the connection is not autocommitted by default, so we
                        # must commit to save our changes
                        conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)


def insert_ten_nonactive_consignee_per_account(account_numbers, user_status):
    try:
        print("inside method add consignees")
        fake = Faker()

        conn = get_db_connection()

        if whichdb == 'mysql' and conn.is_connected():
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            for account in account_numbers:

                for i in range(1, 3):
                    """
                        Connect to SQLite db and get random consignee name and email.
                    """
                    j = 0
                    k = 0
                    okta_email = ""
                    okta_user_name = ""
                    '''
                    DEPROVISIONED
                    PROVISIONED
                    PASSWORD_EXPIRED
                    STAGED
                    SUSPENDED
                    '''
                    while j < 1:
                        con = sqlite3.connect('account_names.db')
                        # for local okta
                        # sql = 'SELECT user, login FROM okta_users_nonactive where status = %s ORDER BY RANDOM() LIMIT 1'
                        # for stage okta
                        sql = 'SELECT firstName, lastName, login FROM stage_okta_users ORDER BY RANDOM() LIMIT 1'
                        cur = con.cursor()
                        cur.execute(sql, ())
                        record = cur.fetchall()
                        # for local okta
                        # okta_user_name = record[0][0]
                        # for stage okta
                        okta_user_name = record[0][0] + " " + record[0][1]
                        # for local okta
                        okta_email = record[0][1]
                        # for stage okta
                        okta_email = record[0][2]
                        cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = %s",
                                       (okta_email,))
                        rw = cursor.fetchall()
                        nr = int(rw[0][0])
                        k = k + 1
                        if nr < 2 or k >= 40000:
                            j = 1

                    cursor.execute("SELECT consigneeEmail FROM consignee_account_mapping WHERE consigneeEmail = %s "
                                   "AND subAccountUCN = %s",
                                   (okta_email, account[0]))
                    # gets the number of rows affected by the command executed
                    cursor.fetchall()
                    row_count = cursor.rowcount
                    # print(\number of affected rows: {}\.format(row_count))
                    if row_count == 0:
                        # print(\------\)
                        sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeName," \
                              "consigneeEmail," \
                              "createdAt,updatedAt) VALUES (%s,%s,%s,%s,NOW(),NOW())"

                        print(sql)

                        cursor1.execute(sql, (account[0], "", okta_user_name, okta_email))
                        print("Record inserted")
                        # the connection is not autocommitted by default, so we
                        # must commit to save our changes
                        conn.commit()
        else:
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)
            count = 0

            for account in account_numbers:
                count = count + 1

                print("Adding consignees to account " + str(count) + " of " + str(len(account_numbers)))

                for i in range(1, 3):
                    """
                        Connect to SQLite db and get random consignee name and email.
                    """
                    j = 0
                    k = 0
                    okta_email = ""
                    okta_user_name = ""
                    okta_user_fn = ""
                    okta_user_ln = ""
                    okta_phone = ""
                    '''
                                        DEPROVISIONED
                                        PROVISIONED
                                        PASSWORD_EXPIRED
                                        STAGED
                                        SUSPENDED
                    '''
                    while j < 1:
                        con = sqlite3.connect('account_names.db')
                        # sql = 'SELECT user, login, phone FROM okta_users_nonactive where status = ? ORDER BY RANDOM() LIMIT 1'
                        # for stage okta
                        sql = 'SELECT firstName, lastName, login, mobilePhone FROM stage_okta_users ORDER BY RANDOM() LIMIT 1'
                        cur = con.cursor()
                        cur.execute(sql, ())
                        record = cur.fetchall()
                        # for local okta
                        # okta_user_name = record[0][0]
                        # for stage okta
                        okta_user_name = record[0][0] + " " + record[0][1]
                        okta_user_fn = okta_user_name.split()[0]
                        okta_user_ln = okta_user_name.split()[1]
                        # for local okta
                        # okta_email = record[0][1]
                        # okta_phone = record[0][2]
                        # for stage okta
                        okta_email = record[0][2]
                        okta_phone = record[0][3]
                        cursor.execute("SELECT count(*) FROM consignee_account_mapping where consigneeEmail = ?",
                                       (okta_email,))
                        rw = cursor.fetchall()
                        nr = int(rw[0][0])
                        k = k + 1
                        # print(str(k) + " " + str(nr) + okta_email)
                        if nr < 299 or k >= 40000:
                            j = 1

                    # print(account)

                    cursor.execute("SELECT consigneeEmail FROM consignee_account_mapping WHERE consigneeEmail = ? "
                                   "AND subAccountUCN = ?",
                                   (okta_email, account[0]))
                    # gets the number of rows affected by the command executed
                    cursor.fetchall()
                    row_count = cursor.rowcount
                    # print(\number of affected rows: {}\.format(row_count))

                    cursor.execute("SELECT count(*) FROM consignee_account_mapping where subAccountUCN = ?",
                                   (account[0],))
                    # gets the number of rows affected by the command executed
                    result = cursor.fetchall()

                    nbr_mapped = int(result[0][0])

                    if row_count == 0 and nbr_mapped <= 20:
                        print("Mapping " + okta_user_fn + " to " + account[0])
                        tphone = fake.phone_number()
                        sql = "INSERT INTO consignee_account_mapping (subAccountUCN,consigneeId,consigneeFirstName,consigneeLastName," \
                              "consigneeEmail," \
                              "createdAt,updatedAt,consigneePhone) VALUES (?,?,?,?,?,getdate(),getdate(),?)"

                        # print(sql)

                        cursor1.execute(sql, (account[0], "", okta_user_fn, okta_user_ln, okta_email, okta_phone))
                        print("Record inserted")
                        # the connection is not autocommitted by default, so we
                        # must commit to save our changes
                        conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)


def delete_data_account_sub_consignee():
    try:
        conn = get_db_connection()

        if whichdb == 'mysql' and conn.is_connected():
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            cursor.execute("TRUNCATE TABLE consignee_account_mapping", ())
            cursor.fetchall()
            cursor.execute("TRUNCATE TABLE main_sub_account_mapping", ())
            cursor.fetchall()
            cursor.execute("TRUNCATE TABLE accounts", ())
            cursor.fetchall()
        else:
            cursor = conn.cursor()
            cursor1 = conn.cursor()
            # cursor.execute("select database();")
            # record = cursor.fetchone()
            # print("You're connected to database: ", record)

            cursor.execute("DELETE FROM consignee_account_mapping", ())
            cursor.execute("DBCC CHECKIDENT('consignee_account_mapping', RESEED, 0)", ())
            # cursor.fetchall()
            cursor.execute("DELETE FROM main_sub_account_mapping", ())
            cursor.execute("DBCC CHECKIDENT('main_sub_account_mapping', RESEED, 0)", ())
            # cursor.fetchall()
            cursor.execute("DELETE FROM accounts", ())
            cursor.execute("DBCC CHECKIDENT('accounts', RESEED, 0)", ())
            # cursor.fetchall()

        conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)
