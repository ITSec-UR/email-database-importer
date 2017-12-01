#!/usr/bin/python
import os
import shutil
import csv
import MySQLdb as mariadb
import itertools
import warnings

database_host = os.environ.get('DATABASE_HOST')
database_name = os.environ.get('DATABASE_NAME')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')

import_folder = os.environ.get('IMPORT_FOLDER')
export_folder = os.environ.get('EXPORT_FOLDER')
assert os.path.exists(import_folder)
assert os.path.exists(export_folder)

mariadb_connection = mariadb.connect(host=database_host, user=database_user, password=database_password, database=database_name)
cursor = mariadb_connection.cursor()
mariadb_connection.autocommit(False)
warnings.filterwarnings('ignore', category=mariadb.Warning)

import_files = [os.path.join(import_folder, file) for file in os.listdir(import_folder)]
for file_idx, import_file in enumerate(import_files):
    print("\rImporting file %d of %d" % (file_idx + 1, len(import_files)))
    with open(import_file) as csv_file:
        csv_data = list(csv.reader(csv_file, delimiter=','))

        unique_user_ids = set(itertools.chain.from_iterable([[row[1], row[2]] for row in csv_data]))
        for uuid_idx, unique_user_id in enumerate(unique_user_ids):
            if (uuid_idx + 1) % 1000 == 0 or uuid_idx == len(unique_user_ids) - 1:
                print("\rImporting user %d of %d..." % (uuid_idx + 1, len(unique_user_ids)), end='')
            cursor.execute("""INSERT IGNORE INTO users (id) 
                              VALUES (%s)""", (unique_user_id,))

        for row_idx, row in enumerate(csv_data):
            if (row_idx + 1) % 1000 == 0 or row_idx == len(csv_data) - 1:
                print("\rImporting email %d of %d..." % (row_idx + 1, len(csv_data)), end='')

            assert len(row) == 5

            mail_timestamp = int(row[0], 16)
            sender_id = row[1]
            recipient_id = row[2]
            size = row[3]
            spam = row[4]

            cursor.execute("""INSERT INTO emails (sent_time, sender_id, recipient_id, size, spam) 
                              VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s)""", (mail_timestamp, sender_id, recipient_id, size, spam,))

        mariadb_connection.commit()

    shutil.move(import_file, export_folder)

