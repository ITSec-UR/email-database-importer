#!/usr/bin/python
import os
import shutil
import csv
import MySQLdb as mariadb
import itertools
import warnings
import subprocess


nfs_host = os.environ.get('NFS_HOST')
nfs_path = os.environ.get('NFS_PATH')
nfs_mount_path = '/tmp/nfs-mount'

database_host = os.environ.get('DATABASE_HOST')
database_name = os.environ.get('DATABASE_NAME')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')

import_folder = os.environ.get('IMPORT_FOLDER')
processed_folder = os.environ.get('PROCESSED_FOLDER')
assert os.path.exists(import_folder)
assert os.path.exists(processed_folder)


os.makedirs(nfs_mount_path, exist_ok=True)
if os.path.ismount(nfs_mount_path):
   subprocess.check_call(['umount', nfs_mount_path])
subprocess.check_call(['mount', '-t', 'nfs', '-o', 'nolock', nfs_host + ':' + nfs_path, nfs_mount_path])


mariadb_connection = mariadb.connect(host=database_host, user=database_user, password=database_password, database=database_name)
cursor = mariadb_connection.cursor()
mariadb_connection.autocommit(False)
warnings.filterwarnings('ignore', category=mariadb.Warning)


remote_files = [file for file in os.listdir(nfs_mount_path)
                if os.path.isfile(os.path.join(nfs_mount_path, file))]
files_to_import = [file for file in os.listdir(import_folder)
                   if os.path.isfile(os.path.join(import_folder, file))]
processed_files = [file for file in os.listdir(processed_folder)
                   if os.path.isfile(os.path.join(processed_folder, file))]


files_to_transfer = [file for file in remote_files
                     if file not in files_to_import + processed_files]
for transfer_idx, transfer_file in enumerate([os.path.join(nfs_mount_path, file) for file in files_to_transfer]):
    print("\rTransfering file %s (%d of %d)" % (os.path.basename(transfer_file), transfer_idx + 1, len(files_to_transfer)))
    shutil.copy2(transfer_file, import_folder)


files_to_import = [file for file in os.listdir(import_folder)
                   if os.path.isfile(os.path.join(import_folder, file))
                   and file not in processed_files]
if len(files_to_import) > 0:
    cursor.execute("""SELECT id FROM users""")
    known_user_ids = set([row[0] for row in cursor.fetchall()])

for file_idx, file_to_import in enumerate([os.path.join(import_folder, file) for file in files_to_import]):
    print("\rImporting file %s (%d of %d)" % (os.path.basename(file_to_import), file_idx + 1, len(files_to_import)))
    with open(file_to_import) as csv_file:
        csv_data = list(csv.reader(csv_file, delimiter=','))

        new_user_ids = set(itertools.chain.from_iterable([[int(row[1]), int(row[2])] for row in csv_data])) - known_user_ids
        for uuid_idx, new_user_id in enumerate(new_user_ids):
            if (uuid_idx + 1) % 1000 == 0 or uuid_idx == len(new_user_ids) - 1:
                print("\rImporting user %d of %d..." % (uuid_idx + 1, len(new_user_ids)), end='')
            cursor.execute("""INSERT IGNORE INTO users (id)
                              VALUES (%s)""", (new_user_id,))
        known_user_ids |= new_user_ids

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

    shutil.move(file_to_import, processed_folder)

files_to_delete = [file for file in os.listdir(import_folder)
                   if os.path.isfile(os.path.join(import_folder, file))
                   and file in processed_files]
for file_to_delete in [os.path.join(import_folder, file) for file in files_to_delete]:
    os.remove(file_to_delete)