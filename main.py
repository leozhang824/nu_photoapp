
#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   Leo Zhang
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  print()
  print(">> Enter a command:")
  print("   0 => end")
  print("   1 => stats")
  print("   2 => users")
  print("   3 => assets")
  print("   4 => download")
  print("   5 => download and display")
  print("   6 => upload")
  print("   7 => add user")

  cmd = int(input())
  return cmd


###################################################################
#
# stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try:
    print("S3 bucket name:", bucketname)

    assets = bucket.objects.all()
    print("S3 assets:", len(list(assets)))

    #
    # MySQL info:
    #
    print("RDS MySQL endpoint:", endpoint)

    #
    # sql query for # of users and assets
    #
    sql = """
    select count(userid) from users
    """
    sql1 = """
    select count(assetid) from assets;
    """
    #
    # output # of users and assets
    #
    row = datatier.retrieve_one_row(dbConn, sql)
    asset = datatier.retrieve_one_row(dbConn, sql1)
    if row is None or asset is None:
      print("Database operation failed...")
    elif row == () or asset == ():
      print("Unexpected query failure...")
    else:
      print("# of users:", row[0])
      print("# of assets:", asset[0])
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# users
#
def users(dbConn):
  """
  Prints out a list of user info: userid, email, name 
  and folder in descending order by userid in the database
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  try:
    #
    # sql query for list of users:
    #
    sql = """
    select userid, email, lastname, firstname, bucketfolder from users
    order by userid desc;
    """
    #
    # output list of users
    #
    rows = datatier.retrieve_all_rows(dbConn, sql)
    if rows is None:
      print("Database operation failed...")
    elif rows == ():
      print("Unexpected query failure...")
    else:
      for row in rows:
        print("User id:", row[0])
        print("  Email:", row[1])
        print("  Name:", row[2], ",", row[3])
        print("  Folder:", row[4])
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# assets
#
def assets(dbConn):
  """
  Prints out a list of asset info: assetid, userid, original 
  name, and key name in descending order by assetid in the database
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  try:
    #
    # sql query for list of users:
    #
    sql = """
    select assetid, userid, assetname, bucketkey from assets
    order by assetid desc;
    """
    #
    # output list of users
    #
    rows = datatier.retrieve_all_rows(dbConn, sql)
    if rows is None:
      print("Database operation failed...")
    elif rows == ():
      print("Unexpected query failure...")
    else:
      for row in rows:
        print("Asset id:", row[0])
        print("  User id:", row[1])
        print("  Original name:", row[2])
        print("  Key name:", row[3])
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# download and if display
#
def download(bucket, dbConn, display):
  """
  Inputs a asset id, looks up within the database, and downloads the
  corresponding file and renames it based on its original name if it exists
  in the database. Else, it outputs an error mesagae 
  
  Parameters
  ----------
  bucket: S3 boto bucket object
  dbConn: open connection to MySQL server
  display: if command asks to display file
  
  Returns
  -------
  nothing
  """

  try:
    #
    # sql query for retrieving specfic asset:
    #
    sql = """
    select assetname, bucketkey from assets
    where assetid = %s;
    """
    #
    # asks for asset id
    #
    print("Enter asset id>")
    s = input()

    #
    # output list of users
    #
    row = datatier.retrieve_one_row(dbConn, sql, [s])
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("No such asset...")
    else:
      #
      # downloads and renames file
      #
      file = awsutil.download_file(bucket, row[1])
      os.rename(file, row[0])
      print("Downloaded from S3 and saved as '", row[0],"'")
      #
      # display file if asked
      #
      if (display):
        image = img.imread(row[0])
        plt.imshow(image)
        plt.show()
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# upload
#
def upload(bucket, dbConn):
  """
  Inputs a file name and a userid, the uplaods said file to the folder 
  under given userid. File is then given an id and asset info (assetid, 
  userid, original name, and key name) is inserted to database
  
  Parameters
  ----------
  bucket: S3 boto bucket object
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  try:
    #
    # ask for filename input and check if valid
    #
    print("Enter local filename>")
    file = input()
    if not os.path.exists(file):
      print("Local file '", file, "'does not exist...")
      return
    #
    # ask for userid input and check if valid
    #
    print("Enter user id>")
    user = input()
    sql = """
    select userid, bucketfolder from users
    where userid = %s;
    """
    check = datatier.retrieve_one_row(dbConn, sql, [user])
    if check == ():
      print("No such user...")
      return
    #
    # create key and upload
    #
    key = check[1] + "/" + str(uuid.uuid4()) + ".jpg"
    key = awsutil.upload_file(file, bucket, key)
    #
    # sql query for updating asset
    #
    sql = """
    insert into assets(userid, assetname, bucketkey)
    values (%s, %s, %s);
    """
    datatier.perform_action(dbConn, sql, [user, file, key])
    
    sql = """
    select LAST_INSERT_ID();
    """
    row = datatier.retrieve_one_row(dbConn, sql)
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("Unexpected query failure...")
    else:
      print("Uploaded and stored in S3 as '", key,"'")
      print("Recorded in RDS under asset id", row[0])
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# add_user
#
def add_user(bucket, dbConn):
  """
  Inputs user info (email, last name, and first name), where
  a new user is created and then inserted to database, specifcally
  the users table.
  
  Parameters
  ----------
  bucket: S3 boto bucket object
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """

  try:
    #
    # ask for basic input: email and name
    #
    print("Enter user's email>")
    email = input()
    print("Enter user's last (family) name>")
    last = input()
    print("Enter user's first (given) name>")
    first = input()

    #
    # create bucket folder
    #
    folder = str(uuid.uuid4())
    #
    # sql query for inserting users
    #
    sql = """
    insert into users(email, lastname, firstname, bucketfolder)
    values (%s, %s, %s, %s);
    """
    datatier.perform_action(dbConn, sql, [email, last, first, folder])
    
    sql = """
    select LAST_INSERT_ID();
    """
    row = datatier.retrieve_one_row(dbConn, sql)
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("Unexpected query failure...")
    else:
      print("Recorded in RDS under user id", row[0])
  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-read-only.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  # stats
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  # users
  elif cmd == 2:
    users(dbConn)
  # assets
  elif cmd == 3:
    assets(dbConn)
  # download
  elif cmd == 4:
    download(bucket, dbConn, False)
  # download and display
  elif cmd == 5:
    download(bucket, dbConn, True)
  # upload
  elif cmd == 6:
    upload(bucket, dbConn)
  # add user
  elif cmd == 7:
    add_user(bucket, dbConn)
  #
  else:
    print("** Unknown command, try again...")
  #
  cmd = prompt()

#
# done
#
print()
print('** done **')