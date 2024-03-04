#-------------------------------------------------------------------------
# AUTHOR: William Go
# FILENAME: db_connection.py
# SPECIFICATION: Homework 2, Creating Documents and Categories with pgadmin
# FOR: CS 4250- Assignment #2
# TIME SPENT: 8 Hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
from psycopg2.extras import RealDictCursor

#Global object to count terms DELETE THIS
term_count = dict()

def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = "CPP"
    DB_USER = "postgres"
    DB_PASS = "pass"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")

def createCategory(cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    sql = "Insert into categories (catId, catName) Values ( %s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    sql = "Insert into documents (docId, docText, docTitle, docDate, docCat) " \
          "Values (%s, %s, %s, %s, %s)"

    recset = [docId, docText, docTitle, docDate, docCat]

    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    #Strip the String and put to lowercase
    newText = docText.lower()
    term = newText.strip()

    #Iterate all terms
    for x in term:
        num_chars = x.len()

        #Insert into terms
        sql = "Insert into terms (term, num_chars) Values ( %s, %s)"
        recset = [term, num_chars]
        cur.execute(sql, recset)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here
    #This will be use the joined index table

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    #THIS GOES THROUGH THE INDEX


    # 2 Delete the document from the database
    # --> add your Python code here
    sql = "Delete from documents where id = %(docId)s"
    cur.execute(sql, {'id': docId})

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    sql = "Insert into documents (docId, docText, docTitle, docDate, docCat) " \
          "Values (%s, %s, %s, %s, %s)"

    recset = [docId, docText, docTitle, docDate, docCat]

    cur.execute(sql, recset)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = ""

    cur.execute("SELECT users.name, comments.text, comments.datetime from comments inner join users on " 
                "comments.id_user = users.id order by datetime asc")

    recset = cur.fetchall()

    for rec in recset:
        index += rec['name'] + " | " + rec['text'] + " | " + str(rec['datetime']) + "\n"

    return index

#This is based on the chat.py version for db_connection
def createTables(cur, conn):
    try:

        sql = "create table categories(catId integer not null, catName character varying(255) not null, " \
              "constraint categories_pk primary key (catId))"
        cur.execute(sql)

        sql = "create table terms(term character varying(255) not null, num_chars integer not null, " \
              "constraint terms_pk primary key (term))"
        cur.execute(sql)

        sql = "create table documents(docId integer not null, docText character varying(255) not null, " \
              "docTitle character varying(255) not null, docDate date not null, " \
              "docCat character varying(255) not null, " \
              "constraint documents_pk primary key (docId), " \
              "constraint documents_categories_id_fkey foreign key (docCat) references categories (catId))"
        cur.execute(sql)

        #This is the joined table which will have info from terms and docs
        # sql = "create table index(docId integer not null, term character varying(255) not null, " \
        #       "docCat character varying(255) not null, " \
        #       "constraint index_pk primary key (docId) references documents (docId), " \
        #       "constraint index_pk primary key (term) references documents (term), " \
        #       "constraint index_categories_id_fkey foreign key (docCat) references documents (catId))"
        # cur.execute(sql)

        conn.commit()

    except:

        conn.rollback()
        print ("There was a problem during the database creation or the database already exists.")