from sqlite3 import Error
import sqlite3
 
 
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)
    #finally:
    #    conn.close()
  
def create_connection_memory():
    """ create a database connection to a database that resides
        in the memory(RAM) instead of a database file on disk.
    """
    try:
        conn = sqlite3.connect(':memory:')
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def list_table_columns(tablename, conn):
    # to list all column names in a table:
    sql = "SELECT * FROM %s" %tablename
    cursor = conn.execute(sql) 
    return [description[0] for description in cursor.description]

def list_table_names(conn):
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [str(name[0]) for name in res]

def add_to_ingredient_localisation(conn, data):
    for name in data:
        sql = '''INSERT INTO ingredient_localisation(name) VALUES(?) '''
        cur = conn.cursor()
        cur.execute(sql,(name,))

def add_to_ingredient_groups(conn,data):
    for name in data:
        sql = '''INSERT INTO ingredient_groups(name) VALUES(?) '''
        cur = conn.cursor()
        cur.execute(sql,(name,))

def add_to_compartments(conn,data):
    for name in data:
        sql = '''INSERT INTO compartments(name) VALUES(?) '''
        cur = conn.cursor()
        cur.execute(sql,(name,))


# create new database

database = "cellPackDatabase.db"
conn = create_connection(database)
 
sql_create_ingredients_table = """ CREATE TABLE IF NOT EXISTS ingredients(
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        source text,
                                        organism text,
                                        molarity real,
                                        protein_count integer,
                                        score text,
                                        uniprot_id text,
                                        localisation_id text,
                                        sphere_file text,
                                        group_id integer
                                    ); """

sql_create_ingredient_groups_table = """CREATE TABLE IF NOT EXISTS ingredient_groups(id integer PRIMARY KEY, name text NOT NULL);"""

sql_create_ingredient_localisation_table = """CREATE TABLE IF NOT EXISTS ingredient_localisation (id integer PRIMARY KEY, name text NOT NULL);"""

sql_create_compartments_table = """CREATE TABLE IF NOT EXISTS compartments(id integer PRIMARY KEY, name text NOT NULL, source text, type text, parent_id integer);"""

sql_create_ingredient_list_table = """CREATE TABLE IF NOT EXISTS ingredient_list(
                          ingredient_id integer NOT NULL,
                          compartment_id integer NOT NULL); """

sql_create_binding_partners = """CREATE TABLE IF NOT EXISTS binding_partners(id1 integer, id2 integer);"""

 
 
if conn is not None:
    # create  tables
    create_table(conn, sql_create_ingredients_table)
    create_table(conn, sql_create_ingredient_groups_table)    
    create_table(conn, sql_create_ingredient_localisation_table) 
    create_table(conn, sql_create_compartments_table)
    create_table(conn, sql_create_ingredient_list_table)
    create_table(conn, sql_create_binding_partners)

    # list all tables in the database:
    alltables = list_table_names(conn)
    print "ALLTABLES in db:", alltables
    cursor = conn.cursor()
    if "compartments" in alltables:
        print "Creating unique index in compartments table on 'name' "
        sql="CREATE UNIQUE INDEX idx_compartment_name ON compartments (name);"
        cursor.execute(sql)
    if "ingredients" in alltables:
        print "Creating unique index in ingredients table on 'name' and 'source'"
        sql="CREATE UNIQUE INDEX idx_ingrediens_name ON ingredients (name, source);"
        cursor.execute(sql)

    if "ingredient_localisation" in alltables:
        # add data to  ingredient_localisation:
        print "Creating unique index in ingredient_localisation table on 'name' "
        sql="CREATE UNIQUE INDEX idx_ingredient_localisation_name ON ingredient_localisation (name);"
        cursor.execute(sql)
        add_to_ingredient_localisation(conn, ["surface", "interior"])

    if "ingredient_groups" in alltables:
        print "Creating unique index in ingredient_group table on 'name' "
        sql="CREATE UNIQUE INDEX idx_ingredient_group_name ON ingredient_groups (name);"
        cursor.execute(sql)
        add_to_ingredient_groups(conn, ["proteins", "fibers"])

    if "ingredient_list" in alltables:
        print "Creating unique index in ingredient_list table on ingredient_id and compartment_id"
        sql="CREATE UNIQUE INDEX idx_ingredient_list ON ingredient_list (ingredient_id, compartment_id);"
        cursor.execute(sql)
        

    conn.commit()
        
else:
    print("Error! cannot create the database connection.")



#['name', 'source', 'organism', 'molarity', 'protein_count', 'score', 'binding_partner', 'uniprot_id' , 'localisation', 'positions', 'principal_vector', 'sphere_file',  'radii', 'group_id']





