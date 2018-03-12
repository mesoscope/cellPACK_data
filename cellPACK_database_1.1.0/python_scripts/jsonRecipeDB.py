import sys
import json
import sqlite3
from sqlite3 import Error

try:
    from beautifultable import BeautifulTable
    table_view = True
except:
    table_view = False

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

def list_table_columns(tablename, conn):
    # to list all column names in a table:
    sql = "SELECT * FROM %s" %tablename
    cursor = conn.execute(sql) 
    return [description[0] for description in cursor.description]

def list_table_names(conn):
    """Returns a list of all table names in the DB """
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [str(name[0]) for name in res]

def sql_query(conn, sql, vals=None, displayNRecords=None):
    """Returns sql query result records. Prints the result in a table form. (if 'beautifultable' module is available)  
    -- sql - a string with SQL query. Example 'SELECT * FROM ingredients WHERE group_id = ?;'
    -- vals - None or a tuple of values(one value for each '?' in the sql string)
    -- displayNRecords: None or 0                 -  do not print the result,
                        'all'                     -  print out all the records from the result
                        n (integer )              -  print n first records from the result,
                        [n, m] (list of two ints) - print from n to m (including) records from the result
    """
    cursor = conn.cursor()
    try:
        if vals is not None:
            assert isinstance(vals, tuple)
            cursor.execute(sql, vals)
        else:
            cursor.execute(sql)
    except Error as e:
        print "ERROR in sql_query():", e, "SQL:", sql, "VALUES:", vals
        return None
    res = cursor.fetchall()

    if displayNRecords and table_view:
        headers = [description[0] for description in cursor.description]
        if displayNRecords == "all":
            display_result(headers, res)
        elif isinstance(displayNRecords, int):
            nrec = min(len(res), displayNRecords)
            display_result(headers, res[:nrec])
        elif isinstance(displayNRecords, (list, tuple)):
            n, m = displayNRecords
            assert n < m or n==m
            assert n in range(0, len(res))
            assert n in range(0, len(res))
            display_result(headers, res[n:m+1])
            
    #print res
    return res

def add_row(tablename, data, conn):
    """data is a list of [columnname, value] pairs
    """
    sql = "INSERT INTO %s" % tablename
    columns, vals = zip(*data)
    colstr ="("
    valstr = "("
    for i, column in enumerate(columns):
        if i > 0:
            colstr += ", "
            valstr += ", "
        colstr += "%s" % column
        valstr += "?" 
    sql = sql + colstr +") VALUES" + valstr + ");"
    cur = conn.cursor()
    try:
        res = cur.execute(sql, tuple(vals))
    except Error as e:
        print "Error in add_row:", (e), ". SQL:",  sql, vals
        return None
    print "SQL:", sql, vals
    rowid = cur.lastrowid  
    conn.commit()
    return rowid


def display_result(headers, data):
    from beautifultable import BeautifulTable
    table = BeautifulTable()
    table._max_table_width = 100
    table.column_headers = headers
    for row in data:
        table.append_row(row)
    print (table)


KW_mapping={"name":"name","source":"pdb","organism":"organism",
            "molarity":"molarity","protein_count":"nbMol","uniprot_id":"",
            "sphere_file":"sphereFile",  "score":"score",
            "localisation_id":"","group_id":"",
            "binding_partners": "partners_name"}

class JsonRecipeParser:

    def __init__(self, database="cellPackDatabase.db"):
        self.database = database
        self.conn = create_connection(database)
        self.compartments = self.getCompartmentsDB()

    def getCompartmentsDB(self):
        """Query the database. Return a dictioanary  containing
        (compartment_name: id) pairs"""
        
        sql="SELECT name , id FROM compartments;"
        res = sql_query(self.conn, sql)
        #print "ALL COMPARTMENS in the db:" , res
        return dict(res)

    def parseJsonFile(self, filename):
        """Parse cpecified with 'filename'  json file. Add ingredients and compartments to the database"""
        f = open( filename ,"r")
        jsondic = json.load(f)
        rootid = None
        if "recipe" in jsondic:
            cname = jsondic["recipe"]["name"]
            rootid = self.addCompartmentToDB(compname=cname, parent=None)
            self.compartments[cname] = rootid
        if "cytoplasme" in jsondic:
            rnode = jsondic["cytoplasme"]
            cname=jsondic["recipe"]["name"]+"cytoplasme"
            if not self.compartments.has_key(cname):
                compid = self.addCompartmentToDB(compname=cname, parent=rootid)
                self.compartments[cname] = compid
            else:
                compid = self.compartments[cname]
            ingrs_dic = jsondic["cytoplasme"]["ingredients"]
            if len(ingrs_dic):
                for ing_name in ingrs_dic:  # ingrs_dic:
                    ing_dic = ingrs_dic[ing_name]
                    ing_id = self.addIngredientToDB(ing_dic, compid, checkInDB=True)

        if "compartments" in jsondic:
            if len(jsondic["compartments"]):
                for cname in jsondic["compartments"]:
                    compid = self.compartments.get(cname, None)
                    comp_dic = jsondic["compartments"][cname]
                    compid = self.parseOneCompartment(compid, cname, comp_dic, parent=rootid)
                    self.compartments[cname] = compid
        self.conn.commit()

    def getIngredientData(self, ingr_dic, is_surface=False):
        """ Return the dictionary of an ingredient to go in the table based on mapping
            and current info in the recipe (ingr_dic)"""
        
        table_dic={"name":None,"source":None,"organism":None,
                "molarity":0.0,"protein_count":0,"uniprot_id":None,
                "sphere_file":None,"localisation_id":2,"group_id":1, "binding_partners":[]}
        for k in KW_mapping:
            if KW_mapping[k] in ingr_dic :
                val = ingr_dic[KW_mapping[k]]
                if val is not None:
                    if val == "None": val=None
                    elif hasattr (val, "__len__") and len(val) == 0:
                        val = None
                if val is not None:
                    table_dic[k] = ingr_dic[KW_mapping[k]]
        #localisation
        if is_surface:
            table_dic["localisation_id"]=1
        #group_id
        if ingr_dic["Type"] == "Grow":
            table_dic["group_id"]=2
        return table_dic

    def addCompartmentToDB(self, compname="compName", parent=None):
        """Add compartment to the DB. Return its id from the compartments table."""
        # query the table "compartments" for name , find its Id
        #
        data = [["name", compname],]
        if parent is not None:
            data.append(["parent_id", parent])
        cid = add_row ("compartments", data, self.conn)
        if cid is not None:
            print ("added compartment to db:", compname, cid, "parent_id:", parent)
        else:
            print "ERROR in addCompartmentToDB", compname, data
        #print (name,cid)
        return cid

    def addIngredientToDB(self, ing_dic, compid, is_surface=False, checkInDB=False):
        """Add ingredient to the DB. Fill fields in the 'ingredients', 'ingredient_list'
           and 'binding_partners' tables."""
        
        table_dic = self.getIngredientData(ing_dic, is_surface=is_surface)
        ingredName =  ing_dic["name"]

        # if checkInDB==True query data base for name (find its 'id', 'source' and 'group_id' if it is there)
        # if such record(s) is NOT found add a new record (take key-value pairs from ing_dict, where
        #      values are not None)
        # else if a record with cpecified name exists - check if the group_id and source fields are filled.
        #    If they are Null - add field values for the record.
        binding_partners = []
        if table_dic.has_key("binding_partners"):
            binding_partners = table_dic.pop("binding_partners")
        fields = []
        values = []
        data =[]
        for k, val in table_dic.items():
            if val is not None:
                data.append([k, val])
                fields.append(k)
                values.append(val)
        ing_id = None
        if checkInDB:
            sql = "SELECT id, group_id, source FROM ingredients WHERE name = ?;"
            res = sql_query (self.conn, sql, (ingredName, ))
            source = table_dic['source']
            if not len(res): # did not find any records with ingredName - add a new record
                ing_id = add_row("ingredients", data, self.conn)
            else: # there are some records with the name
                #import pdb; pdb.set_trace()
                for r in res:
                    if r[1] is None and r[2] is None:
                        # There is a record without a group_id and source: will add data to it
                        ing_id = r[0]
                        print "UPDATING ingredients table for name %s, id %d" % (ingredName, r[0])
                        sql = "UPDATE ingredients SET "
                        for i, field in enumerate(fields):
                            if i > 0:
                                sql += ", "
                            sql += "%s = ?" % field
                        sql += " WHERE name = ?;"
                        values.append(ingredName)
                        cursor = self.conn.cursor()
                        try:
                            cursor.execute(sql, tuple(values))
                        except Error as e:
                            print "Error in addIngredientToCompartment", (e), "SQL:", sql, values
                            return None
                        break
                    elif r[1] is not None and r[2] == source:
                        ing_id = r[0] # this is the id of existing record in the table with the same name and source field values as the ingredient from table_dic (will not overwrite it)
                        break
                    elif r[1] is not None and r[2] != source:
                        ing_id = add_row("ingredients", data, self.conn)
                        break
        else: # add new record without checking
            ing_id = add_row("ingredients", data, self.conn)
            
        if ing_id is not None:
            #print ("ingred", ing_dic["name"],ing_id, "in comp:", compid)
            #print (ing_dic["name"],ing_id,table_dic)
            #print
            # check if the record with the ing_id and compi exists in the ingredients_list table
            sql = "SELECT ingredient_id, compartment_id FROM ingredient_list WHERE ingredient_id=? AND compartment_id=?"
            res = sql_query(self.conn, sql, (ing_id, compid))
            if not len(res):
                # UPDATE ingredient_list table
                add_row("ingredient_list", [["ingredient_id", ing_id], ["compartment_id", compid]], self.conn)
            if len(binding_partners):
                # find out if the binding partner's name exists in the ingredients table:
                # if not : add it to the table (fill out only the "name" field, leaving all other records NULL) and get its Id
                # add record (ind_id, partner_id  to "binding_partners")
                for pname in binding_partners:
                    sql = "SELECT id FROM ingredients WHERE name = ?;"
                    res = sql_query (self.conn, sql, (pname,))
                    if not len(res):
                        # add this ingredient to the ingredients table (only the "name" field)
                        partner_id = add_row("ingredients", [["name", pname]], self.conn)
                    else:
                        partner_id = res[0][0]
                    # check if such pair exists:
                    sql = "SELECT id1, id2 FROM binding_partners WHERE (id1=? AND id2=?) OR (id1=? AND id2=?);"
                    res = sql_query(self.conn, sql, (ing_id, partner_id, partner_id, ing_id))
                    if not len(res):
                        add_row("binding_partners",[["id1", ing_id], ["id2", partner_id]], self.conn)
        else:
            print "WARNING in addIngredientToDB: did not add/update ingredient table with %s" % ingredName
                        
        return ing_id

    def parseOneCompartment(self, compid, name, comp_dic,
                            parent=None):
        """Parses the dictioanry for one compartment. Adds its ingredients to the DB"""
        if "name" in comp_dic :
            name = str(comp_dic["name"])
        if compid is None:
            compid = self.addCompartmentToDB(compname=name, parent=parent)
            if compid is None:
                print "EROOR in parseOneCompartment, compname=%s", name
                return
        if "surface" in comp_dic:
            snode = comp_dic["surface"]
            ingrs_dic = snode["ingredients"]
            if len(ingrs_dic):
                for ing_name in ingrs_dic:  
                    ing_dic = ingrs_dic[ing_name]
                    ing_id = self.addIngredientToDB(ing_dic, compid, is_surface=True, checkInDB=True)
        if "interior" in comp_dic:
            snode = comp_dic["interior"]
            ingrs_dic = snode["ingredients"]
            if len(ingrs_dic):
                for ing_name in ingrs_dic: 
                    ing_dic = ingrs_dic[ing_name]
                    ing_id = self.addIngredientToDB(ing_dic, compid, is_surface=False, checkInDB=True)
        for k in comp_dic :
            # looking for subcompartments:
            if k not in ["surface", "interior", "geom", "name"]:
                #import pdb; pdb.set_trace()
                compid=self.parseOneCompartment(self.compartments.get(k, None), k,
                                                comp_dic[k], parent=compid)
        return compid

    def find_ingredient(self, name, fields=[], like=False):
        """Return records from ingredient table for specified name field.
        ---fields is a list of field values that the record will contain.
        -- if like is True, name is considered as a string pattern used to specify
            the name (may contain wildcards);
        Wildcards:
            % (percent sign) represents zero, one, or more characters.
            _ (underscore) represents exactly one character. """

        if not len(fields): # return all fields
            if like:
                sql = "SELECT * FROM ingredients WHERE name LIKE ?"
            else:
                sql = "SELECT * FROM ingredients WHERE name = ?"
        else:
            sql = "SELECT "
            for count, fild_name in enumerate(fields):
                if count > 0:
                    sql += ", "
                sql += "%s"% fild_name
                count += 1
            sql += " FROM ingredients WHERE "
            if like:
                sql += "name LIKE ?;"
            else:
                sql += "name = ?;"
        res = sql_query (self.conn, sql, (name, ))
        return res

    def add_binding_partners_by_name(self, ingred_name, partner_names=[], checkExists=True):
        """Add binding partner to the binding_partners table. Use values from 'name' field
        to specify the ingredient  and its partners
        -- ingred_name-- name of the ingredient
        -- partner_names -- list of partners names
        """
        
        ingred_id = self.find_ingredient_id(ingred_name)
        if ingred_id is None:
            print "add_binding_partners_by_name(): could not select ingredient %s" % ingred_name
            return
        for pname in partner_names:
            partner_id =  self.find_ingredient_id(ingred_name)
            if partner_id is None:
                print "add_binding_partners_by_name(): could not add binding partner %s" % pname
                continue
            self.add_binding_partner_id(ingred_id, partner_id, checkExists=checkExists)

    def add_binding_partner_id(self, ingred_id, partner_id, checkExists=True):
        """Add binding partner (using id ) to the binding_partners table """
        if checkExists:
            sql = "SELECT id1, id2 FROM binding_partners WHERE (id1=? AND id2=?) OR (id1=? AND id2=?);"
            res = sql_query(self.conn, sql, (ingred_id, partner_id, partner_id, ingred_id))
            if res is not None and not len(res): # no records found
                add_row("binding_partners",[["id1", ingred_id], ["id2", partner_id]], self.conn)
        else: #add new record without checking
            add_row("binding_partners",[["id1", ingred_id], ["id2", partner_id]], self.conn)

    def find_ingredient_id(self, name, like=False):
        """return the 'id' field of the ingredient <name>.
        If 'like' is True , then <name> is a string pattern used to specify the 'name' field value in the SQL query."""
        res = self.find_ingredient(name, fields=["id"], like=like)
        if res is None: # ERROR 
            return None
        if not len(res): # did not find any matching records
            print "find_ingredient_id(): No matching records in ingredient table for name %s:" % name
            return None
        return res[0][0]
        
    def find_ingredient_partners(self, name, ingred_id=None):
        """Return name and id of specified ingredient's binding partners"""
        if ingred_id is None:
            ingred_id = self.find_ingredient_id(name)
            if ingred_id is None:
                print "In find_ingredient_partners() FAILED to SELECT id of ingredient %s" % name
                return
        sql = "SELECT id1 FROM binding_partners WHERE id2=? UNION SELECT id2 FROM binding_partners WHERE id1=?"
        partner_ids = sql_query (self.conn, sql, (ingred_id, ingred_id))
        partners = []
        for rec in partner_ids:
            sql = "SELECT name, id FROM ingredients WHERE id = ?;"
            res = sql_query (self.conn, sql, (rec[0], ))
            partners.append(res[0])
        return partners

    def get_field_from_field(self, field1, field2 , value, tablename="ingredients"):
        """Get record(s) from field <field1> from table <tablename> with condition
        specified by the <value> of the field <field2>."""
        sql = "SELECT %s FROM %s WHERE %s = ?" % (field1, tablename, field2)
        res = sql_query (self.conn, sql, (value, ))
        return res

    def update_ingredient_field(self, field, value, ingredname, ingred_id=None):
        """Update the value in the speccified field <field> of the ingredient with
        <ingredname> 'name' field  or <ingred_id> 'id' field.
        If <ingred_id> is not None - it is used in the sql query instead of the name"""
        if ingred_id is not None:
            sql = "UPDATE ingredients SET %s = ? WHERE id = ?" % field
            res = sql_query (self.conn, sql, (value, ingred_id))
        else:
            sql = "UPDATE ingredients SET %s = ? WHERE name = ?" % field
            res = sql_query (self.conn, sql, (value, ingredname))

    def find_ingredients_in_compartment(self, compname, comp_id=None, localisation_id=None):
        """ Returns a list of [ingred_name, ingred_id] lists of ingredients that belong to
        compartment <compname>.
        If <comp_id> is not None - it is used in the sql query."""

        if comp_id is None:
            sql = "SELECT id FROM compartments WHERE name=?;"
            res = sql_query(self.conn, sql, (compname,))
            comp_id = res[0][0]

        sql =  "SELECT a.name, a.id  FROM ingredients AS a INNER JOIN ingredient_list AS b ON a.id=b.ingredient_id WHERE b.compartment_id = ?"
        values = (comp_id,)
        if localisation_id is not None:
            sql += " AND a.localisation_id = ?;"
            values = (comp_id,localisation_id) 
        res = sql_query(self.conn, sql, values)
        return res

    def find_ingred_compartment(self, ingredname, ingred_id=None):
        """Return [name, id] of the compartmet(s) the ingredient belongs to.
        If <ingred_id> is not None - it is used in the sql query instead of the ingredient name"""
        pass

    def remove_ingred_fromDB(self,ingredname, ingred_id=None):
        """ """
        pass
    

#get uniprot id
global jsonParser         
if __name__ == "__main__":
    fin = "BloodHIVMycoRB.1.0_full1.json"
    if len(sys.argv)>1:
        fin = sys.argv[1]
    #fin = "Mpn_1.0.json"
    jsonParser = JsonRecipeParser()
    jsonParser.parseJsonFile(fin)
    #import pdb; pdb.set_trace()
