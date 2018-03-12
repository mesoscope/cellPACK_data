import sqlite3
database = "cellPackDatabase.db"
from jsonRecipeDB import create_connection , list_table_columns, list_table_names, sql_query, add_row, JsonRecipeParser

try:
    from beautifultable import BeautifulTable
    table_view = True
except:
    table_view = False

jsonParser=JsonRecipeParser("cellPackDatabase.db")
igr_dict =  {
            "molarity": 0,
            "useOrientBias": False,
            "packingPriority": -3,
            "radii": [ [ 35.56, 36.43 ] ],
            "pdb": "2btf",
            "jitterMax": [ 1, 1, 1 ],
            "cutoff_boundary": 0,
            "positions": [
              [
                [ 6.3, 17.58, 5.74 ],
                [ -4.58, -12.78, -4.17 ]
              ]
            ],
            "partners_weight": 0,
            "encapsulatingRadius": 46,
            "name": "profilinactin",
            "orientBiasRotRangeMax": -3.1415927,
            "partners_name": None,
            "organism": "",
            "Type": "MultiSphere",
            "coordsystem": "left",
            "partners_position": None,
            "use_mesh_rb": False,
            "proba_binding": 0.5,
            "excluded_partners_name": None,
            "gradient": "",
            "rejectionThreshold": 30,
            "nbJitter": 5,
            "perturbAxisAmplitude": 0.1,
            "weight": 0.2,
            "meshFile": "profilinactin.dae",
            "placeType": "pandaBullet",
            "proba_not_binding": 0.5,
            "nbMol": 280,
            "principalVector": [ 0, 1, 0 ],
            "score": "",
            "useRotAxis": False,
            "offset": [ 0, 0, 0 ],
            "overwrite_nbMol_value": 280,
            "cutoff_surface": 103,
            "isAttractor": False,
            "properties": {},
            "source": {
              "pdb": "2btf",
              "transform": { "center": True }
            },
            "packingMode": "random",
            "orientBiasRotRangeMin": -3.1415927,
            "rotRange": 6.2831
          }


conn = jsonParser.conn

def getCompartments():
    ### print out the names , id, af all compartments in the DB and the number of ingredients in each compartment
    sql = "SELECT * FROM compartments"
    res = sql_query(conn, sql, displayNRecords="all")
    if not table_view:
        print "ALL COMPARTMENTS", res
    sql = "SELECT name ,id FROM compartments"
    res = sql_query(conn, sql, displayNRecords=0)
    cdict = dict(res)
    ingrComp = {}
    for cname, cid in cdict.items():
        sql = "SELECT a.name, a.id, a.localisation_id FROM ingredients AS a INNER JOIN ingredient_list AS b ON a.id=b.ingredient_id WHERE b.compartment_id = ?;"
        res = sql_query(conn, sql, (cid,), 0)
        ingrComp[cname] = res
    print 
    print "COMPARTMENT NAME  | Number of ingredients"
    for cname, ingr in ingrComp.items():
        ningr = len(ingr)
        print cname, ningr
    print
    print "================================================="
    #print  ingrComp
    
def getAllIngredients():
    sql = "SELECT Count(*) FROM ingredients"
    res = sql_query(conn, sql, displayNRecords=0)
    print  "Number of all ingredients in the table:", res
    print
    sql = "SELECT id, name, source FROM ingredients"
    res = sql_query(conn, sql, displayNRecords="all")
    if not table_view:
        print "id, name, source of all ingredients", res
    dd = {}
    duplicates = []
    for record in res:
        if not dd.has_key(record[1]):
            dd[record[1]] = record
        else:
            rr = dd[record[1]]
            duplicates.append([(rr[1], rr[0]), (record[1], record[0])])
    print "Records with the same name:",  duplicates
    print
    print "================================================="
    return res

def getPartners():
    #shoe all records from binding_partners table
    print "BINDING PARTNERS"
    sql = "select * from binding_partners"
    res = sql_query(conn, sql, displayNRecords="all")
    if not table_view:
        print "ID:", res
    print "Partner 1 | Partner 2"
    print "---------------------"
    for record in res:
        ing_id, partn_id = record
        ingr_name = jsonParser.get_field_from_field("name", "id", ing_id)[0][0]
        partn_name = jsonParser.get_field_from_field("name", "id", partn_id)[0][0]
        print ingr_name , "  |  ", partn_name
        
    print "================================================="
    return res

def test_field_from_field():
    print "Find field_from_field",
    print " source from protein_count=200"
    print jsonParser.get_field_from_field("source", "protein_count", 200)
    #[(u'2zjs_aligned',), (u'4av3_aligned',), (u'HIV1_P6_VPR.pdb',)]
    onSurf = jsonParser.get_field_from_field("name", "localisation_id", 1)
    print
    print "surface ingreds:",
    print onSurf, len(onSurf)
    print 
    interior = jsonParser.get_field_from_field("name", "localisation_id", 2)
    print "interior ingreds:",
    print interior, len(interior)
    print
    print "molarity from name tetramer1A49"
    print jsonParser.get_field_from_field("molarity", "name",  'tetramer1A49')
    print
    print "================================================="
    
def test_addIngred():
    # remove the last ingredient from the table
    print "remove ingredient id 73 from the table"
    ing_id = 73
    sql = "SELECT compartment_id from ingredient_list where ingredient_id = 73"
    comp_id = sql_query(conn, sql)[0][0]
    sql = "DELETE FROM ingredients where id=?;"
    sql_query(conn, sql, (ing_id,))


    # add it using the ingredient dictionary (copied from a json file)
    print "add  the same ingredient %s" % igr_dict["name"] 
    newing_id = jsonParser.addIngredientToDB(igr_dict , comp_id , is_surface=False, checkInDB=True)
    assert ing_id == ing_id
    print "trying to add it again should fail" 
    #another try to add the same ingredient (without checking if it is there) should fail
    newing_id = jsonParser.addIngredientToDB(igr_dict , comp_id , is_surface=False)
    assert newing_id == None
    sql = """SELECT  id, name, source, organism, molarity, protein_count, score, group_id FROM ingredients WHERE id=?;"""
    res = sql_query(conn, sql, (ing_id,), displayNRecords="all")
    print res
    print "================================================="

def test_getPartner():
    partners = jsonParser.find_ingredient_partners("DNA")
    print "partners of DNA:", partners 
    partners = jsonParser.find_ingredient_partners("ribosome_low")
    print "partners of ribosome_low:", partners
    print "================================================="
    
def testUpdateIngred():
    name = 'hexamer1EIY'
    ing_id=jsonParser.find_ingredient_id("hexamer%", like=True)
    spfile = jsonParser.get_field_from_field("sphere_file", "id", ing_id) [0][0]
    #'1EIY_6mer1.sph'
    spfile1 = '1EIY_6mer1_111.sph'
    jsonParser.update_ingredient_field("sphere_file",spfile1 , "", ingred_id=ing_id)
    assert jsonParser.get_field_from_field("sphere_file", "id", ing_id) [0][0] == spfile1
    jsonParser.update_ingredient_field("sphere_file",spfile , "", ingred_id=ing_id)
    assert jsonParser.get_field_from_field("sphere_file", "id", ing_id) [0][0] == spfile


def fetch_tree(rootName="BloodHIVMycoRB.1.0"):
    #rootName = "BloodHIVMycoRB.1.0" # recipe name - root compartment

    # get all compartments from the root

    sql =  """SELECT l1.name l1_name, l2.name l2_name,
              l3.name l3_name, l4.name l4_name,
              l5.name l5_name
               FROM compartments l1
              LEFT JOIN compartments l2
                ON l2.parent_id = l1.id
              LEFT JOIN compartments l3
                ON l3.parent_id = l2.id
              LEFT JOIN compartments l4
                ON l4.parent_id = l3.id
              LEFT JOIN compartments l5
                ON l5.parent_id = l4.id
              WHERE l1.name = ?;"""
    res = sql_query(conn, sql, (rootName,),)
    treeDict = {}
    compDict = treeDict
    for record in res:
        print ""
        indent = ""
        parent = None
        parentDict = treeDict
        for comp_name in record[1:]:
            if comp_name is None: break
            if not parent:
                parentDict[comp_name] = {}
                compDict = parentDict[comp_name]
                
            else:
                indent += "    "
                #import pdb; pdb.set_trace()
                compDict[comp_name] = {}
                parentDict = compDict
                compDict = parentDict[comp_name]
            print indent, "COMPARTMENT: ", comp_name
            parent = comp_name
            # get compartment_id
            sql = "SELECT id FROM compartments WHERE name=?;"
            comp_id = sql_query(conn, sql, (comp_name,))[0][0]

            sql = "SELECT  a.id, a.name, a.source, a.group_id, a.molarity FROM ingredients AS a INNER JOIN ingredient_list AS b ON a.id=b.ingredient_id WHERE b.compartment_id = ? and a.localisation_id = ?;"
            surf_ing = sql_query(conn, sql, (comp_id, 1),)
            if len(surf_ing):
                print indent, "    SURFACE ingredients [%d]:" %len(surf_ing),
                compDict['surface']={}
                for ingred in surf_ing:
                    ingr_ind = ingred[0] # can be used to find binding partners
                    name = ingred[1]
                    print name,
                    compDict['surface'][name] = {"source": ingred[2], "group_id" :ingred[3],
                                                            "molarity":ingred[4]}
            inter_ing = sql_query(conn, sql, (comp_id, 2),)
            if len(inter_ing):
                print ""
                print indent, "    INTERIOR ingredients [%d]:" %len(inter_ing),
                compDict['interior']={}
                for ingred in inter_ing:
                    ingr_ind = ingred[0] # can be used to find binding partners
                    name = ingred[1]
                    print name,
                    compDict['interior'][name] = {"source": ingred[2], "group_id" :ingred[3],"molarity":ingred[4]}

            print ""
    #import pdb; pdb.set_trace()
                    

if __name__ == "__main__":
    getCompartments()
    getAllIngredients()
    getPartners()
    test_field_from_field()
    #test_addIngred()
    test_getPartner()
    #testUpdateIngred()
    fetch_tree("BloodHIVMycoRB.1.0")
    #fetch_tree('MycoPn')
