# Anna Omelchenko 
# this script creates the database "cellPackDatabase.db" in the current directory and adds tables to it:

 >> python python_scripts\create_cellPack_database.py

# this script parses "BloodHIVMycoRB.1.0_full1.json" or a given recipe filename file and fills the 
'compartments',  'ingredients' , 'binding_partners' and 
'ingredient_list' tables:

>> python python_scripts\jsonRecipeDB.py
>> python python_scripts\jsonRecipeDB.py ./recipe/BloodHIVMycoRB.1.0_full1.json

# this script shows how to get some info from the DB:
 >> python python_scripts\test_cellPackDB.py