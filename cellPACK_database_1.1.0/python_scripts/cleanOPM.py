# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 09:24:06 2018
Filter opm PDB file to remove the beads, convert to mmtf ?
use of https://github.com/schrodinger/simplemmtf-python
also check https://github.com/eedlund/Utils/tree/master/BioUtils
@author: ludov
"""
import os
from simplemmtf import mmtfstr, from_atoms, MmtfDict, assert_consistency,_get_array_length

def pdb2mmtf(handle):
    '''
    @type handle: open file handle for reading
    @rtype: bytes
    '''
    def atom_gen():
        state = 0
        for line in handle:
            rec = line[:6]
            if rec == 'MODEL ':
                state += 1
            elif rec in ('ATOM  ', 'HETATM'):
                if (line[17:20]=="DUM"):continue
                yield {
                    u'modelIndex': state,
                    u'atomId': int(line[6:11]),
                    u'atomName': mmtfstr(line[12:16].strip()),
                    u'altLoc': ' ',#mmtfstr(line[16:17].rstrip()),
                    u'groupName': mmtfstr(line[17:20].strip()),
                    u'chainName': mmtfstr(line[21:22].rstrip()),
                    u'groupId': int(line[22:26]),
                    u'secStruct': 0,
                    u'insCode': ' ',#mmtfstr(line[26:27].rstrip()),
                    u'xCoord': float(line[30:38]),
                    u'yCoord': float(line[38:46]),
                    u'zCoord': float(line[46:54]),
                    u'bFactor': float(line[60:66]),
                    u'occupancy': float(line[54:60]),
                    u'chainId': mmtfstr(line[72:76].strip()),
                    u'element': mmtfstr(line[76:78].lstrip()),
                }
    d_out = from_atoms(atom_gen())
    #assert_consistency(d_out)
    return d_out,d_out.encode()
#also need SS,and Bonds!!
workingdir = "D:\\Data\\cellPACK_data_git\\cellPACK_database_1.1.0\\opm\\"
with os.scandir(workingdir+os.sep+"pdb") as it:
    for entry in it:
        filename, file_extension = os.path.splitext(entry.name)
        if (file_extension!=".pdb"): continue
        outfilename = workingdir+filename + '.mmtf'
        #if (os.path.isfile(outfilename)):continue
        with open(outfilename, 'wb') as handle:
            f = open(workingdir+os.sep+"pdb"+os.sep+entry.name)
            d,dencoded = pdb2mmtf(f)
            f.close()
            handle.write(dencoded)
        print(outfilename)
        assert_consistency(d)