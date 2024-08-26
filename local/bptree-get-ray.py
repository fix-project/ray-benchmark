import ray
ray.init()

import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
args = parser.parse_args()
fix_path = args.fix_path

prefix_map = {}
for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
    prefix_map[filename[:48]] = filename[48:]

@ray.remote
def get_object( handle ):
    raw = base64.b16decode( handle.upper() )
    if raw[30] | 0b11111000 == 0b11111000:
        size = raw[30] >> 3
        return raw[:size] 

    prefix = handle[:48]
    filename = prefix + prefix_map[prefix]

    with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
        data = file.read()
        return data


def get_entry( data, i ):
    return base64.b16encode( data[ int(i) * 32: int( i + 1 ) *32 ] ).decode("utf-8").lower()

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ i * 4:( i + 1 ) * 4 ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

def bptree_get_bad_style( root, key ):
    curr_level = root

    while True:
        data = ray.get( get_object.remote( curr_level ) )
        keys = ray.get( get_object.remote( get_entry( data, 0 ) ) )
        isleaf = keys[0] == 1
        keys = keys[1:]
        idx = upper_bound( keys, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys[ ( idx - 1 )* 4 : idx * 4 ], byteorder='little', signed=True ) == key ):
                return ray.get( get_object.remote( get_entry( data, idx ) ) )
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

@ray.remote
def bptree_get_good_style( is_odd, curr_level_data, keys_data, key ):
    if is_odd:
        return bptree_get_good_style.remote( False, curr_level_data, get_object.remote( get_entry( curr_level_data, 0 ) ), key )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ ( idx - 1 )* 4 : idx * 4 ], byteorder='little', signed=True ) == key ):
                return get_object.remote( get_entry( curr_level_data, idx ) )
            else:
                return "Not found"
        else:
            return bptree_get_good_style.remote( True, get_object.remote( get_entry( curr_level_data, idx + 1 ) ), "", key )


bptree_root = os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) 

for i in range( 0, 10 ):
    start = time.time()
    bptree_get_bad_style( bptree_root, -1132553114 )
    end = time.time()
    print( end - start )

for i in range( 0, 10 ):
    start = time.time()
    ref = ray.get( bptree_get_good_style.remote( True, get_object.remote( bptree_root ), "", -1132553114 ) )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    end = time.time()
    print( end - start )
