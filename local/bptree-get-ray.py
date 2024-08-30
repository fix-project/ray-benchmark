import ray
ray.init()

import base64
import argparse
import os
import time 
import psutil

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
args = parser.parse_args()
fix_path = args.fix_path

@ray.remote
class Loader:
    def __init__( self ):
        self.prefix_map = {}
        self.buffer = {}
        self.cpu_id = psutil.Process().cpu_num()

        for filename in os.listdir( os.path.join( fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]

    def get_object( self, handle ):
        psutil.Process().cpu_affinity( [self.cpu_id] )
        raw = base64.b16decode( handle.upper() )
        if raw[30] | 0b11111000 == 0b11111000:
            size = raw[30] >> 3
            return raw[:size] 

        prefix = handle[:48]
        filename = prefix + self.prefix_map[prefix]

        if filename in self.buffer:
            return self.buffer[filename] 

        with open( os.path.join( fix_path, "data/", filename ), 'rb') as file:
            data = file.read()
            self.buffer[filename] = data
            return data

    def get_cpuid( self ):
        return self.cpu_id 

# Create an actor
loader = Loader.remote()
cpuid = ray.get( loader.get_cpuid.remote() )

def get_entry( data, i ):
    return base64.b16encode( data[ int(i) * 32: int( i + 1 ) *32 ] ).decode("utf-8").lower()

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

@ray.remote
def bptree_get_bad_style( root, key ):
    psutil.Process().cpu_affinity( [cpuid] )
    curr_level = root

    while True:
        data = ray.get( loader.get_object.remote( curr_level ) )
        keys = ray.get( loader.get_object.remote( get_entry( data, 0 ) ) )
        isleaf = keys[0] == 1
        keys = keys[1:]
        idx = upper_bound( keys, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                return ray.get( loader.get_object.remote( get_entry( data, idx ) ) )
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

@ray.remote
def bptree_get_good_style( is_odd, curr_level_data, keys_data, key ):
    psutil.Process().cpu_affinity( [cpuid] )
    if is_odd:
        return bptree_get_good_style.remote( False, curr_level_data, loader.get_object.remote( get_entry( curr_level_data, 0 ) ), key )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                return loader.get_object.remote( get_entry( curr_level_data, idx ) )
            else:
                return "Not found"
        else:
            return bptree_get_good_style.remote( True, loader.get_object.remote( get_entry( curr_level_data, idx + 1 ) ), "", key )

@ray.remote
def bptree_get_good_style_collect( bptree_root, key ):
    psutil.Process().cpu_affinity( [cpuid] )
    ref = bptree_get_good_style.remote( True, loader.get_object.remote( bptree_root ), "", key )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

bptree_root = os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) 

#for i in range( 0, 10 ):
#    start = time.time()
#    end = time.time()
#    print( end - start )

for i in range( 0, 10 ):
    start = time.time()
    ray.get( bptree_get_bad_style.remote( bptree_root, -1132553114 ) )
    end = time.time()
    print( end - start )

for i in range( 0, 10 ):
    start = time.time()
    ray.get( bptree_get_good_style_collect.remote( bptree_root, -1132553114 ) )
    end = time.time()
    print( end - start )
