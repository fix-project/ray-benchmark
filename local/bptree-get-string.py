import ray
import struct
ray.init(num_cpus=1)

import base64
import argparse
import os
import time

parser = argparse.ArgumentParser("bptree-get-string-key")
parser.add_argument("style", help="bptree style", type=str)
parser.add_argument("fix_path", help="path to .fix repository", type=str)
parser.add_argument("key_list", help="key to lookup", type=str)
parser.add_argument("begin_key_index", help="beginning index to look up in key_list", type=int)
parser.add_argument("num_of_keys", help="key to lookup", type=int)
parser.add_argument("tree_root_label", help="label of tree root", type=str)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
    for i in range( 0, args.begin_key_index + args.num_of_keys ):
        if i < args.begin_key_index:
            f.readline()
        else:
            key_list.append( f.readline().rstrip() )

def decode( handle ):
    return base64.b16decode( handle.upper() )

def encode( handle ):
    return base64.b16encode( handle ).decode("utf-8").lower()

@ray.remote
class Loader:
    def __init__( self ):
        self.prefix_map = {}
        for filename in os.listdir( os.path.join( args.fix_path, "data/" ) ):
            self.prefix_map[filename[:48]] = filename[48:]

        self.empty_tree = decode( "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7" )
        self.empty_ref = ray.put( "" )
        self.refs = []

    def get_object( self, handle ):
        if handle[30] | 0b11111000 == 0b11111000:
            size = handle[30] >> 3
            return handle[:size]

        if handle[:24] == self.empty_tree:
            return "" 

        handle = encode(handle)
        
        prefix = handle[:48]
        filename = prefix + self.prefix_map[prefix]

        if ( filename.endswith( '400' ) ):
            with open( os.path.join( args.fix_path, "data/", filename ), 'r') as file:
                data = file.read()
            return data

        with open( os.path.join( args.fix_path, "data/", filename ), 'rb') as file:
            data = file.read()
            return data

# Create an actor
loader = Loader.remote()

def get_object( raw ):
    if ( isinstance( raw, ray._raylet.ObjectRef ) ):
        return raw
    else:
        return loader.get_object.remote( raw )

def get_object_deref( raw ):
    return ray.get( get_object( raw ) )

def get_entry( data, i ):
    return data[ int(i) * 32: int( i + 1 ) *32 ]
    #return data[ int( i ) ]

def uint64_from_bytes(byte_array):
    return struct.unpack(">Q", byte_array)[0]

def raw_keys_to_string_keys( keys, begin_idx ):
    res = keys[begin_idx:-1].split('\0')
    return res

def upper_bound( keys, key ):
    low, high = 0, len(keys) - 1
    closest_larger_index = len(keys)

    while low <= high:
        mid = (low + high) // 2
        if keys[mid] > key:
            closest_larger_index = mid
            high = mid - 1
        else:
            low = mid + 1
    return closest_larger_index

@ray.remote
def bptree_get_string_key_bad_style( root, key ):
    curr_level = root

    while True:
        data = get_object_deref( curr_level )
        keys = get_object_deref( get_entry( data, 0 ) )

        if ( isinstance( keys, bytes ) ):
            isleaf = ( keys[0] == b'\x01' )
            string_keys = raw_keys_to_string_keys( keys[1:].decode("utf-8"), 0 )
        else:
            isleaf = ( str.encode(keys[0]) == b'\x01' )
            string_keys = raw_keys_to_string_keys( keys, 1 )

        idx = upper_bound( string_keys, key )

        if isleaf:
            if ( idx != 0 and string_keys[idx - 1] == key ):
                return [get_object_deref( get_entry( data, idx ) )]
            else:
                return "Not found"
        else:
            curr_level = get_entry( data, idx + 1 )

@ray.remote
def bptree_get_string_key_good_style( is_odd, curr_level_data, keys_data, key ):
    if is_odd:
        return bptree_get_string_key_good_style.remote( False, curr_level_data, get_object( get_entry( curr_level_data, 0 ) ), key )
    else:
        if ( isinstance( keys_data, bytes ) ):
            isleaf = ( keys_data[0] == b'\x01' )
            string_keys = raw_keys_to_string_keys( keys_data[1:].decode("utf-8"), 0 )
        else:
            isleaf = ( str.encode(keys_data[0]) == b'\x01' )
            string_keys = raw_keys_to_string_keys( keys_data, 1 )

        idx = upper_bound( string_keys, key )
        if isleaf:
            if ( idx != 0 and string_keys[idx - 1] == key ):
                return get_object( get_entry( curr_level_data, idx ) )
            else:
                return "Not found"
        else:
            return bptree_get_string_key_good_style.remote( True, get_object( get_entry( curr_level_data, idx + 1 ) ), "", key )

bptree_root = decode( os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/" + args.tree_root_label ) ) ) )

@ray.remote
def bptree_get_good_style_collect( bptree_root, key ):
    # psutil.Process().cpu_affinity( [cpuid] )
    ref = bptree_get_string_key_good_style.remote( True, get_object( bptree_root ), "", key )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

start = time.monotonic()

for key in key_list:
    if ( args.style == "good" ):
        ray.get( bptree_get_good_style_collect.remote( bptree_root, key ) )
    else: 
        ray.get( bptree_get_string_key_bad_style.remote( bptree_root, key ) )

end = time.monotonic()

print( end - start, "s" )
