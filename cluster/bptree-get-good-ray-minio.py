import base64
import argparse
import os
import time 

parser = argparse.ArgumentParser("bptree-get-ray")
parser.add_argument( "fix_path", help="path to .fix repository", type=str)
parser.add_argument( "key_list", help="path to list of keys", type=str)
parser.add_argument( "num_of_keys", help="the number of keys to get", type=int)
args = parser.parse_args()

key_list = []
with open( args.key_list, 'r' ) as f:
    for i in range( 0, args.num_of_keys ):
        key_list.append( int( f.readline().rstrip() ) )
fix_path = args.fix_path

import ray
ray.init()

@ray.remote(num_cpus=0)
def get_object( handle ):
    raw = base64.b16decode( handle.upper() )
    if raw[30] | 0b11111000 == 0b11111000:
        size = raw[30] >> 3
        return raw[:size] 

    s3_target = boto3.resource('s3', 
                               endpoint_url='http://localhost:' + str( args.minio_port ) ,
                               aws_access_key_id='minioadmin',
                               aws_secret_access_key='minioadmin',
                               aws_session_token=None,
                               config=boto3.session.Config(signature_version='s3v4'),
                               verify=False )
    obj = s3_target.Object( args.fix_path, str( handle[:48] ) )
    return obj.get()['Body'].read()

def get_entry( data, i ):
    return base64.b16encode( data[ int(i) * 32: int( i + 1 ) *32 ] ).decode("utf-8").lower()

def upper_bound( keys, key ):
    for i in range( 0, int( len( keys ) / 4 ) ):
        x = int.from_bytes( keys[ int(i * 4):int(( i + 1 ) * 4) ], byteorder='little', signed=True )
        if x > key:
            return i;
    return len( keys ) / 4

@ray.remote
def bptree_get_good_style( is_odd, curr_level_data, keys_data, key ):
    if is_odd:
        return bptree_get_good_style.remote( False, curr_level_data, get_object.remote( get_entry( curr_level_data, 0 ) ), key )
    else:
        isleaf = keys_data[0] == 1
        keys_data = keys_data[1:]
        idx = upper_bound( keys_data, key )

        if isleaf:
            if ( idx != 0 and int.from_bytes( keys_data[ int(( idx - 1 )* 4) : int(idx * 4) ], byteorder='little', signed=True ) == key ):
                return get_object.remote( get_entry( curr_level_data, idx ) )
            else:
                return "Not found"
        else:
            return bptree_get_good_style.remote( True, get_object.remote( get_entry( curr_level_data, idx + 1 ) ), "", key )

@ray.remote
def bptree_get_good_style_collect( bptree_root, key ):
    ref = bptree_get_good_style.remote( True, get_object.remote( bptree_root ), "", key )
    while ( isinstance( ref, ray._raylet.ObjectRef ) ):
        ref = ray.get( ref )
    return ref

bptree_root = os.path.basename( os.readlink( os.path.join( args.fix_path, "labels/tree-root" ) ) ) 

refs = []
for key in key_list:
    refs.append( bptree_get_good_style_collect.remote( bptree_root, key ) )

ray.get( refs )
