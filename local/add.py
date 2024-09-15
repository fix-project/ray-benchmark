import ray
ray.init(num_cpus=1)

import time

@ray.remote
def add( x, y ):
    return ord(x) + ord(y)

def make_addend( i ):
    x = i // 254 + 1
    y = i % 254 + 1
    return(x.to_bytes(1,'little'), y.to_bytes(1,'little'))

addends = []
for i in range(0, 4097):
    addends.append( make_addend( i ) )
ray.get( add.remote( addends[4096][0], addends[4096][1] ) )

start = time.monotonic()
for i in range(0, 4096):
    ray.get( add.remote( addends[i][0], addends[i][1] ) )
end = time.monotonic()

print( ( end - start ) * 1_000_000_000, "ns" )
