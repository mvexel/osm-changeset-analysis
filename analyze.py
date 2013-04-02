#!/usr/bin/python
import sys
import os
import gzip
import xml.etree.cElementTree as ET
from shapely.geometry import box
from pymongo import Connection

BASEDIR = "/home/mvexel/osm/changesets/changesets/"
AREATHRESHOLD = 0.25
VERBOSITY = 1000

refboxes = [
        box(-124.7625, 24.5210, -66.9326, 49.3845), 
        box(-179.1506, 51.2097, -129.9795, 71.4410),
        box(-160.2471, 18.9117, -154.8066, 22.2356)] # lower 48, AK, HI

def intersects(bbox):
    for refbox in refboxes:
        if bbox.intersects(refbox):
            return True
    return False

if __name__ == "__main__":
    # mongo init
    conn = Connection()
    db = conn.leipeshit
    changesetcollection = db.changesets
    metacollection = db.meta
    i = 0
    j = 0
    k = 0
    for root, dirs, files in os.walk(BASEDIR):
        dirs.sort(reverse=True)
        files.sort(reverse=True)
        for name in files:
            if name.endswith('.gz'):
                f = gzip.open(os.path.join(root,name), 'rb')
                i += 1
                try:
                    tree = ET.parse(f)
                except ET.ParseError:
                    sys.stdout.write('x')
                    sys.stdout.flush()
                for changeset in tree.getroot().findall('changeset'):
                    if not set(('min_lat','max_lat','min_lon','max_lon')).isdisjoint(changeset.attrib):
                        min_lat = float(changeset.attrib['min_lat'])
                        min_lon = float(changeset.attrib['min_lon'])
                        max_lat = float(changeset.attrib['max_lat'])
                        max_lon = float(changeset.attrib['max_lon'])
                        bbox = box(min_lon, min_lat, max_lon, max_lat)
                        if not i % VERBOSITY: 
                            sys.stdout.write('.')
                            sys.stdout.flush()
                        if intersects(bbox) and bbox.area < AREATHRESHOLD:
                            j += 1
                            thousands = os.path.basename(os.path.normpath(root))
                            seq = int(thousands+name.partition('.')[0])
                            if j == 1:
                                metacollection.update({'collection':'changesets'}, {'highest':seq}, upsert=True)
                            changesetcollection.insert(dict(changeset.attrib, **{'seq':seq}))
                            if not j % VERBOSITY:
                                sys.stdout.write('o')
                                sys.stdout.flush()
                    else:
                        k += 1
                        if not k % VERBOSITY:
                            sys.stdout.write('_')
                            sys.stdout.flush()
    conn.close()
    
