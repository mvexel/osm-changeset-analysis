#!/usr/bin/python
import sys
import os
import bz2file
from datetime import datetime
import uuid
import xml.etree.cElementTree as ET
from shapely.geometry import box
from pymongo import MongoClient

BASEDIR = "/Users/martijnv/osm/planet/changesets"
AREATHRESHOLD = 0.25  # changesets larger than this will be ignored (deg^2)
VERBOSITY = 1000  # integer, higher is less verbose
SPATIALMATCH = "within"  # within or intersects, determines
SESSIONID = uuid.uuid4()

# this is the list of bounding boxes the incoming changesets will be checked
# against. Note that this does not mean that any changes actually happenes in
# the area of interest.
refboxes = [
    box(-124.7625, 24.5210, -66.9326, 49.3845),
    box(-179.1506, 51.2097, -129.9795, 71.4410),
    box(-160.2471, 18.9117, -154.8066, 22.2356)]
# lower 48, AK, HI


def intersects(bbox):
    '''checks whether the incoming box satisfies the spatial match as
    defined in SPATIALMATCH against the reference bounding boxes'''
    for refbox in refboxes:
        if bbox.intersects(refbox):
            return True
    return False

if __name__ == "__main__":
    # mongo init
    client = MongoClient("mongodb://mvexel:^5PmT2OYe6z*nVCi@ds039860.mongolab.com:39860/changesets")
    db = client.changesets
    changesetcollection = db.changesets
    metacollection = db.meta
    # counters
    i = 0
    j = 0
    k = 0
    for root, dirs, files in os.walk(BASEDIR):
        dirs.sort(reverse=True)
        files.sort(reverse=True)
        for name in files:
            if name.endswith('.bz2'):
                f = bz2file.open(os.path.join(root, name), 'rb')
                i += 1
                try:
                    for event, elem in ET.iterparse(f):
                        if elem.tag == "changeset":
                            print elem.items()
                            # check if changeset has bbox metadata (is not empty)
                            if all(k in elem.attrib for k in ('min_lat', 'max_lat', 'min_lon', 'max_lon')):
                                min_lat = float(elem.attrib['min_lat'])
                                min_lon = float(elem.attrib['min_lon'])
                                max_lat = float(elem.attrib['max_lat'])
                                max_lon = float(elem.attrib['max_lon'])
                                bbox = box(min_lon, min_lat, max_lon, max_lat)
                                # send output to stdout
                                if not i % VERBOSITY:
                                    sys.stdout.write('.')
                                    sys.stdout.flush()
                                if intersects(bbox) and bbox.area < AREATHRESHOLD:
                                    j += 1
                                    thousands = os.path.basename(
                                        os.path.normpath(root))
                                    seq = int(thousands + name.partition('.')[0])
                                    # update metadata collection with highest
                                    # changeset count
                                    if j == 1:
                                        metacollection.update(
                                            {'collection': 'changesets'},
                                            {'uuid': SESSIONID,
                                                'highest': seq,
                                                'date': datetime.now()},
                                            upsert=True)
                                    changesetcollection.insert(
                                        dict(elem.attrib, **{'seq': seq}))
                                    # send output to stdout
                                    if not j % VERBOSITY:
                                        sys.stdout.write('o')
                                        sys.stdout.flush()
                            else:
                                k += 1
                                # send output to stdout
                                if not k % VERBOSITY:
                                    sys.stdout.write('_')
                                    sys.stdout.flush()
                            elem.clear()
                except ET.ParseError:
                    sys.stdout.write('x')
                    sys.stdout.flush()
    metacollection.update(
        {'uuid': SESSIONID},
        {'fullrun': True},
        upsert=True)
    del client
