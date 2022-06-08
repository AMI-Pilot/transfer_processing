#!/bin/env python3
import _preamble
import xml.etree.ElementTree as ET
from ami import Ami
from ami.metadata import avalon_mods
from pathlib import Path
import logging

#marcfile = Path("/srv/amidata/devel/originals/04ca8e0f-837d-45a0-9634-83c8f5b43466/marc.xml")
#eadfile = Path("/srv/amidata/devel/new_data_20220307/39015091568454/ead.xml")
metsfile = Path("/srv/amidata/devel/new_data_20220307/39015091568454/data/mets.xml")
ami = Ami()
metadir = ami.get_directory("metadata")

logging.getLogger().setLevel(logging.DEBUG)


for has_video in (True, False):
    for marcfile in (None, Path("/srv/amidata/devel/originals/04ca8e0f-837d-45a0-9634-83c8f5b43466/marc.xml")):
        for eadfile in (None, Path("/srv/amidata/devel/new_data_20220307/39015091568454/ead.xml")):
            if (eadfile is None and marcfile is None) or (eadfile is not None and marcfile is not None):
                continue
            print(f"**** Testing: video={has_video}, marcfile={marcfile!s}, eadfile={eadfile!s}")
            try:
                print("Result ---> ", avalon_mods("39015091568454", metadir, metsfile, marcfile, eadfile, has_video=False))
            except Exception as e:
                logging.exception(e)



