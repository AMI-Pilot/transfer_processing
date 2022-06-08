from pathlib import Path
import logging
import csv
import xml.etree.ElementTree as ET

logger = logging.getLogger()

"""
Lookup title metadata.
"""


class Metadata:
    "Source of id to title lookup files"
    def __init__(self, metadir: Path):
        self.metadir = metadir

    def lookup_title(self, barcode):
        "Lookup the title from the spreadsheets via a barcode"
        logging.info(f"Lookup title in {self.metadir!s}")
        for csvfile in self.metadir.glob("*.csv"):
            with open(csvfile) as f:
                csvreader = csv.DictReader(f)
                if 'Barcode' not in csvreader.fieldnames:
                    logger.warning(f"Metadata file {csvfile!s} doesn't have a 'Barcode' column. Skipping")
                    continue
                if 'Title' not in csvreader.fieldnames:
                    logger.warning(f"Metadata file {csvfile!s} doesn't have a 'Title' column. Skipping")
                    continue
                for row in csvreader:
                    if row['Barcode'] == barcode:
                        return row['Title']
        raise KeyError(f"No title metadata for barcode {barcode}")


def denamespace(xmlfile: Path):    
    "load an XML file, stripping the namespaces"
    with open(xmlfile) as f:
        it = ET.iterparse(f)
        for _, el in it:
            _, has_namespace, postfix = el.tag.rpartition('}')
            if has_namespace:
                el.tag = postfix
            for at in list(el.attrib.keys()):
                if '}' in at:
                    el.attrib[at.split('}', 1)[1]] = el.attrib[at]
                    del el.attrib[at]
    return it.root
        
def prettify(element, indent='  '):
    # alas we're on 3.8, so ElementTree.indent isn't available.  Stole this from stack overflow.
    queue = [(0, element)]  # (level, element)
    while queue:
        level, element = queue.pop(0)
        children = [(level + 1, child) for child in list(element)]
        if children:
            element.text = '\n' + indent * (level+1)  # for child open
        if queue:
            element.tail = '\n' + indent * queue[0][0]  # for sibling open
        else:
            element.tail = '\n' + indent * (level-1)  # for parent close
        queue[0:0] = children  # prepend so children come before siblings


def avalon_mods(barcode, metadir, metsfile: Path, marcfile: Path, eadfile: Path, has_video=False):
    "Generate an avalon-compatible MODS file"
    
    metadata = {'title': Metadata(metadir).lookup_title(barcode),
                'identifiers': [],
                'dateissued': '19uu'
    }
    
    # identifiers from mets
    tree = denamespace(metsfile)    
    for xlink in tree.findall("dmdSec[@ID='DMD1']/mdRef"):
        metadata['identifiers'].append([xlink.attrib.get('MDTYPE', 'Unknown'), xlink.attrib['href']])

    if marcfile and marcfile.exists():
        tree = denamespace(marcfile)
        for marc100 in tree.findall("record/datafield[@tag='100']"):
            logging.debug(ET.tostring(marc100))
            if marc100.attrib.get('ind1', '') == '1' and marc100.attrib.get('ind2', '') == ' ':        
                metadata['dateissued'] = marc100.find("subfield[@code='d']").text.split('-', 1)[0]                
                break    

    elif eadfile and eadfile.exists():
        tree = denamespace(eadfile)
        metadata['dateissued'] = tree.find("archdesc[@level='collection']/did/unittitle/unitdate").text.split('-', 1)[0]
        
    else:
        raise FileNotFoundError("Neither marc file nor ead file exists")
    

    logging.debug(f"Metadata: {metadata}")

    # generate the XML from the metadata collected.
    mods = ET.Element('mods', attrib={'version': '3.5', 'xsi:schemaLocation': "http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-5.xsd"})
    # title        
    ET.SubElement(ET.SubElement(mods, 'titleInfo'), 
                 'title').text = metadata['title']
                       
    # author
    # UMICH didn't specify where to find this.
    #ET.SubElement(ET.SubElement(mods, 'name', attrib={'type': "personal", 'usage': "Primary"}), 
    #            'namePart').text = "The author name"
    
    # date issued
    ET.SubElement(ET.SubElement(mods, 'originInfo'), 
                'dateIssued').text = metadata['dateissued']

    # format template (typeOfResource and physicalDescription)
    ET.SubElement(mods, 'typeOfResource').text = 'moving image' if has_video else 'sound recording'
    pd = ET.SubElement(mods, 'physicalDescription')
    ET.SubElement(pd, 'form', attrib={'authority': 'gmd'}).text =  'video recording' if has_video else 'sound recording'
    # I can't guarantee a fixed set of inputs from UMICH, so we're going to ignore this.
    #ET.SubElement(pd, 'form', attrib={'authority': 'marcsmd'}).text = "'sound disc' or 'videocassete' or 'sound tape reel' or 'videodisc"
        
    # unit note.
    ET.SubElement(mods, 'note', attrib={'type': 'general'}).text = "Collection Name: UMICH"

    # identifiers...can repeat for different things...
    ET.SubElement(mods, 'identifier', attrib={'type': 'local', 'displayLabel': 'UMICH Barcode'}).text = barcode
    for d, n in metadata['identifiers']:
        ET.SubElement(mods, 'identifier', attrib={'type': 'local', 'displayLabel': d}).text = n

    # record info template
    # No info from UMICH to generate this.
    #ri = ET.SubElement(mods, 'recordInfo')
    #ET.SubElement(ri, 'recordCreationDate', attrib={'encoding': 'iso8601'}).text = 'creation date'
    #ET.SubElement(ri, 'recordChangeDate', attrib={'encoding': 'iso8601'}).text = 'update date'
    #ET.SubElement(ri, 'recordIdentifier', attrib={'source': 'UMICH'}).text = barcode
    
    prettify(mods)
    return ET.tostring(mods).decode()
    








 




    # this code is based on the pod-export-to-mods.xsl from MDPI.
    mods = ET.Element('mods', attrib={'version': '3.5', 'xsi:schemaLocation': "http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-5.xsd"})        
    # title        
    #ET.SubElement(ET.SubElement(mods, 'titleInfo'), 
    #            'title').text = Metadata(ami.get_directory('metadata')).lookup_title(pkg.get_id())    
    
    # author
    ET.SubElement(ET.SubElement(mods, 'name', attrib={'type': "personal", 'usage': "Primary"}), 
                'namePart').text = "The author name"
    
    # date issued
    try:
        if marcfile.exists():
            tree = ET.parse(marcfile)
            unitdate = tree.find("")
        elif eadfile.exists():
            tree = ET.parse(eadfile)
            unitdate = tree.find("archdesc[@level='collection']/did/unittitle/unitdate")
            unitdate = unitdate.split('-', 1)[0]
        else:
            dateIssued = '19uu'
    except Exception as e:
        logger.exception(e)
        dateIssued = '19uu'
    ET.SubElement(ET.SubElement(mods, 'originInfo'), 
                'dateIssued').text = dateIssued

    # format template (typeOfResource and physicalDescription)
    ET.SubElement(mods, 'typeOfResource').text = 'moving image' if has_video else 'sound recording'
    pd = ET.SubElement(mods, 'physicalDescription')
    ET.SubElement(pd, 'form', attrib={'authority': 'gmd'}).text =  'video recording' if has_video else 'sound recording'
    ET.SubElement(pd, 'form', attrib={'authority': 'marcsmd'}).text = "'sound disc' or 'videocassete' or 'sound tape reel' or 'videodisc"
        
    # unit note.
    ET.SubElement(mods, 'note', attrib={'type': 'general'}).text = "Collection Name: UMICH"

    # identifiers...can repeat for different things...
    ET.SubElement(mods, 'identifier', attrib={'type': 'local', 'displayLabel': 'UMICH Barcode'}).text = pkg.get_id()
    ET.SubElement(mods, 'identifier', attrib={'type': 'local', 'displayLabel': 'Bag Number'}).text = 'uuid-uuid-uuid-uuid'

    # record info template
    ri = ET.SubElement(mods, 'recordInfo')
    ET.SubElement(ri, 'recordCreationDate', attrib={'encoding': 'iso8601'}).text = 'creation date'
    ET.SubElement(ri, 'recordChangeDate', attrib={'encoding': 'iso8601'}).text = 'update date'
    ET.SubElement(ri, 'recordIdentifier', attrib={'source': 'UMICH'}).text = pkg.get_id()
    return ET.tostring(mods).decode()