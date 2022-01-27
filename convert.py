import argparse
import csv
from itertools import combinations, product
from typing import Generator, Mapping

from lxml import etree
from tqdm import tqdm


# I/O
parser = argparse.ArgumentParser(description='Convert personography xml to csv')
parser.add_argument('-i', '--input', type=str, help='xml file to convert (defaults to "HAMpersons.xml")', default="HAMpersons.xml")
parser.add_argument('-o', '--output', type=str, help='filename stub for created csvs (optional) - two will be created in the form `output`_agents.csv, `output`_relationships.csv')

args = parser.parse_args()

# Some constants
INPUT = str(args.input)
OUTPUT = str(args.output) if args.output else "output"
AGT_OUT = f"{OUTPUT}_agents.csv"
REL_OUT = f"{OUTPUT}_relationships.csv"
NS = {
    "tei": "http://www.tei-c.org/ns/1.0",
    "xml": "http://www.w3.org/XML/1998/namespace"
    }
CONCHAR = "$" # To avoid confusing the date module, which uses pipes as part of the string representation of dates

# Mappings from XML nodes to Heurist Agent fields
AGT_MAP = {
    f"person.attrib['{{{NS['xml']}}}id']": "XML ID",
    f"person.find('{{{NS['tei']}}}persName/{{{NS['tei']}}}surname')": "Surname",
    f"person.find('{{{NS['tei']}}}persName/{{{NS['tei']}}}forename')": "Forename",
    f"person.findall('{{{NS['tei']}}}persName/{{{NS['tei']}}}rs')": "Alternate Name(s) / title(s)",
    f"person.find('{{{NS['tei']}}}sex')": "Gender",
    f"person.find('{{{NS['tei']}}}birth')":"Birth Date",
    f"person.find('{{{NS['tei']}}}death')": "Death Date",
    f"person.findall('{{{NS['tei']}}}note')": "Research Notes",
    f"[ref.attrib['target'] for ref in person.findall('{{{NS['tei']}}}listBibl/{{{NS['tei']}}}bibl/{{{NS['tei']}}}ref')]": "URL(s)"
}

# Relations are a more complex case
# The Relationship Type is given in the 'name' attribute, but the 'source' and 'target' need to be inferred from
# the 'passive' (i.e. source), 'active' (i.e. target), and 'mutual' (i.e. source and target) attributes
REL_FIELDS = [
    "Source", # Use xml id
    "Target", # Use xml id
    "Relationship Type", # Use 'name' attribute of relation node
]


def eval_to_str(expr:str, locals:Mapping) -> str:
    """Evaluates the given expression in the provided environment, and returns the output as a string"""
    xml_output = eval(expr, locals)

    # Handle the output
    if isinstance(xml_output, list):
        # Handle special cases: notes
        if f"{{{NS['tei']}}}note" in expr:
            return handle_note(xml_output)
        if f"{{{NS['tei']}}}ref" in expr:
            return CONCHAR.join([url for url in xml_output])
        else:
            return CONCHAR.join([node.text for node in xml_output])
    elif isinstance(xml_output, etree._Element):
        # Handle special cases: births and deaths
        if f"{{{NS['tei']}}}birth" in expr or f"{{{NS['tei']}}}death" in expr:
            return handle_date(xml_output)
        else:
            return xml_output.text
    elif isinstance(xml_output, str):
        return xml_output
    else:
        # Base case basically covers if the node query returns None
        return ""

def handle_date(node:etree._Element) -> str:
    """Returns a Heurist string representation of the date in the node"""
    att = node.attrib

    if 'when' in att:
        return att['when']
    elif 'notBefore' in att or 'notAfter' in att:
        # Heurist uses TPQ and TAQ for date ranges, with the meanings:
        # TPQ = Terminus Post Quem
        # TAQ = Terminus Ante Quem
        TPQ = att['notBefore'] if 'notBefore' in att else ''
        TAQ = att['notAfter'] if 'notAfter' in att else ''
        return f"[ |VER=1|TYP=p|TPQ={TPQ}|TAQ={TAQ}|DET=0|CLD=Gregorian|SPF=0|EPF=0 ]"
    else:
        return ""

def handle_note(notelist:list) -> str:
    """Concatenates all the notes with their typenames, ignoring empty note nodes and 
    child nodes"""

    def _format_note(note:etree._Element) -> str:
        """Extracts text from an individual note"""
        # Use the .tostring method to get the raw xml from inside the note node,
        # with the 'method' keyword argument to strip out the tags, then apply
        # str.strip() to the output to remove all the extraneous whitespace
        # Longer notes sometimes have line breaks, which interfere with the csv import. So replace
        # these with <br/> elements (Heurist will render the html).
        return etree.tostring(note, method='text', encoding='unicode').strip().replace('\n', '<br/>')
    
    def _format_type(note:etree._Element) -> str:
        """Extracts type of note if available, and appends a colon with space"""
        return note.attrib['type'] + ': ' if 'type' in note.attrib else ''

    def _stringify_note(note:etree._Element) -> str:
        """Apply _format_note and _format_type to the given note"""

        # This condition first text that the note has a text attribute, and then checks
        # whether the text contains anything other that whitespace.
        if note.text and note.text.strip():
            return f"{_format_type(note)}{_format_note(note)}"
        else:
            return None

    # NB: str.join returns an empty string if the given sequence is empty. This is the desired
    # output for handle_note, which should either rerturn a concatenated string if there is data,
    # or an empty string for the csv cell if there is no data
    return CONCHAR.join([_stringify_note(note) for note in notelist if _stringify_note(note)])

def handle_relations(person:etree._Element, rel_dict:dict) -> dict:
    """Returns a new rel_dict containing new relationships found in person"""

    relations = person.findall(f"{{{NS['tei']}}}note/{{{NS['tei']}}}listRelation/{{{NS['tei']}}}relation")

    def _convert_rel_type(name:str) -> str:
        """Converts the given relation type name into a Heurist-style predicate:
        e.g. uncle |--> isUncleOf"""
        if name == "1stCousin":
            return "isCousinOf"
        elif name == "dummy":
            return None
        else:
            return "is" + name.title() + "Of"

    if not relations:
        return rel_dict
    else:
        tmp_dict = {}
        for rel in relations:
            if not 'name' in rel.attrib:
                continue
            else:
                rel_type = _convert_rel_type(rel.attrib['name'])
            # For undirected relationships, just grab them all. The combinations() function from 
            # itertools automatically sorts each pair, so there is no possibility of accidentally
            # creating two records for (src, tar) = rel_type and (tar, src) = rel_type.
            if 'mutual' in rel.attrib:
                for pair in combinations(rel.attrib['mutual'].split(), 2):
                    tmp_dict[pair] = rel_type
            # The second case is directed relationships -- these require checking for duplicates
            elif 'active' in rel.attrib and 'passive' in rel.attrib:
                # Both the 'active' and 'passive' attributes regularly contain a list of persons, seperated
                # by whitespace. Use itertools.product to generate all the possible pairings
                srcs, tars = rel.attrib['active'], rel.attrib['passive']
                for src,tar in product(srcs.split(), tars.split()):
                    # Check that the inverse relationship isn't already recorded
                    if (tar,src) in rel_dict:
                        continue
                    else:
                        tmp_dict[(src, tar)] = rel_type
    
    # Nice way to create new dict by merging two existing dicts in Python 3.9+
    return rel_dict | tmp_dict


def convertXML(tree:etree._ElementTree) -> None:
    """Converts Mary Hamilton Personography.xml into Heurist csv"""

    persons = tree.xpath("//tei:listPerson[1]/tei:person", namespaces=NS) # All persons in the first listPerson element

    with open(AGT_OUT, 'tw', newline='') as agent_csv, open(REL_OUT, 'tw', newline='') as rel_csv:
        # Initialise DictWriter objects to create csv files
        agt_writer = csv.DictWriter(agent_csv, fieldnames=AGT_MAP.values(), dialect="unix")
        agt_writer.writeheader()

        # The relationships csv will be stored in memory as a dict, and then written outside the person
        # loop, to avoid the creation of duplicate records
        rel_writer = csv.DictWriter(rel_csv, fieldnames=REL_FIELDS, dialect="unix")
        rel_writer.writeheader()
        rel_dict = {}

        for person in tqdm(persons):
            # Write person fields
            person_row = {field:eval_to_str(expr, {'person':person}) for expr,field in AGT_MAP.items()}
            agt_writer.writerow(person_row)

            # Handle relations
            rel_dict = handle_relations(person, rel_dict)
        
        # To pass rel_dict to .writrows(), we need to transform it from a dict into a list of dicts:
        # {(src,tar):rel_type} |--> [{'Source':src,'Target':tar,'Relationship Type':rel_type}]
        rel_writer.writerows([{'Source':key[0], 'Target':key[1], 'Relationship Type':val} for key,val in rel_dict.items()])

if __name__ == "__main__":
    with open(INPUT, mode="rb") as file:
        tree = etree.parse(file)
    
    convertXML(tree)