import terminusdb_client as woql
from terminusdb_client import WOQLQuery
import os
import time
import pprint
pp = pprint.PrettyPrinter(indent=4)
from utils_3store import *

verbose = False # CONTROL
reset_db = True # CONTROL

gYear_type = 'xsd:gYear'
raw_gYear_type = ensure_raw_type(gYear_type)

schema_declarations = {} # special things about the schema loaded
# from seshat_schema_kevin import *
# from seshat_schema_test import *
from seshat_schema_equinox_flat import *

# These are the low-level (castable) types we use in boxed classes, e.g., scm:String has property String that holds an xsd:string
# Note capitalization!
boxed_basic_types = [
    # ("xsd:boolean", "Boolean","True or False"),
    ("xsd:string", "String","Any text or sequence of characters"),
    ("xsd:decimal", "Decimal", "A decimal number."),
    ("xsd:integer", "Integer", "A simple number."),
    ("xsd:nonNegativeInteger", "Positive Integer", "A simple number greater than 0."),
    ("xsd:anyURI", "Any URI", "Any URl. An xsd:anyURI value."),
    ("xsd:gYear", "Year", "A particular Gregorian 4 digit year YYYY - negative years are BCE."),
    ("xdd:gYearRange", "Year Range", "A 4-digit Gregorian year, YYYY, or if uncertain, a range of years YYYY-YYYY."),
    ("xdd:integerRange", "Integer Range", "A simple number or range of integers."),
    ("xdd:decimalRange", "Decimal Range", "A decimal value or, if uncertain, a range of decimal values."),
    ("xdd:dateRange", "Date Range", "A date or a range of dates YYYY-MM-DD"),
    ]

def normaliseID(raw, _type):
    """Ensure all ids in raw have a (proper) prefix.
    """
    schema_prefix = "scm:"  # In case we decide to call it 'seshat:' or something
    if _type == "id":
        if type(raw) is list:
            ids = []
            for p in raw:
                ids.append(normaliseID(p,_type))
            return ids
        else:
            bits = raw.split(":")
            if len(bits) > 1:
                return raw # already prefixed
            else:
                return schema_prefix + raw
    if _type == "type":
        # coerce something like 'xdd:IntegerRange' to our 'scm:integerRange' type, which has different case conventions
        bits = raw.split(":")
        if len(bits) > 1:
            raw = bits[1]
        return schema_prefix + raw[0].upper() + raw[1:]
    return raw

# Build several dictionaries
# wiki section/subsection names to schema names
# wiki variable names to schema names and their underlying xdd/xsd types
# save as pkl with same version number
variable_info = {} # variable label: (property,scoped,type)
type_info = {} # typeclass: (type_property,raw_type)
def create_seshat_schema(client):
    """The query which creates the schema
    Parameters - it uses variables rather than the fluent style as an example
    ==========
    client : a WOQLClient() connection
    """
    # presummably this goes into 'scm:' by default
    # what do I do if I wanted to name the schema to publish it (like bike_scm)?
    # presummably I would have to then dump it in rdf format and make it available on the web for parsing by other 3store apps right?
    # what do I use to load an external schema?

    # This controls 'debugging mode' True to incrementally execute, False to append and execute once
    execute_incrementally = False # if False, flat defined in 7s; if True, 577s
    def process_q(q,message=None):
        if execute_incrementally:
            if message is not None:
                print(message)
            try:
                q.execute(client,commit_msg=message)
            except Exception as exception: # API error or whatever
                print(f"Execution ERROR for: {message} -- skipped")
                print(f"{exception.msg}")
    
        return q
    
    # kevin.js: initial un-numbered q
    q = WOQLQuery().doctype("Organization",
                            label="Organization",
                            description="A human organization of any type - has the capacity to act as a unit, in some sense").abstract()
    all_q = process_q(q,"Organization")
    for class_defn in class_defns:
        if len(class_defn) == 4:
            name,label,description,parents = class_defn
        else:
            name,label,description = class_defn
            parents = []
        # name = normaliseID(name,'id')
        q = WOQLQuery().doctype(name,label=label,description=description)
        for parent in parents:
            # parent = normaliseID(parent,'id')
            q.parent(parent)
        all_q = all_q + process_q(q,name)

    # kevin.js: q2
    q = WOQLQuery().doctype("Topic",
                            label="Topic Class",
                            description="A class that represents a topic").abstract()
    all_q = all_q + process_q(q,"Topic")
    for topic in topics:
        if len(topic) == 4:
            name,label,description,parents = topic
        else:
            name,label,description = topic
            parents = []
        # name = normaliseID(name,'id')
        q = WOQLQuery().doctype(name,label=label,description=description)
        for parent in parents:
            # parent = normaliseID(parent,'id')
            q.parent(parent)
        all_q = all_q + process_q(q,name)

    q = WOQLQuery().doctype("CitedWork",label="Cited Work").property("remote_url", "xsd:anyURI")
    all_q = all_q + process_q(q,'CitedWork')
    


    # This call defines the boxed datatypes we use, e.g., scm:IntegerRange whose 'type' is xdd:IntegerRange
    # kevin.js: q9
    if False:
        q = (WOQLQuery().add_class("scm:Box")
             .label("Box Class")
             .description("A class that represents a boxed datatype")
             .abstract())
        all_q = all_q + process_q(q,'scm:Box')
        
    for bbt in boxed_basic_types:
        datatype,label,description = bbt
        raw_datatype = ensure_raw_type(datatype) # how we are actually storing it
        Datatype = normaliseID(datatype,'type') # get the scm: prefixed, upper-cased name, e.g., scm:GYear from xsd:gYear
        no_prefix_Datatype = Datatype.split(":")[1]
        type_info[no_prefix_Datatype] = (Datatype,datatype) 
        if verbose:
            print(f"box type property: {Datatype} domain: {Datatype} type: {datatype} as {raw_datatype}")

        qt = WOQLQuery().add_class(Datatype).label(label)
        if False:
            # no parent, e.g., scm:Box, needs to be added to qt so far (except it bundles all our types together)
            qt.parent('scm:Box')

        # scm:Integer is a class with a property scm:Integer that permits an xsd:integer
        # when when we mixin scm:Integer into a <prop>_Value class, and it has type xsd:integer
        qp = (WOQLQuery().add_property(Datatype,raw_datatype)
              .domain(Datatype)
              .label(label)
              .description(description))
        q = WOQLQuery().woql_and(qt,qp)
        all_q = all_q + process_q(q,Datatype)
              
    # kevin.js: q5+q6
    # Is this class needed for generate_choice_list()?
    # JSB probably not.  it is like Box which just serves to organize the different types
    # q = WOQLQuery().doctype("Enumerated",label="Enumerated Type",description="A type that consists of a fixed set of choices")
    # all_q = all_q + process_q(q,"Enumerated")

    for etype in enumerations:
        Name,label,description,choices = etype
        Name = Name[0].upper() + Name[1:] # ensure leading capitlized
        scm_Name = normaliseID(Name,'id') # e.g., scm:EpistemicState
        if False:  # DEBUG since fixed_generate_choice_list() uses prefix _:, which causes upset
            # choices = normaliseID(choices,'id')
            for choice in choices:
                choice[0] = normaliseID(choice[0],'id')
            # q = WOQLQuery().generate_choice_list(name,clslabel=label,clsdesc=description,choices=choices)
            qus = fixed_generate_choice_list(cls=scm_name,clslabel=label,clsdesc=description,choices=choices)
            qsc = fixed_generate_choice_list(cls=scm_scoped_name,clslabel=label,clsdesc=description,choices=choices)
            q = WOQLQuery().woql_and(qus,qsc)
        else:
            # HACK define the class but not its structure
            # treat it like a Box type with a string
            raw_type = ensure_raw_type(scm_Name)
            if raw_type != 'xsd:string':
                print(f"Incorrect ensure_raw_type() entry for {scm_Name}")
                continue
            # class scm:EpistemicState with property EpistemicState of type xsd:string
            # class scm:Confidence with property scm:Confidence of type xsd:string # << this is used in property_Value confidence
            type_info[Name] = (scm_Name,scm_Name) 
            if verbose:
                print(f"enumeration property: {scm_Name} domain: {scm_Name}")
            qt = WOQLQuery().add_class(scm_Name).label(label)
            qp = (WOQLQuery().add_property(scm_Name,raw_type)
                  .domain(scm_Name)
                  .label(label)
                  .description(description))
            q = WOQLQuery().woql_and(qt,qp)

        all_q = all_q + process_q(q,f"{scm_Name}")

    # define these after types and enumerations so we can refer to them
    # kevin.js: q7
    q = (WOQLQuery().add_class("Note").label("A Note on a value").description("Editorial note on the value")
         .property("citation", "scm:CitedWork").label("Citation").description("A link to a cited work")
         .property("quotation", "xsd:string").label("Quotation").description("A quotation from a work"))
    all_q = all_q + process_q(q,'Note')

    q = (WOQLQuery().add_class("ScopedValue")
         .abstract()
         # These were xdd:integerRange
         # Don't we want these to be xsd:gYear and not a range?
         # Peter says we should always permit 'unknown' and 'suspected unknown' as a value
         # Thus we should add the following.  If it is NOT asserted, then lookup its usual typed value
         # Otherwise the string is 'unknown' (high confidence) or 'suspected unknown' (low confidence) (another enum) and remove it from EpistemicState
         # .property("unknown", 'xsd:string').label("Unknown").description("Whether the value is unknown")
         .property("start", raw_gYear_type).label("From").description("The start of a time range")
         .property("end", raw_gYear_type).label("To").description("The end of a time range")
         .property("confidence", "scm:Confidence").label("Confidence").description("Qualifiers of the confidence of a variable value")
         .property("notes", "scm:Note") .label("Notes").description("Editorial notes on values"))
    all_q = all_q + process_q(q,'ScopedValue')



    # kevin.js: q11
    for p in unscoped_properties:
        if len(p) == 5:
            npid, nptype, label, description, domain = p
        else:
            npid, nptype, label, description = p
            domain = "scm:PoliticalAuthority"
        # TODO no prefix nptype?
        variable_info[label] = (npid,False,nptype) # do this before adding scm: to npid!
        npid = normaliseID(npid, "id") # add scm: if missing a prefix
        raw_nptype = ensure_raw_type(nptype)
        q = WOQLQuery().add_property(npid, raw_nptype).label(label).description(description).domain(domain)
        all_q = all_q + process_q(q,npid)

    # kevin.js: q15
    for p in scoped_properties:
        if len(p) == 6:
            npid, nptype, parents, label, description, domain = p
        else:
            npid, nptype, parents, label, description = p
            domain = "scm:PoliticalAuthority"
        parents = normaliseID(parents,"id")
        parents.append("scm:ScopedValue") # these seshat polity properties always inherit from ScopedValue -- that is where datatypes and values are stored

        nptype = normaliseID(nptype, "type") # convert to our boxed types always
        # No need to lookup 'raw' type -- that was done up when we defined our boxed types
        no_prefix_nptype = nptype.split(":")[1]
        variable_info[label] = (npid,True,no_prefix_nptype) # before we add scm: to npid

        npid = normaliseID(npid, "id")
        parents.append(nptype)
        newclass = npid + "_Value"

        if verbose:
            print(f"property: {npid} domain: {domain} type: {newclass} parents: {parents}")

        q = WOQLQuery().add_class(newclass).label(label).description(description)
        for parent in parents:
            q.parent(parent)
        all_q = all_q + process_q(q,newclass)

        q = WOQLQuery().add_property(npid, nptype).label(label).description(description).domain(domain)
        all_q = all_q + process_q(q,npid)

    if not execute_incrementally:
        execute_incrementally = True # force execution
        process_q(all_q,"Defining the Seshat Schema")

    return True

if __name__ == "__main__":
    start_time = time.time()
    db_id = "seshat_jsb_mb" #  this gets its own scm: and doc: world
    client = woql.WOQLClient(server_url = "https://127.0.0.1:6363", insecure=True)
    client.connect(key="root", account="admin", user="admin")
    existing = client.get_database(db_id, client.account())

    if existing and reset_db: # ensure this database is re-created so the schema is asserted cleanly
        print(f"Deleting database {db_id} to reset!")
        client.delete_database(db_id)
        existing = False
        
    if not existing:
        # any need to supply prefixes?  what about include_schema=True so you don't have to reload it?
        client.create_database(db_id, accountid="admin", label = "Seshat Databank Jim", description = "Create a graph with historical data")
    else:
        # updating data (and/or the schema)
        client.set_db(db_id)

    create_seshat_schema(client)
    save_schema_info((schema_declarations,variable_info,type_info))
    display_variable_info(variable_info,type_info)
    print('Execution time: %.1fs' % (time.time() - start_time))
