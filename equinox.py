#!/usr/bin/python3
from terminusdb_client import WOQLClient
import re
import csv
import json
import math
import os
import sys
import urllib.parse
import time

from functools import reduce
from difflib import SequenceMatcher

dbid = "seshat"
label = "Seshat"
description = "A knowledge graph of human polities."
prefixes = {'@base' : 'http://data.seshatdatabank.info/polities/',
            '@schema' : 'http://lib.seshatdatabank.info/schema#' }

basic_schema = [
    { "@type" : "Class",
      "@id" : "GeoCoordinate",
      "@subdocument" : [],
      "@key" : { "@type" : "Lexical",
                 "@fields" : ["latitude", "longitude"] },
      "latitude" : "xsd:decimal",
      "longitude" : "xsd:decimal"
    },

    { "@type" : "Class",
      "@id" : "GeoPolyline",
      "@subdocument" : [],
      "@key" : { "@type" : "Hash",
                 "@fields" : ["polyline"] },
      "polyline" : { "@type" : "List",
                     "@class" : "GeoCoordinate" }
    },

    { "@type" : "Class",
      "@id" : "GeoPolygon",
      "@subdocument" : [],
      "@key" : { "@type" : "Hash",
                 "@fields" : ["polygon"] },
      "polygon" : { "@type" : "List",
                    "@class" : "GeoCoordinate" }
    },

    { "@type" : "Class",
      "@id" : "GeoMultiCoordinate",
      "@subdocument" : [],
      "@key" : { "@type" : "Hash",
                 "@fields" : ["coordinates"] },
      "coordinates" : { "@type" : "Set",
                        "@class" : "GeoCoordinate" }
    },

    { '@type' : 'Class',
      '@id' : 'DateRange',
      '@subdocument' : [],
      "@key" : { "@type" : "Hash",
                 "@fields" : ["from", "to"] },
      'from' : 'xsd:gYear',
      'to' : 'xsd:gYear'
     },

    { '@type' : 'Class',
      '@id' : 'IntegerRange',
      '@subdocument' : [],
      "@key" : { "@type" : "Hash",
                 "@fields" : ["from", "to"] },
      'from' : 'xsd:integer',
      'to' : 'xsd:integer'
     },

    { '@type' : 'Enum',
      '@id' : 'Presence',
      '@value' : [
          'present',
          'absent'
      ]
     },

    { '@type' : 'Enum',
      '@id' : 'SupraPolityRelations',
      '@value' : [
          'none',
          'alliance',
          'vassalage',
          'nominal'
      ]
     },

    { '@type' : 'Enum',
      '@id' : 'Centralization',
      '@value' : [
          'none',
          'nominal',
          'unitary state',
          'confederated state',
          'quasi-polity',
      ]
     },

    { '@type' : 'Enum',
      '@id' : 'LineageType',
      '@value' : [
          'continuity',
          'hostile',
          'disruption/continuity',
          'elite migration',
          'population migration',
          'cultural assimilation'
      ]
     },

    { '@type' : 'Class',
      '@id' : 'TemporalScope',
      '@abstract' : [],
      'date_range' : {'@type' : 'Optional',
                      '@class' : 'DateRange'}
     },

    { '@type' : 'Class',
      '@id' : 'City',
      '@key' : {'@type' : 'Lexical',
                '@fields' : [ 'name' ]},
      'name' : 'xsd:string',
      'alternative names' : {'@type' : 'Set',
                             '@class' : 'xsd:string' },
      'location' : {'@type' : 'Optional',
                    '@class' : 'GeoCoordinate'}
     },

    { '@type' : 'Class',
      '@id' : 'Value',
      '@subdocument' : [],
      '@abstract' : [],
      '@inherits' : 'TemporalScope',
     },

    { '@type' : 'Class',
      '@id' : 'IntegerValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'xsd:integer'
     },

    { '@type' : 'Class',
      '@id' : 'PresenceValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'Presence'
     },

    { '@type' : 'Class',
      '@id' : 'StringValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'xsd:string'
     },

    { '@type' : 'Class',
      '@id' : 'DateRangeValue',
      '@subdocument' : [],
      '@key' : {'@type' : 'Random'},
      'value' : 'DateRange'
     },

    { '@type' : 'Class',
      '@id' : 'IntegerRangeValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'IntegerRange'
     },

    { '@type' : 'Class',
      '@id' : 'CentralizationValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'Centralization'
     },

    { '@type' : 'Class',
      '@id' : 'CapitalValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'City'
     },

    { '@type' : 'Class',
      '@id' : 'SupraPolityRelationsValue',
      '@subdocument' : [],
      '@inherits' : ['TemporalScope'],
      '@key' : {'@type' : 'Random'},
      'value' : 'SupraPolityRelations'
     },

    { '@type' : 'Class',
      '@id' : 'Variable',
      '@subdocument' : [],
      '@abstract' : [],
      '@inherits' : [],
     },

    { '@type' : 'Class',
      '@id' : 'PolityLineage',
      'preceding (quasi)polity' : 'Polity',
      'succeeding (quasi)polity' : 'Polity',
      'relationship' : { '@type' : 'Set',
                         '@class' : 'LineageType' }
     }
]

def date_range_object(date_string):
    bce_date = '^(\d+)\s*BCE$'
    ce_date = '^(\d+)\s*CE$'

    bce_date_range = '^(\d+)\s*-\s*(\d+)\s*BCE$|^(\d+)\s*BCE\s*-\s*(\d+)\s*BCE$'
    ce_date_range = '^(\d+)\s*-\s*(\d+)\s*CE$|^(\d+)\s*CE-\s*(\d+)\s*CE$'
    bce_ce_date_range = '^(\d+)\s*BCE-\s*(\d+)\s*CE$'

    m = re.match(bce_date,date_string)
    if m:
        from_time = - int(m[1])
        to_time = from_time
        return { 'from' : from_time, 'to' : to_time }

    m = re.match(ce_date,date_string)
    if m:
        from_time = int(m[1])
        to_time = from_time
        return { 'from' : from_time, 'to' : to_time }

    m = re.match(bce_date_range,date_string)
    if m:
        start = m[1] if m[1] else m[3]
        end = m[2] if m[2] else m[4]
        from_time = -int(start)
        to_time = -int(end)
        return { 'from' : from_time, 'to' : to_time }

    m = re.match(ce_date_range,date_string)
    if m:
        start = m[1] if m[1] else m[3]
        end = m[2] if m[2] else m[4]
        from_time = int(start)
        to_time = int(end)
        return { 'from' : from_time, 'to' : to_time }

    m = re.match(bce_ce_date_range,date_string)
    if m:
        start = m[1] if m[1] else m[3]
        end = m[2] if m[2] else m[4]
        from_time = int(start)
        to_time = int(end)
        return { 'from' : from_time, 'to' : to_time }

    raise Exception('Unable to parse date')

def integer_from_to(value_from, value_to):
    range_pat = "(\d+)-(\d+)"
    m = re.match(range_pat, value_from)
    if m:
        start = int(m[1])
        end = int(m[2])
    elif re.match('\d+',value_from):
        start = int(value_from)
        if re.match('\d+',value_to):
            end = int(value_to)
        else:
            end = start
    elif re.match('\d+',value_to):
        end = int(value_to)
        start = end
    else:
        return None
    return [start, end]

def date_gyear(date_string):
    bce_date = '^(\d+)\s*BCE$'
    ce_date = '^(\d+)\s*CE$'
    if date_string == '':
        return None

    m = re.match(bce_date,date_string)
    if m:
        return - int(m[1])
    m = re.match(ce_date,date_string)
    if m:
        return int(m[1])

    raise Exception(f"Could not parse date: {date_string}")

def date_from_to(value_from, value_to):
    start = date_gyear(value_from)
    end = date_gyear(value_to)
    if not start and not end:
        return None
    elif not start:
        start = end
    elif not end:
        end = start
    else:
        pass
    return (start,end)

def epistemic(value):
    print(value)
    if re.match('suspected unknown', value):
        return 'suspected unknown'
    elif re.match('unknown',value):
        return 'unknown'
    elif re.match('known',value):
        return 'known'
    elif re.match('inferred',value):
        return 'inferred'
    elif re.match('disputed',value):
        return 'disputed'
    elif value == '':
        return None
    else:
        return 'known' # Is this correct?

def epistemic_family(variable,section,value_range):
    variable_class = class_name(variable)
    section_class = class_name(section)
    return { '@type' : 'Class',
             '@id' : variable_class,
             '@subdocument' : [],
             '@key' : { '@type' : 'Random' },
             '@oneOf' : {
                 'known' : value_range,
                 'unknown' : 'sys:Unit',
                 'suspected_unknown' : 'sys:Unit',
                 'inferred' : value_range
             }
            }

def centralization(value):
    if value == '':
        return None
    return value

def presence(value):
    if re.match('.*present', value):
        return 'present'
    elif re.match('.*absent',value):
        return 'absent'
    else:
        return None

def supra_polity_relations(value):
    if re.match('.*vassalage', value):
        return 'vassalage'
    elif re.match('.*none',value):
        return 'none'
    elif re.match('.*alliance',value):
        return 'alliance'
    elif re.match('.*nominal', value):
        return 'nominal'
    elif value == '':
        return None
    else:
        return value

def prop_name(name):
    return urllib.parse.quote(name.lower().replace(' ', '_'))

def class_name(name):
    return urllib.parse.quote(name.title().replace(' ', ''))

def epistemic_instance(var_obj,value_type,epistemic_state,value,date_range):
    value_obj = {'@type' : value_type, 'value' : value}
    if date_range:
        start,end = date_range
        value_obj['date_range'] = { '@type' : 'DateRange',
                                    'from' : start,
                                    'to' : end}
    if value is None:
        var_obj['unknown'] = []
        return var_obj
    if epistemic_state == 'known':
        var_obj['known'] = value_obj
        return var_obj
    elif epistemic_state == 'unknown':
        var_obj['unknown'] = []
        return var_obj
    elif epistemic_state == 'suspected unknown':
        var_obj['suspected_unknown'] = []
        return var_obj
    elif epistemic_state == 'inferred':
        var_obj['inferred'] = value_obj
        return var_obj
    else:
        print(f"Warning: Failure to process epistemic_state: {epistemic_state} {var_obj}")

class Schema:
    def __init__(self):
        self.sections = {}
        self.subsections = {}
        self.variables = {}
        self.prop = {}

    def register(self,section,subsection):
        section_class = class_name(section)
        section_prop = prop_name(section)
        subsection_class = class_name(subsection)
        subsection_prop = prop_name(subsection)

        if not (section in self.sections):
            self.sections[section] = {'@type' : 'Class',
                                      '@id' : section_class }
        if not (subsection == '') and not (subsection in self.subsections):
            self.subsections[subsection] = { '@type' : 'Class',
                                             '@id' : subsection_class }
            self.sections[section][subsection_prop] = { '@type' : 'Optional',
                                                        '@class' : subsection_class }
            return subsection
        if not (subsection == ''):
            return subsection
        elif section in self.sections:
            return section
        else:
            raise Exception("Unable to find section or subsection")


    def infer_type(self,variable,value_from,value_to,value_note,section):
        integer_pat = '^\s*\d+\s*$'
        presence_pat = '.*present.*|.*absent.*'
        centralization_pat = 'nominal|unitary state|confederated state|quasi-polity'

        if variable in self.variables:
            # We may need to upgrade here...
            return self.variables[variable]
        elif variable == 'Capital':
            return epistemic_family(variable,section,'CapitalValue')
        elif variable == 'Supra-polity relations':
            return epistemic_family(variable,section,'SupraPolityRelationsValue')
        elif re.match(integer_pat, value_from):
            return epistemic_family(variable,section,'IntegerValue')
        elif re.match(presence_pat, value_from):
            return epistemic_family(variable,section,'PresenceValue')
        elif re.match(centralization_pat,value_from):
            return epistemic_family(variable,section,'CentralizationValue')
        else:
            return epistemic_family(variable,section,'StringValue')

    def infer_family(self,typeid,value_note,date_note):
        if value_note == 'complex':
            return { '@type' : 'Set',
                     '@class' : typeid }
        else:
            return typeid

    def register_variable(self,
                          variable,
                          value_from,
                          value_to,
                          date_from,
                          date_to,
                          fact_type,
                          value_note,
                          date_note,
                          error_note,
                          section_class):
        ty = self.infer_type(variable,value_from,value_to,value_note,section_class)
        variable_class = class_name(variable)
        variable_prop = prop_name(variable)
        if section_class in self.sections:
            self.sections[section_class][variable_prop] = {'@type' : 'Set',
                                                           '@class' : variable_class }
        elif section_class in self.subsections:
            section_class_def = self.subsections[section_class]
            self.subsections[section_class][variable_prop] = {'@type' : 'Set',
                                                              '@class' : variable_class}
        else:
            raise Exception(f"Unknown section title: {section_class}")

        self.variables[variable] = ty
        family = self.infer_family(ty['@id'],value_note,date_note)
        self.prop[variable] = family

    def dump_polity(self):
        polity = {'@type' : 'Class',
                  '@id' : 'Polity'}
        for section in self.sections:
            section_prop = prop_name(section)
            section_class = class_name(section)
            polity[section_prop] = { '@type' : 'Optional', '@class' : section_class}
        return polity

    def dump_schema(self):
        polity = self.dump_polity()
        elements = [polity]
        keys = []
        for var in self.variables:
            elements.append(self.variables[var])
        for key in self.sections:
            elements.append(self.sections[key])
        for key in self.subsections:
            elements.append(self.subsections[key])
        return elements

    def infer_value(self,var_obj,variable,value_from,value_to,date_from,date_to,fact_type):
        value_type = self.variables[variable]['@oneOf']['known']
        if 'StringValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            return epistemic_instance(var_obj,value_type,epistemic_state,value_from,date_range)
        elif 'IntegerRangeValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            integer_range = integer_from_to(value_from,value_to)
            return epistemic_instance(var_obj,value_type,epistemic_state,integer_range,date_range)
        elif 'IntegerValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            integer_range = integer_from_to(value_from,value_to)
            if integer_range:
                (start,end) = integer_range
            else:
                start = None
            return epistemic_instance(var_obj,value_type,epistemic_state,start,date_range)
        elif 'DateRangeValue' == value_type:
            date_range = date_from_to(value_from,value_to)
            return epistemic_instance(var_obj,value_type,epistemic,date_range,None)
        elif 'CapitalValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            if value_from != '':
                 city = { '@type' : 'City',
                          'name' : value_from }
                 return epistemic_instance(var_obj,value_type,epistemic_state,city,date_range)
            else:
                return None
        elif 'PresenceValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            presence_state = presence(value_from)
            return epistemic_instance(var_obj,value_type,epistemic_state,presence_state,date_range)
        elif 'CentralizationValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            centralization_state = centralization(value_from)
            return epistemic_instance(var_obj,value_type,epistemic_state,centralization_state,date_range)
        elif 'SupraPolityRelationsValue' == value_type:
            date_range = date_from_to(date_from,date_to)
            epistemic_state = epistemic(value_from)
            supra_state = supra_polity_relations(value_from)
            return epistemic_instance(var_obj,value_type,epistemic_state,supra_state,date_range)
        else:
            raise Exception(f"Unknown Value Type {value_type} for {variable}")

        return var_obj

def extend_polity(polity, section, subsection, variable, value):
    if value is None:
        return

    variable_class = class_name(variable)
    variable_prop = prop_name(variable)
    section_class = class_name(section)
    section_prop = prop_name(section)
    subsection_class = class_name(subsection)
    subsection_prop = prop_name(subsection)

    if not (section_prop in polity):
        polity[section_prop] = { '@type' : section_class }

    if subsection != '':
        if subsection_prop not in polity[section_prop]:
            subsection_obj = {'@type' : subsection_class}
            polity[section_prop][subsection_prop] = subsection_obj
        else:
            subsection_obj = polity[section_prop][subsection_prop]

        #print(f"subsection_obj: {subsection_obj}")
        if variable_prop in subsection_obj:
            subsection_obj[variable_prop].append(value)
        else:
            subsection_obj[variable_prop] = [value]
        #polity[section_prop][subsection_prop][variable_prop] = variable_obj
    else:
        section_obj = polity[section_prop]

        if variable_prop in section_obj:
            section_obj[variable_prop].append(value)
        else:
            section_obj[variable_prop] = [value]
        #polity[section_prop][variable_prop] = value

def infer_schema(csvpath):
    schema = Schema()
    with open(csvpath, newline='') as csvfile:
        next(csvfile) # drop header
        rows = csv.reader(csvfile, delimiter='|')
        for row in rows:
            (nga,polity,section,subsection,variable,value_from,value_to,
             date_from,date_to,fact_type,value_node,date_note,error_note) = row
            section = section.title()
            subsection = subsection.title()
            variable_class = class_name(variable)
            variable_prop = prop_name(variable)
            if polity == 'Code book':
                pass # special case?
            else:
                section_class = schema.register(section,subsection)
                schema.register_variable(variable,
                                         value_from,
                                         value_to,
                                         date_from,
                                         date_to,
                                         fact_type,
                                         value_node,
                                         date_note,
                                         error_note,
                                         section_class)
    return schema

def import_schema(client,schema_objects):
    results = client.insert_document(schema_objects,
                                     graph_type="schema")
    print(f"Added schema objects: {results}")

def load_data(csvpath,schema):
    with open(csvpath, newline='') as csvfile:
        next(csvfile) # drop header
        rows = csv.reader(csvfile, delimiter='|')
        polities = []
        this_polity = { '@id' : None }
        var_obj = { '@type' : None }
        for row in rows:
            (nga,polity,section,subsection,variable,value_from,value_to,
             date_from,date_to,fact_type,value_note,date_note,error_note) = row
            if polity == 'Code book':
                continue # string out of band value
            section = section.title()
            subsection = subsection.title()
            variable_class = class_name(variable)
            variable_prop = prop_name(variable)
            section_class = class_name(section)
            section_prop = prop_name(section)
            subsection_class = class_name(subsection)
            subsection_prop = prop_name(subsection)

            if not ('Polity/' + polity == this_polity['@id']):
                this_polity = { '@id' : 'Polity/' + polity,
                                '@type' : 'Polity' }
                polities.append(this_polity)

            var_obj = { '@type' : variable_class }
            value = schema.infer_value(var_obj,
                                       variable,
                                       value_from,
                                       value_to,
                                       date_from,
                                       date_to,
                                       value_note)
            extend_polity(this_polity, section, subsection, variable, var_obj)

        return polities

def import_data(client,objects):
    chunk_size = 1
    size = len(objects)
    chunks = (len(objects) + 1) // chunk_size
    for m in range(0,chunks):
        start_time = time.time()
        object_slice = objects[m * chunk_size: min((m+1) * chunk_size, size)]
        print(json.dumps(object_slice, indent=4))
        results = client.insert_document(object_slice)
        #results = client.insert_document(document)
        print(f"Added documents: {results}")
        elapsed_time = (time.time() - start_time)
        time_per_polity = (elapsed_time/chunk_size)
        print(f"Creation and insert execution time for {chunk_size} polities: {elapsed_time}s ({time_per_polity}s/test polity)")

def run():
    csvpath = "equinox.csv"
    key = os.environ['TERMINUSDB_ACCESS_TOKEN']
    endpoint = os.environ['TERMINUSDB_ENDPOINT']
    team = os.environ['TERMINUSDB_TEAM']
    team_quoted = urllib.parse.quote(team)
    #client = WOQLClient(f"https://cloud.terminusdb.com/{team_quoted}/")
    if endpoint == 'http://127.0.0.1:6363':
        client = WOQLClient(f"{endpoint}")
    else:
        client = WOQLClient(f"{endpoint}/{team_quoted}/")

    # make sure you have put the token in environment variable
    # https://docs.terminusdb.com/v10.0/#/terminusx/get-your-api-key
    # print(f"key: {key}")
    use_token = True
    if key == 'false':
        use_token = False
        client.connect(user='admin', team=team, use_token=use_token)
    else:
        client.connect(team=team, use_token=use_token)

    exists = client.get_database(dbid)
    if exists:
        client.delete_database(dbid, team=team)

    client.create_database(dbid,
                           team,
                           label=label,
                           description=description,
                           prefixes=prefixes)

    # client.delete_database(dbid, team=team, force=True)

    schema = infer_schema(csvpath)
    schema_objects = basic_schema + schema.dump_schema()
    print(json.dumps(schema_objects, indent=4))
    import_schema(client,schema_objects)
    objects = load_data(csvpath,schema)
    # print(json.dumps(objects, indent=4))
    import_data(client,objects)

if __name__ == "__main__":
    run()
