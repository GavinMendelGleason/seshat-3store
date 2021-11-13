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
      '@id' : 'EpistemicState',
      '@value' : [
          'known',
          'unknown',
          'inferred',
          'suspected unknown',
          'disputed'
      ]
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
      '@id' : 'DegreeOfCentralization',
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
      '@id' : 'EpistemicScope',
      '@abstract' : [],
      'epistemic_state' : {'@type' : 'Optional',
                           '@class' : 'EpistemicState'}
     },

    { '@type' : 'Class',
      '@id' : 'TemporalScope',
      '@abstract' : [],
      'date_range' : {'type' : 'Optional',
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
                    '@class' : 'GeoCoordinate'} },

    { '@type' : 'Class',
      '@id' : 'Variable',
      '@subdocument' : [],
      '@abstract' : [],
      '@inherits' : ['TemporalScope', 'EpistemicScope'],
     },

    { '@type' : 'Class',
      '@id' : 'TemporalVariable',
      '@subdocument' : [],
      '@abstract' : [],
      '@inherits' : ['TemporalScope'],
     },

    { '@type' : 'Class',
      '@id' : 'EpistemicVariable',
      '@subdocument' : [],
      '@abstract' : [],
      '@inherits' : ['EpistemicScope'],
     },

    { '@type' : 'Class',
      '@id' : 'EpistemicTemporalVariable',
      '@subdocument' : [],
      '@abstract' : [],
      '@inherits' : ['EpistemicVariable', 'TemporalVariable'],
     },

    { '@type' : 'Class',
      '@id' : 'PresenceVariable',
      '@subdocument' : [],
      '@key' : {'@type' : 'Hash', '@fields' : ['value']},
      '@inherits' : ['EpistemicTemporalVariable'],
      'value' : 'Presence'
     },

    { '@type' : 'Class',
      '@id' : 'DateRangeVariable',
      '@subdocument' : [],
      '@key' : {'@type' : 'Hash', '@fields' : ['value']},
      '@inherits' : ['Variable'],
      'value' : 'DateRange'
     },

    { '@type' : 'Class',
      '@id' : 'IntegerRangeVariable',
      '@subdocument' : [],
      '@key' : {'@type' : 'Hash', '@fields' : ['value']},
      '@inherits' : ['TemporalVariable'],
      'value' : 'IntegerRange'
     },

    { '@type' : 'Class',
      '@id' : 'CentralizationVariable',
      '@subdocument' : [],
      '@inherits' : ['EpistemicTemporalVariable'],
      '@key' : {'@type' : 'Hash', '@fields' : ['value']},
      'value' : 'DegreeOfCentralization'
    },

    { '@type' : 'Class',
      '@id' : 'StringVariable',
      '@subdocument' : [],
      '@inherits' : ['TemporalVariable'],
      '@key' : {'@type' : 'Hash', '@fields' : ['value']},
      'value' : 'xsd:string'
    },

   { '@type' : 'Class',
     '@id' : 'CapitalVariable',
     '@subdocument' : [],
     '@inherits' : ['TemporalVariable'],
     '@key' : {'@type' : 'Hash', '@fields' : ['value']},
     'value' : 'City'
    },

    { '@type' : 'Class',
     '@id' : 'SupraPolityRelationsVariable',
     '@subdocument' : [],
     '@inherits' : ['EpistemicTemporalVariable'],
     '@key' : {'@type' : 'Hash', '@fields' : ['value']},
     'value' : 'SupraPolityRelations'
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
    elif not (value_from == ''):
        start = int(value_from)
        if not (value_to == ''):
            end = int(value_to)
        else:
            end = start
    elif not (value_to == ''):
        end = int(value_to)
        start = end

    return (start, end)

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

def centralization(value):
    if value == '':
        return None
    return value

def presence(value):
    if re.match('present', value):
        return 'present'
    elif re.match('absent',value):
        return 'absent'
    else:
        return None

def supra_polity_relations(value):
    if value == '':
        return None
    return value

class Schema:
    def __init__(self):
        self.sections = {}
        self.subsections = {}
        self.variables = {}
        self.prop = {}

    def register(self,section,subsection):
        if not (section in self.sections):
            self.sections[section] = {'@type' : 'Class',
                                      '@id' : section,
                                      '@abstract' : []}

        if not (subsection == '' or subsection in subsection):
            self.subsections[subsection] = { '@type' : 'Class',
                                             '@id' : subsection,
                                             '@abstract' : [],
                                             '@inherits' : [section] }
            return subsection
        elif section in self.sections:
            return section
        else:
            raise Error("Unable to find section or subsection")

    def infer_type(self,variable,value_from,value_to,value_note,section):
        integer_pat = '^\s*\d+\s*$'
        presence_pat = 'present|absent'
        centralization_pat = 'none|nominal|unitary state|confederated state|quasi-polity'

        if variable in self.variables:
            return self.variables[variable]
        elif variable == 'Capital':
            return { '@type' : 'Class',
                     '@id' : variable,
                     '@subdocument' : [],
                     '@key' : { '@type' : 'Hash',
                                '@fields' : ['value'] },
                     '@inherits' : ['CapitalVariable',section]}
        elif re.match(integer_pat, value_from):
            return { '@type' : 'Class',
                     '@id' : variable,
                     '@subdocument' : [],
                     '@key' : { '@type' : 'Hash',
                                '@fields' : ['value'] },
                     '@inherits' : ['IntegerRangeVariable',section]}
        elif re.match(presence_pat, value_from):
            return { '@type' : 'Class',
                     '@id' : variable,
                     '@subdocument' : [],
                     '@key' : { '@type' : 'Hash',
                                '@fields' : ['value'] },
                     '@inherits' : ['PresenceVariable',section]}
        elif re.match(centralization_pat,value_from):
            return { '@type' : 'Class',
                     '@id' : variable,
                     '@subdocument' : [],
                     '@key' : { '@type' : 'Hash',
                                '@fields' : ['value'] },
                     '@inherits' : ['CentralizationVariable',section]}
        else:
            return { '@type' : 'Class',
                     '@id' : variable,
                     '@subdocument' : [],
                     '@key' : { '@type' : 'Hash',
                                '@fields' : ['value'] },
                     '@inherits' : ['StringVariable',section]}

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
        self.variables[variable] = ty
        family = self.infer_family(ty['@id'],value_note,date_note)
        self.prop[variable] = family

    def dump_polity(self):
        polity = {'@type' : 'Class',
                  '@id' : 'Polity'}
        for var in self.prop:
            ty = self.prop[var]
            polity[var] = ty
        return polity

    def dump_schema(self):
        polity = self.dump_polity()
        elements = [polity]
        for var in self.variables:
            elements.append(self.variables[var])
        return elements

    def infer_value(self,variable,value_from,value_to,date_from,date_to,fact_type):
        var_obj = { '@type' : variable }
        print(variable)
        if 'StringVariable' in self.variables[variable]['@inherits']:
            var_obj['value'] = value_from
        elif 'IntegerRangeVariable' in self.variables[variable]['@inherits']:

            epistemic_state = epistemic(value_from)
            if epistemic_state:
                var_obj['epistemic_state'] = epistemic_state
            else:
                integer_range = integer_from_to(value_from,value_to)
                if integer_range:
                    (lower, higher) = integer_range
                    var_obj['value'] = { '@type' : 'IntegerRange',
                                         'from' : lower,
                                         'to' : higher }

            date_range = date_from_to(date_from,date_to)
            if date_range:
                (start, end) = date_range
                var_obj['date_range'] = { '@type' : 'DateRange',
                                          'from' : start,
                                          'to' : end }
        elif 'DateRangeVariable' in self.variables[variable]['@inherits']:
            date_range = date_from_to(value_from,value_to)
            if date_range:
                (start, end) = date_range
                var_obj['value'] = { '@type' : 'DateRange',
                                     'from' : start,
                                     'to' : end }
        elif 'CapitalVariable' in self.variables[variable]['@inherits']:
            date_range = date_from_to(date_from,date_to)
            if date_range:
                (start, end) = date_range
                var_obj['date_range'] = { '@type' : 'DateRange',
                                          'from' : start,
                                          'to' : end }
            if value_from != '':
                var_obj['value'] = { '@type' : 'City',
                                     'name' : value_from }
        elif 'PresenceVariable' in self.variables[variable]['@inherits']:
            date_range = date_from_to(date_from,date_to)
            if date_range:
                (start, end) = date_range
                var_obj['date_range'] = { '@type' : 'DateRange',
                                          'from' : start,
                                          'to' : end }
            epistemic_state = epistemic(value_from)
            if epistemic_state:
                var_obj['epistemic_state'] = epistemic_state
            presence_state = presence(value_from)
            if presence_state:
                var_obj['value'] = presence_state
        elif 'CentralizationVariable' in self.variables[variable]['@inherits']:
            date_range = date_from_to(date_from,date_to)
            if date_range:
                (start, end) = date_range
                var_obj['date_range'] = { '@type' : 'DateRange',
                                          'from' : start,
                                          'to' : end }
            centralization_state = centralization(value_from)
            if centralization_state:
                var_obj['value'] = centralization_state
        elif 'SupraPolityRelationsVariable' in self.variables[variable]['@inherits']:
            date_range = date_from_to(date_from,date_to)
            if date_range:
                (start, end) = date_range
                var_obj['date_range'] = { '@type' : 'DateRange',
                                          'from' : start,
                                          'to' : end }

            epistemic_state = epistemic(value_from)
            if epistemic_state:
                var_obj['epistemic_state'] = epistemic_state

            supra_state = supra_polity_relations(value_from)
            if supra_state:
                var_obj['value'] = supra_state

        else:
            raise Exception('Unknown Variable Type')

        return var_obj

def infer_schema(csvpath):
    schema = Schema()
    with open(csvpath, newline='') as csvfile:
        next(csvfile) # drop header
        rows = csv.reader(csvfile, delimiter='|')
        for row in rows:
            (nga,polity,section,subsection,variable,value_from,value_to,
             date_from,date_to,fact_type,value_node,date_note,error_note) = row
            #variable = urllib.parse.quote(variable.lower().replace(' ', '_'))
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
        for row in rows:
            (nga,polity,section,subsection,variable,value_from,value_to,
             date_from,date_to,fact_type,value_note,date_note,error_note) = row
            if not ('Polity/' + polity == this_polity['@id']):
                this_polity = {'@type' : 'Polity', '@id' : 'Polity/' + polity }
                polities.append(this_polity)

            value = schema.infer_value(variable,
                                       value_from,
                                       value_to,
                                       date_from,
                                       date_to,
                                       value_note)
            if this_polity[variable]:
                if isinstance(this_polity[variable],list):
                    this_polity[variable].append(value)
                else:
                    this_polity[variable] = [this_polity[variable],value]
            else:
                this_polity[variable] = value
        return polities

def import_data(client,objects):
    chunk_size = 50
    size = len(objects)
    chunks = (len(objects) + 1) // chunk_size
    for m in range(0,chunks):
        start_time = time.time()
        object_slice = objects[m * chunk_size: min((m+1) * chunk_size, size)]
        results = client.insert_document(object_slice)
        print(f"Added schema objects: {results}")
        elapsed_time = (time.time() - start_time)
        time_per_polity = (elapsed_time/chunk_size)
        print(f"{insert_type} creation and insert execution time for {n_polities} polities: {elapsed_time}s ({time_per_polity}s/test polity)")

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
    print(json.dumps(objects, indent=4))
    import_data(client,objects)

if __name__ == "__main__":
    run()
