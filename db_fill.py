import pymongo
import pandas as pd


def rename_func(s):
    return s.replace('(', '').replace(')', '').replace(' ', '_').lower()


################################## STAGES ################################

stages = [ '0', '1', '2', '3a', '3b', '4a', '4b', '5', '7', '10a', '10b' ]

rruff_db = pd.read_csv('data/rruff_database_2021_12_25.csv',
                       #index_col='mineral_name',
                       dtype=str,
                       na_filter=False
                      ).transpose()

rruff_db_stages = {
    s: pd.read_csv(f'data/rruff_database_2021_12_25_S{s}.csv',
                   #index_col='mineral_name',
                   dtype=str,
                   na_filter=False
                  ).transpose()
    for s in stages
}

stages_where_minerals_appeared = { mineral: [] for mineral in rruff_db.keys() }
for mineral in rruff_db.keys():
    for stage in stages:
        if mineral in rruff_db_stages[stage].keys():
            stages_where_minerals_appeared[mineral].append(stage)

rruff_db.loc['Stages'] = stages_where_minerals_appeared.values()

################################## PGMs #################################

pgm = pd.read_csv('data/2021_hazen_pgm.csv',
                  index_col='Mineral Name',
                  dtype=str,
                 ).transpose().fillna(0).replace('1', 1)

def p(mineral, mode):
    try:
        return pgm[mineral][mode]
    except:
        return None

minerals = pgm.columns[1:]
modes = pgm.transpose().columns[:-3]

mineral_pgms = { mineral:
                [ mode for mode in modes if p(mineral, mode) == 1 ]
                for mineral in minerals
               }


# putting data in db appropriate form

rruff_db.rename(rename_func, inplace=True)
print(rruff_db)
d = rruff_db.transpose().to_dict('records')
for record in d:
    tmp = record['country_of_type_locality'].replace(' /', '/').replace('/ ', '/').replace('/ ', '/')

    record['chemistry_elements'] = record['chemistry_elements'].split(' ')
    record['rruff_ids'] = record['rruff_ids'].split(' ')
    record['ima_status'] = record['ima_status'].split('|')
    record['country_of_type_locality'] = tmp.split('/')
    record['crystal_systems'] = record['crystal_systems'].replace(', ', '|').split('|')
    record['valence_elements'] = record['valence_elements'].split(' ')
    record['database_id'] = int(record['database_id'])
    record['year_first_published'] = int(record['year_first_published'])
    try:
        record['pgm'] = mineral_pgms[record['mineral_name']]
    except:
        pass

print('records obtained')

val = {
    '$jsonSchema':
        {
            'bsonType': 'object',
            'required': [ 'mineral_name', 'ima_chemistry_plain', 'rruff_chemistry_plain', 'chemistry_elements', 'rruff_ids', 'ima_number', 'database_id',
                          'year_first_published', 'crystal_systems', 'valence_elements', 'ima_mineral_symbol'
                        ],
            'properties': {
                'mineral_name': {
                    'bsonType': 'string',
                    'description': 'name of the mineral'
                },
                'mineral_name_plain': {
                    'bsonType': 'string',
                    'description': 'plain name of the mineral'
                },
                'mineral_name_html': {
                    'bsonType': 'string',
                    'description': 'html name of the mineral'
                },
                'ima_chemistry_plain': {
                    'bsonType': 'string',
                    'description': 'ima chemical formula (plain)'
                },
                'ima_chemistry_concise': {
                    'bsonType': 'string',
                    'description': 'ima chemical formula (concise)'
                },
                'ima_chemistry_html': {
                    'bsonType': 'string',
                    'description': 'ima chemical formula (html)'
                },
                'rruff_chemistry_plain': {
                    'bsonType': 'string',
                    'description': 'rruff chemical formula (plain)'
                },
                'rruff_chemistry_concise': {
                    'bsonType': 'string',
                    'description': 'rruff chemical formula (concise)'
                },
                'rruff_chemistry_html': {
                    'bsonType': 'string',
                    'description': 'rruff chemical formula (html)'
                },
                'database_id': {
                    'bsonType': 'int',
                    'description': 'id in the database'
                },
                'rruff_ids': {
                    'bsonType': 'array',
                    'description': 'rruff ids',
                    'items': {
                        'bsonType': 'string',
                        'description': 'rruff id'
                    }
                },
                'ima_number': {
                    'bsonType': 'string',
                    'description': 'ima number of the mineral'
                },
                'chemistry_elements': {
                    'bsonType': 'array',
                    'description': 'the elements in the chemical formula',
                    'items': {
                        'bsonType': 'string',
                        'description': 'an element in the chemical formula'
                    }
                },
                'country_of_type_locality': {
                    'bsonType': 'array',
                    'description': 'countries of type locality',
                    'items': {
                        'bsonType': 'string',
                        'description': 'country of type locality'
                    }
                },
                'year_first_published': {
                    'bsonType': 'int',
                    'description': 'year of first published description'
                },
                'ima_status': {
                    'bsonType': 'array',
                    'description': 'ima status',
                    'items': {
                        'bsonType': 'string',
                        'description': 'ima status'
                    }
                },
                'structural_groupname': {
                    'bsonType': 'string',
                    'description': 'structural groupname'
                },
                'fleischers_groupname': {
                    'bsonType': 'string',
                    'description': 'fleischers groupname'
                },
                'status_notes': {
                    'bsonType': 'string',
                    'description': 'status notes'
                },
                'crystal_systems': {
                    'bsonType': 'array',
                    'description': 'crystal systems',
                    'items': {
                        'bsonType': 'string',
                        'description': 'crystal system',
                        'enum': [ '', 'unknown', 'amorphous', 'triclinic', 'monoclinic', 'orthorhombic', 'tetragonal', 'trigonal', 'hexagonal', 'cubic' ]
                    }
                },
                'oldest_known_age_ma': {
                    'bsonType': 'string',
                    'description': 'oldest known age (in ma)'
                },
                'valence_elements': {
                    'bsonType': 'array',
                    'description': 'valence elements',
                    'items': {
                        'bsonType': 'string',
                        'description': 'valence element'
                    }
                },
                'ima_mineral_symbol': {
                    'bsonType': 'string',
                    'description': 'ima mineral symbol'
                },
                'stages': {
                    'bsonType': 'array',
                    'description': 'stages',
                    'items': {
                        'bsonType': 'string',
                        'description': 'stage',
                        'enum': [ '0', '1', '2', '3a', '3b', '4a', '4b', '5', '6', '7', '8', '9', '10a', '10b' ]
                    }
                },
                'pgm': {
                    'bsonType': 'array',
                    'description': 'paragenetic modes',
                    'items': {
                        'bsonType': 'string',
                        'description': 'paragenetic mode',
                        'enum': [ 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p9', 'p10', 'p11',
                                  'p12', 'p13', 'p14', 'p15', 'p16', 'p17', 'p18', 'p19', 'p20', 'p21',
                                  'p22', 'p23', 'p24', 'p25', 'p26', 'p27', 'p28', 'p29', 'p30', 'p31',
                                  'p32', 'p33', 'p34', 'p35', 'p36', 'p37', 'p38', 'p39', 'p40', 'p41',
                                  'p42', 'p43', 'p44', 'p45a', 'p45b', 'p46', 'p47a', 'p47b', 'p47c',
                                  'p47d', 'p47e', 'p47f', 'p47g', 'p47h', 'p47i', 'p48', 'p49', 'p50',
                                  'p51', 'p52', 'p53', 'p54', 'p55', 'p56', 'p57', 'Lunar'
                                ]
                    }
                }
            }
        }
}

client = pymongo.MongoClient('localhost', 27017)
db = client.mineral_db
db.mineral_coll.drop()
db.create_collection('mineral_coll', validator=val)
db.mineral_coll.insert_many(d)
