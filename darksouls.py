from collections import namedtuple


base_props = ['steamid', 'name', 'sl', 'phantom_type', 'mp_zone', 'world']
ext_props = ['covenant', 'indictments', 'dscm_version']
DSNode = namedtuple('DSNode', base_props)
DSCMNode = namedtuple('DSCMNode', base_props + ext_props)
