"""JSON Schema of mongodb data structure for Bookmark Manager project.

"""
url_json_schema = {
    'type': 'object',
    'description': 'bookmark url object validation',
    'required': ['_id', 'name', 'date_added', 'id_no',
                 'url', 'icon', 'keywords'],
    'additionalProperties': False,
    'properties': {
            '_id': {
                'bsonType': 'binData',
                'description': 'url guid'
            },
            'name': {
                'bsonType': 'string',
                'description': 'url name'
            },
            'date_added': {
                'bsonType': 'date',
                'description': 'date when an url had been created'
            },
            'id_no': {
                'bsonType': 'int',
                'description': 'legacy field, not used'
            },
            'url': {
                'bsonType': 'string',
                'description': 'URL reference'
            },
            'icon': {
                'bsonType': 'string',
                'description': 'web page icon'
            },
            'keywords': {
                'bsonType': 'string',
                'description': 'web page tags'
            }
    }
}

folder_json_schema = {'$jsonSchema': {
    'bsonType': 'object',
    'title': 'bookmark folder object validation',
    'required': ['_id', 'name', 'date_added', 'id_no',
                 'parent_guid', 'children', 'date_modified'],
    'additionalProperties': False,
    'properties': {
        '_id': {
            'bsonType': 'binData',
            'description': 'folder guid'
        },
        'name': {
            'bsonType': 'string',
            'description': 'folder name'
        },
        'date_added': {
            'bsonType': 'date',
            'description': 'date when a folder had been created'
        },
        'id_no': {
            'bsonType': 'int',
            'description': 'legacy field, not used'
        },
        'parent_guid': {
            'bsonType': ['binData', 'null'],
            'description': 'folder parent_guid'
        },
        'date_modified': {
            'bsonType': 'date',
            'description': 'date when a folder has been modified'
        },
        'children': {
            'bsonType': 'array',
            'description': 'list of child urls',
            'uniqueItems': True,
            'items': url_json_schema
            }
    }
}}