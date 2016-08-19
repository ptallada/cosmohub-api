from flask_restful import fields

USER = {
    'id'         : fields.Integer,
    'name'       : fields.String,
    'email'      : fields.String,
    'is_enabled' : fields.Boolean,
    'is_admin'   : fields.Boolean,
    'groups'     : fields.List(fields.String(attribute='name')),
}

CATALOGS = {
    'id'           : fields.Integer,
    'name'         : fields.String,
    'version'      : fields.String,
    'description'  : fields.String,
    'is_public'    : fields.Boolean,
    'is_simulated' : fields.Boolean,
    'ts_released'  : fields.DateTime('iso8601'),
    'ts_uploaded'  : fields.DateTime('iso8601'),
}

DATASET = {
    'id'          : fields.Integer,
    'name'        : fields.String,
    'version'     : fields.String,
    'description' : fields.String,
    'rows'        : fields.Integer,
    'recipe'      : fields.Raw,
    'path_readme' : fields.String,
    'ts_defined'  : fields.DateTime('iso8601'),
}

FILE = {
    'id'            : fields.Integer,
    'name'          : fields.String,
    'version'       : fields.String,
    'description'   : fields.String,
    'size'          : fields.Integer,
    'path_readme'   : fields.String,
    'path_contents' : fields.String,
    'ts_uploaded'   : fields.DateTime('iso8601'),
}

CATALOG = CATALOGS.copy()
CATALOG.update({
    'relation' : fields.String,
    'rows'     : fields.Integer,
    'summary'  : fields.String,
    'datasets' : fields.List(fields.Nested(DATASET)),
    'files'    : fields.List(fields.Nested(FILE)),
})

COLUMN = {
    'name' : fields.String,
    'type' : fields.String,
}

QUERY = {
    'id'           : fields.Integer,
    'sql'          : fields.String,
    'format'       : fields.String,
    'status'       : fields.String,
    'job_id'       : fields.String,
    'size'         : fields.Integer,
    'ts_submitted' : fields.DateTime('iso8601'),
    'ts_started'   : fields.DateTime('iso8601'),
    'ts_finished'  : fields.DateTime('iso8601'),
}
