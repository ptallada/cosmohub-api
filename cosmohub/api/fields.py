from flask_restful import fields

USER = {
    'id'     : fields.Integer,
    'name'   : fields.String,
    'email'  : fields.String,
    'groups' : fields.List(fields.String(attribute='name')),
}

CATALOGS = {
    'id'          : fields.Integer,
    'name'        : fields.String,
    'description' : fields.String,
    'summary'     : fields.String,
    'view'        : fields.String,
    'version'     : fields.String,
    'date'        : fields.DateTime('iso8601'),
    'public'      : fields.Boolean,
    'simulated'   : fields.Boolean,
}

PREBUILT = {
    'id'           : fields.Integer,
    'catalog_id'   : fields.Integer,
    'name'         : fields.String,
    'description'  : fields.String,
    'size'         : fields.Integer,
    'path_catalog' : fields.String,
    'path_readme'  : fields.String,
}

CATALOG = CATALOGS.copy()
CATALOG.update({
    'prebuilts'   : fields.List(fields.Nested(PREBUILT)),
})

QUERY = {
    'id'      : fields.Integer,
    'status'  : fields.String,
    'created' : fields.DateTime('iso8601', attribute='ts_created'),
    'started' : fields.DateTime('iso8601', attribute='ts_started'),
    'ended'   : fields.DateTime('iso8601', attribute='ts_ended'),
    'sql'     : fields.String,
}
