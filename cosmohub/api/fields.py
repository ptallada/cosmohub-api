from flask_restful import fields

User = {
    'id'                 : fields.Integer,
    'name'               : fields.String,
    'email'              : fields.String,
    'is_admin'           : fields.Boolean,
    'groups'             : fields.Raw,
    'ts_created'         : fields.DateTime('iso8601'),
    'ts_email_confirmed' : fields.DateTime('iso8601'),
    'ts_last_login'      : fields.DateTime('iso8601'),
}

CatalogCollection = {
    'id'           : fields.Integer,
    'name'         : fields.String,
    'version'      : fields.String,
    'description'  : fields.String,
    'is_public'    : fields.Boolean,
    'is_simulated' : fields.Boolean,
    'ts_released'  : fields.DateTime('iso8601'),
    'ts_uploaded'  : fields.DateTime('iso8601'),
}

Dataset = {
    'id'              : fields.Integer,
    'name'            : fields.String,
    'version'         : fields.String,
    'description'     : fields.String,
    'rows'            : fields.Integer,
    'recipe'          : fields.Raw,
    'ts_defined'      : fields.DateTime('iso8601'),
}

File = {
    'id'                : fields.Integer,
    'name'              : fields.String,
    'version'           : fields.String,
    'description'       : fields.String,
    'size'              : fields.Integer,
    'ts_uploaded'       : fields.DateTime('iso8601'),
}

Catalog = CatalogCollection.copy()
Catalog.update({
    'summary'  : fields.String,
    'relation' : fields.String,
    'rows'     : fields.Integer,
    'datasets' : fields.List(fields.Nested(Dataset)),
    'files'    : fields.List(fields.Nested(File)),
})

Query = {
    'id'               : fields.Integer,
    'sql'              : fields.String,
    'format'           : fields.String,
    'status'           : fields.String,
    'job_id'           : fields.String,
    'size'             : fields.Integer,
    'ts_submitted'     : fields.DateTime('iso8601'),
    'ts_started'       : fields.DateTime('iso8601'),
    'ts_finished'      : fields.DateTime('iso8601'),
}

UserToken = {
    'id'         : fields.Integer,
    'name'       : fields.String,
    'email'      : fields.String,
}

Group = {
    'id'           : fields.Integer,
    'name'         : fields.String,
    'description'  : fields.String,
    'ts_created'   : fields.DateTime('iso8601'),
}

ACL = {
    'ts_requested' : fields.DateTime('iso8601'),
    'is_granted'   : fields.Boolean,
    'ts_resolved'  : fields.DateTime('iso8601'),
}