from flask_restful import fields

from .fields import DownloadQuery

User = {
    'id'         : fields.Integer,
    'name'       : fields.String,
    'email'      : fields.String,
    'is_enabled' : fields.Boolean,
    'is_admin'   : fields.Boolean,
    'groups'     : fields.List(fields.String(attribute='name')),
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
    'id'          : fields.Integer,
    'name'        : fields.String,
    'version'     : fields.String,
    'description' : fields.String,
    'rows'        : fields.Integer,
    'recipe'      : fields.Raw,
    'path_readme' : fields.String,
    'ts_defined'  : fields.DateTime('iso8601'),
}

File = {
    'id'            : fields.Integer,
    'name'          : fields.String,
    'version'       : fields.String,
    'description'   : fields.String,
    'size'          : fields.Integer,
    'path_readme'   : fields.String,
    'path_contents' : fields.String,
    'ts_uploaded'   : fields.DateTime('iso8601'),
}

Catalog = CatalogCollection.copy()
Catalog.update({
    'relation' : fields.String,
    'rows'     : fields.Integer,
    'summary'  : fields.String,
    'datasets' : fields.List(fields.Nested(Dataset)),
    'files'    : fields.List(fields.Nested(File)),
})

Column = {
    'name' : fields.String,
    'type' : fields.String,
}

Query = {
    'id'           : fields.Integer,
    'sql'          : fields.String,
    'format'       : fields.String,
    'status'       : fields.String,
    'job_id'       : fields.String,
    'size'         : fields.Integer,
    'ts_submitted' : fields.DateTime('iso8601'),
    'ts_started'   : fields.DateTime('iso8601'),
    'ts_finished'  : fields.DateTime('iso8601'),
    'download'     : DownloadQuery(attribute='id'),
}

Token = {
    'id'         : fields.Integer,
    'name'       : fields.String,
    'email'      : fields.String,
}