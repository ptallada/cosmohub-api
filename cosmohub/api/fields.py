from flask_restful import fields

User = {
    'id'                 : fields.Integer(attribute='uidNumber.value'),
    'name'               : fields.String(attribute='cn.value'),
    'email'              : fields.String(attribute='gecos.value'),
    #'is_superuser'       : fields.Boolean(default=False),
    #'groups'             : fields.Raw,
    'ts_created'         : fields.DateTime('iso8601', attribute='OA_createTimestamp.value'),
    'ts_email_confirmed' : fields.DateTime('iso8601', attribute='OA_createTimestamp.value'), # FIXME
    'ts_last_login'      : fields.DateTime('iso8601', attribute='OA_authTimestamp.value'),
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
    'ts_defined'  : fields.DateTime('iso8601'),
}

File = {
    'id'          : fields.Integer,
    'name'        : fields.String,
    'version'     : fields.String,
    'description' : fields.String,
    'size'        : fields.Integer,
    'ts_uploaded' : fields.DateTime('iso8601'),
}

Catalog = CatalogCollection.copy()
Catalog.update({
    'summary'      : fields.String,
    'citation'     : fields.String,
    'distribution' : fields.String,
    'relation'     : fields.String,
    'rows'         : fields.Integer,
    'datasets'     : fields.List(fields.Nested(Dataset)),
    'files'        : fields.List(fields.Nested(File)),
})

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
}

UserToken = {
    'id'    : fields.Integer(attribute='uidNumber.value'),
    'uid'   : fields.String(attribute='uid.value'),
    'name'  : fields.String(attribute='cn.value'),
    'email' : fields.String(attribute='gecos.value'),
}

Group = {
    'id'          : fields.Integer(attribute='gidNumber.value'),
    'name'        : fields.String(attribute='cn.value'),
    'description' : fields.String(attribute='description.value'),
    #'is_private'  : fields.Boolean(attribute='uidNumber.value'),
    'ts_created'  : fields.DateTime('iso8601', attribute='OA_createTimestamp.value'), 
}

#ACL = {
#    'ts_requested' : fields.DateTime('iso8601'),
#    'is_granted'   : fields.Boolean,
#    'is_admin'     : fields.Boolean,
#    'ts_resolved'  : fields.DateTime('iso8601'),
#}
