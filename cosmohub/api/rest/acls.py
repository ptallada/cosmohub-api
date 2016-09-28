from flask import g
from flask_restful import (
    marshal,
    reqparse,
    Resource,
)
from sqlalchemy import literal
from sqlalchemy.orm import (
    aliased,
    contains_eager,
)
from sqlalchemy.sql import and_

from cosmohub.api import db, api_rest

from .. import fields
from ..database import model
from ..database.session import transactional_session
from ..security import (
    auth_required,
    Privilege,
)

class AclCollection(Resource):
    #decorators = [auth_required(Privilege(['user']) & Privilege(['admin']))]
    decorators = [auth_required(Privilege(['user']))]

    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            Admin_ACL = aliased(model.ACL)
            
            users = session.query(
                model.User
            ).join(
                model.Group, literal(True),
            ).join(
                model.ACL, and_(
                    model.User.id == model.ACL.user_id,
                    model.Group.id == model.ACL.group_id,
                ),
                isouter=True,
            ).join(
                Admin_ACL,
                and_(
                    Admin_ACL.group_id == model.ACL.group_id,
                    Admin_ACL.is_granted,
                    Admin_ACL.is_admin,
                ),
            ).filter(
                Admin_ACL.user_id == g.session['user'].id
            ).options(
                contains_eager(
                    model.User.acls, # @UndefinedVariable
                    model.ACL.group,
                ),
            ).all()
            
            groups = session.query(model.Group).join(
                model.Group.users_admins, # @UndefinedVariable
            ).filter(
                model.User.id == g.session['user'].id
            ).all()
            
            for user in users:
                user.groups = []
                
                for group in groups:
                    data = marshal(group, fields.Group)
                    if group in user.acls:
                        data.update(marshal(user.acls[group], fields.ACL))
                    user.groups.append(data)
            
            #g.session['track']({
            #    't' : 'event',
            #    'ec' : 'users',
            #    'ea' : 'list',
            #    'ev' : len(users),
            #})
            
            return marshal(users, fields.User)

api_rest.add_resource(AclCollection, '/acls')


class AclItem(Resource):
    decorators = [auth_required(Privilege(['user']))]

    def patch(self, id_):
        parser = reqparse.RequestParser()
        parser.add_argument('groups_granted', action='append', default=[])
        parser.add_argument('groups_revoked', action='append', default=[])

        attrs = parser.parse_args(strict=True)
        
        with transactional_session(db.session) as session:
            groups_granted = session.query(
                model.User
            ).filter(
                model.User.id == g.session['user'].id
            ).join(
                model.User.groups_administered,
            ).filter(
                model.Group.name.in_(attrs['groups_granted']),
            ).with_for_update().one().groups_administered
            
            groups_revoked = session.query(
                model.User
            ).filter(
                model.User.id == g.session['user'].id
            ).join(
                model.User.groups_administered,
            ).filter(
                model.Group.name.in_(attrs['groups_revoked']),
            ).with_for_update().one().groups_administered
            
            user = session.query(
                model.User
            ).join(
                model.User.acls, # @UndefinedVariable
            ).join(
                model.ACL.group
            ).filter(
                model.User.id == id_
            ).options(
                contains_eager(
                    model.User.acls, # @UndefinedVariable
                    model.ACL.group,
                )
            ).with_for_update().one()
            
            for group in groups_granted:
                if group not in user.acls:
                    user.acls[group] = model.ACL(group=group, user=user)
                user.acls[group].is_granted = True
            
            for group in groups_revoked:
                if group not in user.acls:
                    user.acls[group] = model.ACL(group=group, user=user)
                user.acls[group].is_granted = False
            
            #g.session['track']({
            #    't' : 'event',
            #    'ec' : 'catalogs',
            #    'ea' : 'list',
            #    'el' : catalog.id,
            #})
            
            #return data

api_rest.add_resource(AclItem, '/acls/<int:id_>')
