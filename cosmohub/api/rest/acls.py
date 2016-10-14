from flask import (
    current_app,
    g,
    render_template,
)
from flask_restful import (
    marshal,
    reqparse,
    Resource,
)
from sqlalchemy import literal
from sqlalchemy.orm import (
    aliased,
    contains_eager,
    undefer_group,
)
from sqlalchemy.sql import and_

from cosmohub.api import (
    api_rest,
    db,
    mail,
)

from .. import fields
from ..database import model
from ..database.session import transactional_session
from ..security import (
    auth_required,
    Privilege,
)

class AclCollection(Resource):
    decorators = [auth_required(Privilege('/user/admin'))]

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
            ).options(
                undefer_group('text')
            ).all()
            
            for user in users:
                user.groups = []
                
                for group in groups:
                    data = marshal(group, fields.Group)
                    if group in user.acls:
                        data.update(marshal(user.acls[group], fields.ACL))
                    user.groups.append(data)
            
            g.session['track']({
                't' : 'event',
                'ec' : 'acls',
                'ea' : 'list',
                'ev' : len(users),
            })
            
            return marshal(users, fields.User)

api_rest.add_resource(AclCollection, '/acls')


class AclItem(Resource):
    decorators = [auth_required(Privilege('/user/admin'))]

    def patch(self, id_):
        parser = reqparse.RequestParser()
        parser.add_argument('groups_granted', action='append', default=[])
        parser.add_argument('groups_revoked', action='append', default=[])
        parser.add_argument('notify', required=True, type=bool)

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
            
            session.flush()
            
            if attrs['notify'] and session.dirty:
                groups = [
                    group
                    for group in user.acls
                    if user.acls[group].is_granted
                ]
                
                mail.send_message(
                    subject = current_app.config['MAIL_SUBJECTS']['acls_updated'],
                    recipients = [user.email],
                    body = render_template('mail/acls_updated.txt', groups=groups),
                    html = render_template('mail/acls_updated.html', groups=groups),
                )
            
            g.session['track']({
                't' : 'event',
                'ec' : 'acls',
                'ea' : 'edit',
                'ev' : user.id,
            })

api_rest.add_resource(AclItem, '/acls/<int:id_>')
