import logging
import urllib
import urlparse
import werkzeug.exceptions as http_exc
import zlib

from flask import g, current_app, render_template, request
from flask_restful import Resource, reqparse, marshal
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func

from cosmohub.api import db, api_rest, mail, recaptcha

from .. import fields
from ..db import model
from ..db.session import transactional_session, retry_on_serializable_error
from ..security import auth_required, Privilege, Token

log = logging.getLogger(__name__)

class UserItem(Resource):
    @auth_required(Privilege(['user']))
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            user = session.query(model.User).options(
                    joinedload('groups')
                ).filter_by(
                    id=g.session['user'].id
                ).one()

            return marshal(user, fields.User)

    @auth_required( Privilege(['user'], ['fresh']) | Privilege(['password_reset']) )
    def patch(self):
        parser = reqparse.RequestParser()
        parser.add_argument('name', store_missing=False)
        parser.add_argument('email', store_missing=False)
        parser.add_argument('password', store_missing=False)
        attrs = parser.parse_args(strict=True)

        with transactional_session(db.session) as session:
            user = session.query(model.User).join(
                model.User.groups
            ).filter_by(
                id=g.session['user'].id
            ).with_for_update().one()

            privilege = Privilege(['user'], ['fresh'])
            privilege |= Privilege(['password_reset'], [zlib.adler32(user.password.hash)])
            if not privilege.can(g.session['privilege']):
                raise http_exc.Unauthorized
            
            for key, value in attrs.iteritems():
                setattr(user, key, value)
            
            session.flush()
            
            if 'email' in attrs:
                user.ts_email_confirmed = None
                
                token = Token(
                    user, 
                    Privilege(['email_confirm'], [zlib.adler32(user.email)]), 
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['email_confirm'],
                )
                
                url = api_rest.url_for(UserEmailConfirm, auth_token=token.dump(), _external=True)
                
                mail.send_message(
                    subject = current_app.config['MAIL_SUBJECTS']['email_confirmation'],
                    recipients = [user.email],
                    body = render_template('email_confirmation.txt', user=user, url=url),
                    html = render_template('email_confirmation.html', user=user, url=url),
                )

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('name', required=True)
        parser.add_argument('email', required=True)
        parser.add_argument('password', required=True)
        parser.add_argument('groups', required=True, action='append')
        parser.add_argument('recaptcha', required=True)
        parser.add_argument('redirect_to', required=True)

        attrs = parser.parse_args(strict=True)

        if not recaptcha.verify(attrs['recaptcha']):
            raise http_exc.Unauthorized('Captcha invalid')
        del attrs['recaptcha']
        
        url = urlparse.urljoin(request.environ['HTTP_REFERER'], attrs['redirect_to'])
        del attrs['redirect_to']
        
        with transactional_session(db.session) as session:
            groups = session.query(model.Group).filter(
                model.Group.name.in_(attrs['groups']),
            ).with_for_update().all()

            if len(groups) != len(attrs['groups']):
                raise http_exc.BadRequest("One or more of the requested groups do not exist.")

            # Use the all_groups relationship
            attrs['all_groups'] = attrs['groups']
            attrs['all_groups'] = set(groups)
            del attrs['groups']
            
            user = model.User(**attrs)
            session.add(user)
            session.flush()
            
            token = Token(
                user, 
                Privilege(['email_confirm'], [zlib.adler32(user.email)]), 
                expires_in=current_app.config['TOKEN_EXPIRES_IN']['email_confirm'],
            )
            
            url += '?' + urllib.urlencode({ 'auth_token' : token.dump() })
            
            mail.send_message(
                subject = current_app.config['MAIL_SUBJECTS']['new_user'],
                recipients = [user.email],
                body = render_template('new_user.txt', user=user, url=url),
                html = render_template('new_user.html', user=user, url=url),
            )
            
            return marshal(user, fields.User), 201

    @auth_required(Privilege(['user'], ['fresh']))
    def delete(self):
        @retry_on_serializable_error
        def delete_user(user_id):
            with transactional_session(db.session) as session:
                user = session.query(model.User).filter_by(
                    id=user_id
                ).one()
                
                session.remove(user)
        
        delete_user(g.session['user'].id)

        return '', 204

api_rest.add_resource(UserItem, '/user')

class UserEmailConfirm(Resource):
    @auth_required(Privilege(['email_confirm']))
    def get(self):
        with transactional_session(db.session) as session:
            user = session.query(model.User).join(
                model.User.groups
            ).filter_by(
                id=g.session['user'].id
            ).with_for_update().one()
            
            privilege = Privilege(['email_confirm'], [zlib.adler32(user.email)])
            if not privilege.can(g.session['privilege']):
                raise http_exc.Forbidden
            
            user.ts_email_confirmed = func.now()
            
            session.flush()
            
            if not user.ts_last_login:
                mail.send_message(
                    subject = current_app.config['MAIL_SUBJECTS']['account_activated'].format(name=user.name),
                    recipients = [user.email],
                    body = render_template('account_activated.txt',  user=user),
                    html = render_template('account_activated.html', user=user),
                )
            
            return marshal(user, fields.User)

api_rest.add_resource(UserEmailConfirm, '/user/email_confirm')

class UserPasswordReset(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', required=True)
        parser.add_argument('recaptcha', required=True)
        parser.add_argument('redirect_to', required=True)
        
        attrs = parser.parse_args(strict=True)
        
        if not recaptcha.verify(attrs['recaptcha']):
            raise http_exc.Unauthorized('Captcha invalid')
        
        url = urlparse.urljoin(request.environ['HTTP_REFERER'], attrs['redirect_to'])
        
        with transactional_session(db.session, read_only=True) as session:
            user = session.query(model.User).filter_by(
                email=attrs['email']
            ).one()
            
            token = Token(
                user, 
                Privilege(['password_reset'], [zlib.adler32(user.password.hash)]), 
                expires_in=current_app.config['TOKEN_EXPIRES_IN']['password_reset'],
            )
            
            url += '?' + urllib.urlencode({ 'auth_token' : token.dump() })
            
            mail.send_message(
                subject = current_app.config['MAIL_SUBJECTS']['password_reset'],
                recipients = [user.email],
                body = render_template('password_reset.txt',  user=user, url=url),
                html = render_template('password_reset.html', user=user, url=url),
            )

api_rest.add_resource(UserPasswordReset, '/user/password_reset')
