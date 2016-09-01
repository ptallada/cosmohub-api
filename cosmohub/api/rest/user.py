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

from ..marshal import schema
from ..db import model
from ..db.session import transactional_session, retry_on_serializable_error
from ..security import (
    auth_required,
    PRIV_USER,
    PRIV_FRESH_LOGIN,
    PRIV_PASSWORD_RESET,
    PRIV_EMAIL_CONFIRM,
)

log = logging.getLogger(__name__)

class UserItem(Resource):
    @auth_required(PRIV_USER)
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            user = session.query(model.User).options(
                    joinedload('groups')
                ).filter_by(
                    id=getattr(g, 'current_user')['id']
                ).one()

            return marshal(user, schema.User)

    @auth_required( (PRIV_USER & PRIV_FRESH_LOGIN) | PRIV_PASSWORD_RESET )
    def patch(self):
        @retry_on_serializable_error
        def patch_user(user_id, attrs):
            with transactional_session(db.session) as session:
                user = session.query(model.User).options(
                    joinedload('groups')
                ).filter_by(
                    id=user_id
                ).one()

                priv = PRIV_USER & PRIV_FRESH_LOGIN
                priv |= PRIV_PASSWORD_RESET(zlib.adler32(user.password.hash))
                if not priv.can():
                    raise http_exc.Unauthorized
                
                for key, value in attrs.iteritems():
                    setattr(user, key, value)
                
                session.flush()
                
                if 'email' in attrs:
                    user.ts_email_confirmed = None
                    
                    token = marshal(user, schema.Token)
                    token.update({ 'privs' : [PRIV_EMAIL_CONFIRM(zlib.adler32(user.email)).to_list()]})
                    token = current_app.jwt.dumps(token)
                    url = api_rest.url_for(UserEmailConfirm, auth_token=token, _external=True)
                    
                    mail.send_message(
                        subject = 'confirm your mail',
                        recipients = [user.email],
                        body = render_template('email_confirmation.txt', user=user, url=url),
                        html = render_template('email_confirmation.html', user=user, url=url),
                    )
                
                return marshal(user, schema.User)

        parser = reqparse.RequestParser()
        parser.add_argument('name',     store_missing=False)
        parser.add_argument('email', store_missing=False)
        parser.add_argument('password', store_missing=False)
        attrs = parser.parse_args(strict=True)

        g.current_user = patch_user(getattr(g, 'current_user')['id'], attrs)

        return g.current_user

    def post(self):
        @retry_on_serializable_error
        def post_user(attrs):
            with transactional_session(db.session) as session:
                groups = session.query(model.Group).filter(
                    model.Group.name.in_(attrs['groups']),
                ).all()

                if len(groups) != len(attrs['groups']):
                    raise http_exc.BadRequest("One or more of the requested groups does not exist.")

                # Use the all_groups relationship
                attrs['all_groups'] = attrs['groups']
                attrs['all_groups'] = set(groups)
                del attrs['groups']
                
                url = urlparse.urljoin(request.environ['HTTP_REFERER'], attrs['redirect_to'])
                del attrs['redirect_to']
                
                user = model.User(**attrs)
                session.add(user)
                session.flush()
                
                token = marshal(user, schema.Token)
                token.update({ 'privs' : [PRIV_EMAIL_CONFIRM(zlib.adler32(user.email)).to_list()]})
                token = current_app.jwt.dumps(token)
                
                url += '?' + urllib.urlencode({ 'auth_token' : token })
                
                mail.send_message(
                    subject = 'Welcome to CosmoHub: Account activation',
                    recipients = [user.email],
                    body = render_template('new_user.txt', user=user, url=url),
                    html = render_template('new_user.html', user=user, url=url),
                )
                
                return marshal(user, schema.User)

        parser = reqparse.RequestParser()
        parser.add_argument('name',        store_missing=False)
        parser.add_argument('email',       store_missing=False)
        parser.add_argument('password',    store_missing=False)
        parser.add_argument('groups',      store_missing=False, action='append')
        parser.add_argument('recaptcha',   store_missing=False)
        parser.add_argument('redirect_to', store_missing=False)

        attrs = parser.parse_args(strict=True)

        if not recaptcha.verify(attrs['recaptcha']):
            raise http_exc.Unauthorized('Captcha invalid')
        
        del attrs['recaptcha']
        
        return post_user(attrs), 201

    @auth_required(PRIV_USER & PRIV_FRESH_LOGIN)
    def delete(self):
        @retry_on_serializable_error
        def delete_user(user_id):
            with transactional_session(db.session) as session:
                user = session.query(model.User).filter_by(
                    id=user_id
                ).one()
                
                session.remove(user)
        
        delete_user(getattr(g, 'current_user')['id'])

        return '', 204

api_rest.add_resource(UserItem, '/user')

class UserEmailConfirm(Resource):
    @auth_required(PRIV_EMAIL_CONFIRM)
    def get(self):
        @retry_on_serializable_error
        def email_confirm(user_id):
            with transactional_session(db.session) as session:
                user = session.query(model.User).options(
                    joinedload('groups')
                ).filter_by(
                    id=getattr(g, 'current_user')['id']
                ).one()
                
                priv = PRIV_EMAIL_CONFIRM(zlib.adler32(user.email))
                if not priv.can():
                    raise http_exc.Forbidden
                
                user.ts_email_confirmed = func.now()
                
                session.flush()
                
                return marshal(user, schema.User)
        
        return email_confirm(getattr(g, 'current_user')['id'])

api_rest.add_resource(UserEmailConfirm, '/user/email_confirm')

class UserPasswordReset(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', required=True)
        parser.add_argument('recaptcha', store_missing=False)
        
        attrs = parser.parse_args(strict=True)
        
        if not recaptcha.verify(attrs['recaptcha']):
            raise http_exc.Unauthorized('Captcha invalid')
        
        with transactional_session(db.session, read_only=True) as session:
            user = session.query(model.User).filter_by(
                email=attrs['email']
            ).one()
            
            token = marshal(user, schema.Token)
            token.update({ 'privs' : [PRIV_PASSWORD_RESET(zlib.adler32(user.password.hash)).to_list()]})
            token = current_app.jwt.dumps(token)
            url = api_rest.url_for(UserPasswordReset, auth_token=token, _external=True)
            
            mail.send_message(
                subject = 'Password Reset',
                recipients = [user.email],
                body = render_template('password_reset.txt',  user=user, url=url),
                html = render_template('password_reset.html', user=user, url=url),
            )

api_rest.add_resource(UserPasswordReset, '/user/password_reset')
