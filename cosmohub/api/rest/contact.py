import logging

from flask import g
from flask_restful import Resource, reqparse

from cosmohub.api import api_rest, mail

log = logging.getLogger(__name__)

class Contact(Resource):

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', required=True)
        parser.add_argument('subject', required=True)
        parser.add_argument('message', required=True)
        attrs = parser.parse_args(strict=True)
        
        mail.send_message(
            sender = attrs['email'],
            subject = 'Contact request: ' + attrs['subject'],
            recipients = ['cosmohub@pic.es'],
            body = 'FROM: ' + attrs['email'] + '\n\n' + attrs['message'],
        )
        
        g.session['track']({
            't' : 'event',
            'ec' : 'contact',
        })

api_rest.add_resource(Contact, '/contact')
