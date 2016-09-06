import logging

from flask_restful import Resource, reqparse

from cosmohub.api import api_rest, mail

log = logging.getLogger(__name__)

class Contact(Resource):

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', store_missing=False)
        parser.add_argument('subject', store_missing=False)
        parser.add_argument('message', store_missing=False)
        attrs = parser.parse_args(strict=True)
        
        mail.send_message(
            sender = attrs['email'],
            subject = 'Contact request: ' + attrs['subject'],
            recipients = ['cosmohub@pic.es'],
            body = 'FROM: ' + attrs['email'] + '\n\n' + attrs['message'],
        )

api_rest.add_resource(Contact, '/contact')
