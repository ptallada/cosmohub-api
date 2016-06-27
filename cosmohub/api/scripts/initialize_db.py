# -*- coding: utf-8 -*-
import argparse
import logging
import sys

from cosmohub.api.release import __version__

log = logging.getLogger(__name__)

def _query_yes_no(question, default='no'):
    """Ask a yes/no question via raw_input() and return their answer.

    'question' is a string that is presented to the user.
    'default' is the presumed answer if the user just hits <Enter>.
        It must be 'yes' (the default), 'no' or None (meaning
        an answer is required of the user).

    The 'answer' return value is one of 'yes' or 'no'.
    """
    valid = {'yes':'yes',   'y':'yes',  'ye':'yes',
             'no':'no',     'n':'no'}
    if default == None:
        prompt = ' [y/n]: '
    elif default == 'yes':
        prompt = ' [Y/n]: '
    elif default == 'no':
        prompt = ' [y/N]: '
    else:
        raise ValueError('invalid default answer: «%s»' % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return default
        elif choice in valid.keys():
            return valid[choice]
        else:
            sys.stdout.write('Please answer with «yes» or «no» (or «y» or «n»).\n')

def _parse_args(args):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--drop-all', '-d', action='store_true', default=False,
        help="drop entire database model before initialization")
    parser.add_argument('--help', '-?', action='help',
        help='show this help message and exit')
    parser.add_argument('--version', '-V', action='version',
        version='%%(prog)s %s' % __version__)

    options = vars(parser.parse_args(args))

    return options

def main(args=None):
    if not args:
        args = sys.argv[1:]

    options = _parse_args(args)

    from cosmohub.api.main import app, db

    with app.app_context():
        if options.pop('drop_all'):
            try:
                ans = _query_yes_no(
                    '\n'
                    'This will ERASE ALL DATA from the CosmoHub API on {bind}.\n'
                    'Are you sure?'.format(
                        bind = db.engine
                    )
                )

            except KeyboardInterrupt:
                sys.stdout.write('\n')
                return
            if not ans:
                return
            else:
                log.info('Dropping CosmoHub API model on {bind}'.format(bind=db.engine))
                db.drop_all()

        log.info('Creating CosmoHub API model on {bind}'.format(bind=db.engine))
        db.create_all()

        log.info('Done')

if __name__ == '__main__':
    sys.exit(main())
