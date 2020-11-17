# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import cherrypy
import json
import argparse


class EchoServer(object):
    @cherrypy.expose
    def echo(self):
        if cherrypy.request.method == 'POST':
            body = cherrypy.request.body.read()
            print("Body: ", str(body))
            headers = cherrypy.request.headers
            print("Headers: ", str(headers))
            return json.dumps({"headers": str(headers), "body": str(body)})
        else:
            return "POST request with data expected"


parser = argparse.ArgumentParser(description='Sample http client built with seastar')
parser.add_argument('--https', dest='https', action='store_true',
                     help='run as https server')

args = parser.parse_args()
if args.https:
    # Set up SSL
    cherrypy.server.ssl_module = 'builtin'
    cherrypy.server.ssl_certificate = 'cherrypy-cert.pem'
    cherrypy.server.ssl_private_key = 'cherrypy-pkey.pem'

cherrypy.quickstart(EchoServer())
