#!/usr/bin/env python3

import socket
import os
import yaml
import logging

STATUS_OK = "100 Ok\n"
STATUS_NO_SUCH_KEY = "200 No such key\n\n"
STATUS_READ_ERROR = "201 Read error\n\n"
STATUS_FILE_FORMAT_ERROR = "202 File format error\n\n"
STATUS_UNKNOWN_METHOD = "203 Unknown method\n\n"
STATUS_NO_SUCH_FIELD = "204 No such field\n\n"
STATUS_BAD_REQUEST = "300 Bad request\n\n"

#logging.basicConfig(level=logging.DEBUG)


class ConnectionClosed(Exception):

    pass

class BadRequest(Exception):

    pass

def valid_GET_headers(req):
    headers=list(req.keys())
    if len(headers) != 2:
        return False
    elif 'Key' not in headers[0] or 'Field' not in headers[1]:
        return False
    elif ' ' in headers or (':' or '/') in headers[0]:
        return False
    else:
        return True
    
def valid_FIELDS_headers(req):
    header = list(req.keys())
    if len(header) != 1:
        return False
    elif 'Key' not in header or ('/' or ' ' or ':') in header:
        return False
    else:
        return True

    
class Request:

    def __init__(self, f):

        lines = []
        while True:
            line = f.readline()
            line = line.decode('utf-8')
            line_strip = line.rstrip()    
            logging.debug(f'Recieved {line_strip}')
            lines.append(line_strip)
            if line_strip == '':
                if not line:
                    logging.error('Client disconnected')
                    raise ConnectionClosed
                else:
                    break
        for line in lines[1:]:
            if ':' in line:
                if len(line.split(':')) > 2:
                    raise BadRequest
        
        self.method = lines[0]
        self.content = dict(line.split(':') for line in lines[1:] if ':' in line)

class Response:

    def __init__(self, status, headers = {}, content = ""):

        self.status = status
        self.headers = headers
        self.content = content

    def send(self, f):

        f.write(f'{self.status}'.encode('utf-8'))
        for key,item in self.headers.items():
            f.write(f'{key}:{item}'.encode('utf-8'))
        if self.content:
            f.write(self.content.encode('utf-8'))
        f.flush()

def method_GET(req):
    if not valid_GET_headers(req.content):
        return Response(STATUS_BAD_REQUEST)

    filename=req.content.get("Key")
    field=req.content.get("Field")
    try:
        with open(os.path.join('data/', f"{filename}.yaml"), 'r') as file:
            content = yaml.safe_load(file)
        response=yaml.dump(content[field])
        header={'Content-length':f'{len(response.encode("utf-8"))}\n\n'}
        return Response(STATUS_OK, header, response)
    except FileNotFoundError:
        return Response(STATUS_NO_SUCH_KEY)
    except OSError:
        return Response(STATUS_READ_ERROR)
    except yaml.YAMLError:
        return Response(STATUS_FILE_FORMAT_ERROR)
    except KeyError or TypeError:
        return Response(STATUS_NO_SUCH_FIELD)

def method_KEYS(req):
    dir = 'data/'
    if not os.path.isdir(dir):
        return Response(STATUS_READ_ERROR)
    files=os.listdir(dir)
    files=[file.replace('.yaml','') for file in files if file.endswith('.yaml')]
    keys = yaml.dump(files)
    header = {'Content-length':f'{len(keys.encode("utf-8"))}\n\n'}
    return Response(STATUS_OK, header, keys)

def method_FIELDS(req):
    if not valid_FIELDS_headers(req.content):
        return Response(STATUS_BAD_REQUEST)
    filename=req.content.get('Key')
    logging.info(filename)
    try:
        with open(os.path.join('data/',f"{filename}.yaml"), 'r') as file:
            content=yaml.safe_load(file)
        fields=yaml.dump(list(content.keys()))
        header={'Content-length':f'{len(fields.encode("utf-8"))}\n\n'}
        return Response(STATUS_OK, header, fields)
    except FileNotFoundError:
        return Response(STATUS_NO_SUCH_KEY)
    except OSError:
        return Response(STATUS_READ_ERROR)
    except yaml.error.YAMLError:
        return Response(STATUS_FILE_FORMAT_ERROR)


METHODS={
    'GET':method_GET,
    'KEYS':method_KEYS,
    'FIELDS':method_FIELDS,
}

def start_server(host='localhost', port = 9999):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    logging.info(f"Server set up on {host}:{port}")

    while True:
        connection, address = server.accept()
        logging.info(f"Connection established from {address}")
        while True:
            f = connection.makefile('rwb')
            try:
                req=Request(f)
            except ConnectionClosed:
                logging.info(f'connection closed {address}')
                break;
            except BadRequest:
                Response(STATUS_BAD_REQUEST).send(f)
                continue;
            if req.method in METHODS:
                response=METHODS[req.method](req)
            else:
                response=Response(STATUS_UNKNOWN_METHOD)
            response.send(f)

        connection.close()

if __name__ == '__main__':
    start_server()
