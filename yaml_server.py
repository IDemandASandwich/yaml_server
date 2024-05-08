#!/usr/bin/env python3

import socket
import os
import yaml
import logging
import multiprocessing

STATUS_OK = (100,"Ok")
STATUS_NO_SUCH_KEY = (200, "No such key")
STATUS_READ_ERROR = (201,"Read error")
STATUS_FILE_FORMAT_ERROR = (202,"File format error")
STATUS_UNKNOWN_METHOD = (203,"Unknown method")
STATUS_NO_SUCH_FIELD = (204,"No such field")
STATUS_WRITE_ERROR = (205, 'Write error')
STATUS_YAML_ERROR = (206, 'YAML error')
STATUS_BAD_REQUEST = (300,"Bad request")

logging.basicConfig(level=logging.DEBUG)

class ErrorResponse(Exception):

    def __init__(self, response):

        self.response = response

class connectionClosed(Exception):

    pass

class YamlObject(dict):

    def load(self, key, lock):

        try:
            with lock:
                with open(os.path.join('data/', f"{key}.yaml"), 'r') as file:
                    content = yaml.safe_load(file)
                self.update(content)
        except FileNotFoundError:
            raise ErrorResponse(Response(STATUS_NO_SUCH_KEY))
        except OSError:
            raise ErrorResponse(Response(STATUS_READ_ERROR))
        except yaml.YAMLError:
            raise ErrorResponse(Response(STATUS_FILE_FORMAT_ERROR))
        except (KeyError,TypeError):
            raise ErrorResponse(Response(STATUS_NO_SUCH_FIELD))

    def save(self, key, lock):
        
        with lock:
            try:
                with open(os.path.join('data/', f"{key}.yaml"), 'w') as file:
                    yaml.dump(dict(self), file)
            except IOError:
                raise ErrorResponse(Response(STATUS_WRITE_ERROR))

def valid_GET_headers(req):
    headers=list(req.keys())
    if len(headers) != 2:
        return False
    elif headers[0] != 'Key' or headers[1] != 'Field':
        return False
    elif ' ' in req['Field'] or ' ' in req['Key'] or ':' in req['Key'] or '/' in req['Key']:
        return False
    else:
        return True

def valid_FIELDS_headers(req):
    header = list(req.keys())
    if len(header) != 1:
        return False
    elif 'Key' not in header or '/' in req or ' ' in req or ':' in req:
        return False
    else:
        return True

def valid_PUT_headers(req):
    headers = list(req.keys())
    if len(headers) != 3:
        return False
    elif headers[0] != 'Key' or headers[1] != 'Field' or headers[2] != 'Content-length':
        return False
    elif ' ' in req['Field'] or ' ' in req['Key'] or ':' in req['Key'] or '/' in req['Key']:
        return False
    else:
        return True

class Request:

    def __init__(self, f):

        lines = []
        while True:
            line = f.readline()
            logging.debug(f'Recieved {line}')
            line = line.decode('utf-8')
            line_strip = line.rstrip()
            lines.append(line_strip)
            if line_strip == '':
                if not line:
                    raise connectionClosed
                else:
                    break

        if lines[0] not in METHODS:
            raise ErrorResponse(Response(STATUS_UNKNOWN_METHOD))
        
        for line in lines[1:]:
            if ':' in line:
                if len(line.split(':')) > 2:
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))

        self.method = lines[0]
        self.headers = dict(line.split(':') for line in lines[1:] if ':' in line)

        if self.method == 'PUT':
            try:
                content_length = int(self.headers.get('Content-length', 0))
                line_content = yaml.safe_load(f.read(content_length).decode('utf-8'))
                logging.debug(f'recieved:{line_content}')
                self.content = line_content
            except yaml.error.YAMLError:
                raise ErrorResponse(Response(STATUS_YAML_ERROR))

class Response:

    def __init__(self, status, headers = {}, content = ""):

        self.status = status
        self.headers = headers
        self.content = content

    def send(self, f):

        f.write(f'{self.status[0]} {self.status[1]}\n'.encode('utf-8'))
        for key,item in self.headers.items():
            f.write(f'{key}:{item}'.encode('utf-8'))
        f.write('\n'.encode('utf-8'))
        if self.content:
            f.write(self.content.encode('utf-8'))
        f.flush()

def method_GET(req, lock):
    if not valid_GET_headers(req.headers):
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    try:
        filename=req.headers.get("Key")
        field=req.headers.get("Field")
        obj=YamlObject()
        obj.load(filename, lock)
        
        response=yaml.dump(obj[field])
        header={'Content-length':f'{len(response.encode("utf-8"))}\n'}
        return Response(STATUS_OK, header, response)
    except (KeyError,TypeError):
            raise ErrorResponse(Response(STATUS_NO_SUCH_FIELD))

def method_KEYS(req, lock):
    dir = 'data/'
    if not os.path.isdir(dir):
        raise ErrorResponse(Response(STATUS_READ_ERROR))
    files=os.listdir(dir)
    files=[file.replace('.yaml','') for file in files if file.endswith('.yaml')]
    keys = yaml.dump(files)
    header = {'Content-length':f'{len(keys.encode("utf-8"))}\n'}
    return Response(STATUS_OK, header, keys)

def method_FIELDS(req, lock):
    if not valid_FIELDS_headers(req.headers):
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))
    filename=req.headers.get('Key')
    logging.info(filename)
    
    obj = YamlObject()
    obj.load(filename, lock)

    fields=yaml.dump(list(obj.keys()))
    header={'Content-length':f'{len(fields.encode("utf-8"))}\n'}
    return Response(STATUS_OK, header, fields)

def method_PUT(req, lock):
    if not valid_PUT_headers(req.headers):
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))
    filename=req.headers.get('Key')
    field=req.headers.get('Field')
    new=req.content
    
    obj = YamlObject()
    obj.load(filename,lock)

    obj[field] = new
    obj.save(filename,lock)

    return Response(STATUS_OK)


METHODS={
    'GET':method_GET,
    'KEYS':method_KEYS,
    'FIELDS':method_FIELDS,
    'PUT':method_PUT,
}

def handle_client(client_socket, address, lock):
    logging.info(f"Connection established from {address}")
    f = client_socket.makefile('rwb')

    while True:
        try:
            req=Request(f)
            METHODS[req.method](req, lock).send(f)
        except ErrorResponse as exc:
            exc.response.send(f)
        except connectionClosed:
            logging.info(f'Connection closed: {address}')
            break
        f.flush()

def start_server(host='localhost', port = 9999):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
    server.bind((host, port))
    server.listen()
    logging.info(f"Server set up on {host}:{port}")
    lock = multiprocessing.Lock()

    while True:
        connection, address = server.accept()
        process=multiprocessing.Process(target=handle_client,args=(connection,address,lock))
        process.daemon=True
        process.start()
        connection.close()


if __name__ == '__main__':
    start_server()
