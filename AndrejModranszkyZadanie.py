#!/usr/bin/env python3

import socket
import sys
import os
import yaml

STATUS_NO_SUCH_KEY = "200 No such key\n\n"
STATUS_READ_ERROR = "201 Read error\n\n"
STATUS_FILE_FORMAT_ERROR = "202 File format error\n\n"
STATUS_UNKNOWN_METHOD = "203 Unknown method\n\n"
STATUS_NO_SUCH_FIELD = "204 No such field\n\n"
STATUS_BAD_REQUEST = "300 Bad request\n\n"


def valid_key(key):
    return ' ' not in key and ':' not in key and '/' not in key and 'Key' in key

def valid_field(field):
    return ' ' not in field and 'Field' in field

def valid_GET_headers(data):
    if len(data) < 2:
        return False
    elif len(data[1]) < 2 or type(data[1]) is not dict:
        return False
    else:
        return True

def method_get(data):
    keys=list(data.keys())
    if not valid_key(keys[0]) or not valid_field(keys[1]):
        return STATUS_BAD_REQUEST,False
    
    filename=data.get("Key")
    field=data.get("Field")
    try:
        with open(os.path.join('data/', f"{filename}.yaml"), 'r') as file:
            content = yaml.safe_load(file)
        return yaml.dump(content[field]), True
    except FileNotFoundError:
        return STATUS_NO_SUCH_KEY, False
    except OSError:
        return STATUS_READ_ERROR, False
    except yaml.YAMLError:
        return STATUS_FILE_FORMAT_ERROR, False
    except KeyError or TypeError: 
        return STATUS_NO_SUCH_KEY, False

def handle_request(req):
    method = req[0]

    if method == "GET":
        if not valid_GET_headers(req):
            return STATUS_BAD_REQUEST
        
        data = req[1]
        response, success = method_get(data)
        if success:
            return f"100 OK\nContent-length:{len(response.encode('utf-8'))}\n\n{response}"
        else:
            return response
        
    else:
        return STATUS_UNKNOWN_METHOD


def start_server(host='localhost', port = 9999):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((host, port))
            server.listen()
            print(f"Server set up on {host}:{port}")
            while True:
                connection, address = server.accept()
                with connection:
                    print(f"Connection established from {address}")
                    data = []
                    data_dict = {}
                    while True:
                        line = connection.recv(1024).decode('utf-8')
                        if not line:
                            print("Client disconnected.")
                            break
                        elif line == '\n':
                            break
                        line_split = line.rstrip().split(':')
                        if len(line_split) == 2:
                            data_dict[line_split[0]] = line_split[1]
                        else:
                            data.append(line_split[0])
                    data.append(data_dict)

                    response = handle_request(data)
                    connection.sendall(response.encode('utf-8'))
                    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        server.close()

if __name__ == '__main__':
    start_server()