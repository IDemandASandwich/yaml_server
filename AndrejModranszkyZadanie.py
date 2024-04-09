#!/usr/bin/env python3

import socket
import sys
import os
import yaml

def valid_key(key):
    return ' ' not in key and ':' not in key and '/' not in key

def valid_field(field):
    return ' ' not in field

def handle_request(req):
    data = req.strip().split('\n')
    method = data[0].strip()
    headers = dict(line.split(':') for line in data[1:] if ':' in data)
    if method == 'GET':
        print()
    elif method == 'KEYS':
        print()
    elif method == 'FIELDS':
        print()
    elif method == 'SHUTDOWN':
        return "205 Server shutting down\n\n", True
    else:
        return "203 Unknown method\n\n", False

def start_server(host='localhost', port = 9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((host, port))
        server.listen()
        print(f"Server set up on {host}:{port}")
        while True:
            connection, address = server.accept()
            with connection:
                print(f"Connection established from {address}")
                while True:
                    data = connection.recv(1024)
                    if not data:
                        print("Client disconnected.")
                        break
                    response, shutdown = handle_request(data.decode('utf-8'))
                    connection.sendall(response.encode('utf-8'))
                    if shutdown:
                        print("Shutting down server.")
                        return

if __name__ == '__main__':
    start_server()