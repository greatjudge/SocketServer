import os
import asyncio
import socket

from collections import defaultdict
from copy import deepcopy
from dotenv import load_dotenv


class Storage:
    """Класс для хранения метрик в памяти процесса"""
    def __init__(self):
        self._data = defaultdict(dict)

    def put(self, key, value, timestamp):
        self._data[key][timestamp] = value

    def get(self, key):
        if key == '*':
            return deepcopy(self._data)
        if key in self._data:
            return {key: deepcopy(self._data.get(key))}
        return {}


class StorageDriver:
    """Класс, предосталяющий интерфейс для работы с хранилищем данных"""
    def __init__(self, storage):
        self.storage = storage

    def run_command(self, data):
        method, *params = data.split()

        if method == "put":
            key, value, timestamp = params
            value, timestamp = float(value), int(timestamp)
            self.storage.put(key, value, timestamp)
            return {}
        elif method == "get":
            key = params.pop()
            if params:
                raise ValueError()
            return self.storage.get(key)
        else:
            raise ValueError()


class Server:
    storage = Storage()
    sep='\n'
    error_message = "wrong command"
    code_err = 'error'
    code_ok = 'ok'

    def __init__(self, host, port):
        self.loop = asyncio.get_event_loop()  # Берем действующий событийный цикл, иначе создаем новый
        self.serv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.driver = StorageDriver(self.storage)
        self._host = host
        self._port = port

    def message_from_raw(self, data):
        try:
            request = data.decode()
            raw_data = self.driver.run_command(request.rstrip(self.sep))
            message = ''

            for key, values in raw_data.items():
                message += self.sep.join(f'{key} {value} {timestamp}'
                                         for timestamp, value in sorted(values.items()))
                message += self.sep
            code = 'ok'
        except (ValueError, UnicodeDecodeError, IndexError):
            message = self.error_message + self.sep
            code = self.code_err
        finally:
            return code, message

    def start(self):
        self.serv_socket.bind((self._host, self._port))
        self.serv_socket.setblocking(False)
        self.serv_socket.listen()
        self.loop.run_until_complete(self._main())

    async def _main(self):
        # Создаем асинхронную задачу и запускаем её
        await self.loop.create_task(self._accept_clients())

    async def _accept_clients(self):
        while True:
            client, addr = await self.loop.sock_accept(self.serv_socket)
            print(f'Connected with {addr}')
            self.loop.create_task(self._listen_client(client))  # await?

    async def _listen_client(self, client):
        while True:
            data = await self.loop.sock_recv(client, 2048)
            if not data:
                break
            code, message = self.message_from_raw(data)
            response = f'{code}{self.sep}{message}{self.sep}'
            await self.loop.sock_sendall(client, response.encode())


def run_server(host, port):
    server = Server(host, port)
    server.start()


if __name__ == '__main__':
    load_dotenv('.env')
    run_server(os.environ.get('SERVER_HOST'), int(os.environ.get('SERVER_PORT')))
