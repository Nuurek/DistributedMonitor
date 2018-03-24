import os


class Client:
    def __init__(self):
        self.name = f"Client {os.getpid()}"

    def run(self):
        print(f"[{self.name}] Hello world!")


if __name__ == '__main__':
    client = Client()
    client.run()
