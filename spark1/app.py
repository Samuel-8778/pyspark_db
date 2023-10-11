from flask import Flask
from service1 import app # Import the blueprint

app1 = Flask(__name__)

# Register the blueprint


@app.route('/')
def hello_world():
    return 'Hello, World!'


if __name__ == '__main__':
    app.run(port=4500)
