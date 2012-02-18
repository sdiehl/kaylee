Directions:
===========

Install ZeroMQ 2.1.x:

For Ubuntu Linux

    $ apt-get install zeromq-bin libzmq-dev libzmq0

For Arch Linux

    $ pacman -S zeromq

Build your virtualenv:

    $ cd kaylee
    $ virtualenv --no-site-packages env
    $ source env/bin/activative

Install neccesary packages:

    $ pip install -r requirements.txt 

Example
=======

Grab a large dataset (Moby Dick):

    $ wget http://www.gutenberg.org/cache/epub/2701/pg2701.txt
    $ mv pg2701.txt mobydick.txt

Run server:
    
    $ python example.py
    
Run worker(s):

    $ python kaylee/client.py

Or

    >>> from kaylee import Client
    >>> c = Client()
    >>> c.connect()
    >>> c.start()

License
=======

Released under a MIT License. Do with it what you please.
