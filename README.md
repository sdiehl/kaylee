Motivation
==========

Kaylee is a small MapReduce implementation mostly meant as a
proof of concept to illustrate the power of ZeroMQ and for
education purpose

My goal was not to write a Hadoop clone but to build a starting point
that one could use to learn about MapReduce.

The main bottleneck in this implementation is that the Shuffle
phase requires all data to be moved to the ``server`` instance
which is not generally a good idea for performance. But this lets
us a implement a simple shuffler using a Python defaultdict in
just a few lines of code which is easy to understand.

Directions:
===========

Install ZeroMQ:

For Arch Linux

    $ pacman -S zeromq

For Ubuntu Linux

    $ add-apt-repository ppa:chris-lea/zeromq
    $ apt-get update
    $ apt-get install zeromq-bin libzmq-dev libzmq0

For Macintosh:

    $ brew install zeromq

Build your virtualenv:

    $ cd kaylee
    $ virtualenv --no-site-packages env
    $ source env/bin/activative

Install necessary packages:

    $ pip install -r requirements.txt 

Example
=======

Let's do the 'hello world' of Map-Reduce, taking a large corpus
and counting the occurrences of words in parallel.

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
