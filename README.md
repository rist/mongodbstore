This is a backend for the RingoJS Storable interface based on MongoDB[1].

  [1]: http://www.mongodb.org/

__THIS CODE HASN'T BEEN UPDATED IN 3 YEARS AND IS PROBABLY NOT COMPATIBLE ANY MORE WHITH CURRENT VERSIONS OF RINGOJS__

Requirements
============

It is written against version 1.4.3 of MongoDB.

Notes
=============

This code is heavily inspired (read copy&paste) by the googlestore storage layer of RingoJS.

Functionality
=============

Initializing the store:

    include('ringo/storage/mongodbstore');
    store = new Store('server', 27017, 'dbName');

Creating a new Storable class:

    Book = store.defineEntity('book');

Creating and saving a new Storable instance:

    var b = new Book({title: "DBs for dummies"});
    b.save();

Retrieving all objects from a db:

    var books = Book.all();

Retrieving an object by id:

    var book = Book.get(id);

Deleting an object from the db:

    book.remove();

Running a query on the database:

    Book.query().equals('prop', value).select();

The Query.select() method takes an optional argument to retrieve
just a property rather than the whole object, e.g. '_id' or 'title'.

