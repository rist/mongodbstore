// Run w/, e.g.: $ ringo test/all

var log = require('ringo/logging').getLogger(module.id);
var arrays = require('ringo/utils/arrays');

include("console");
include('test');
include('assert');

var {Store} = require('../lib/ringo/storage/mongodbstore');
var {Storable} = require('ringo-storable');
var store = new Store('localhost', 27017, "mongodbstore");

var personId, person;

var Person = store.defineEntity('Person', {properties : {firstName : "string", lastName : "string", birthDate : "date", birthYear : "number", ssn : "number", vitae : "string", address : "Address", phoneNumbers : "array"}});
var Address = store.defineEntity('Address', {properties : {city : "string", street : "string"}});
var PhoneNumber = store.defineEntity('PhoneNumber', {properties : {prefix : "number", number : "number"}})

const FIRST_NAME_1 = 'Hans';
const FIRST_NAME_2 = 'Herbert';
const LAST_NAME = 'Wurst';
const BIRTH_DATE_MILLIS = 123456789000;
const BIRTH_YEAR = new Date(BIRTH_DATE_MILLIS).getFullYear();
const SSN_1 = 'AT-1234291173';
const SSN_2 = 'AT-4321291173';
const SSN_3 = 'AT-1235291173';
const SSN_4 = 'AT-5321291173';
const VITAE = 'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, ' +
        'sed diam nonumy eirmod tempor invidunt ut labore et dolore magna ' +
        'aliquyam erat, sed diam voluptua. At vero eos et accusam et justo ' +
        'duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata ' +
        'sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, ' +
        'consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ' +
        'ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero ' +
        'eos et accusam et justo duo dolores et ea rebum. Stet clita kasd ' +
        'gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.';

exports.setUp = exports.tearDown = function () {
    for each (let instance in Person.all()) {
       instance.remove(); // Clean up.
    }
    for each (let instance in Address.all()) {
       instance.remove(); // Clean up.
    }
};

exports.testPersistCreation = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    equal(FIRST_NAME_1, person.firstName);
    equal(LAST_NAME, person.lastName);
    deepEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    equal(BIRTH_YEAR, person.birthYear);
    equal(VITAE, person.vitae);
};

exports.testPersistUpdating = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    personId = person._id;
    person.firstName = FIRST_NAME_2;
    person.save();
    person = Person.get(personId);
    assertPerson();
    equal(FIRST_NAME_2, person.firstName);
    equal(LAST_NAME, person.lastName);
    deepEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    equal(BIRTH_YEAR, person.birthYear);
    equal(VITAE, person.vitae);
};

exports.testPersistDeletion = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    personId = person._id;
    person.remove();
    person = Person.get(personId);
    isNull(person);
    equal(0, Person.all().length);
};
 
// exports.testPersistOneToOneRelations = function () {
//     person = createTestPerson();
//     var address = new Address();
//     address.city = "Vienna";
//     address.street = "Börsegasse 11";
//     person.address = address;
//     person.save();
// 
//     isNotUndefined(Address.all()[0]);
//     isNotUndefined(Person.all()[0].address);
// 
//     deepEqual(Person.all()[0].address, Address.all()[0]);
// };

// exports.testPersistRelations = function () {
//     person = createTestPerson();
//     person.save();
//     var phoneA = new PhoneNumber();
//     phoneA.prefix = "0043";
//     phoneA.number = "01123456";
//     var phoneB = new PhoneNumber();
//     phoneB.city = "0049";
//     phoneB.street = "0234568";
//     person.addresses = [phoneA, phoneB];
//     person.save();
//     var phoneNumbers = Person.all()[0].phoneNumbers;
//     equal(2, phoneNumbers.length);
//     isTrue(phoneNumbers[0] != phoneNumbers[1] && phoneNumbers[0] == phoneA || phoneNumbers[0] == phoneB && phoneNumbers[1] == phoneA || phoneNumbers[1] == phoneB)
// };

exports.testMapReduce = function () {
    var personA = createTestPerson();
    personA.save();
    var personB = createTestPerson();
    personB.save();
    var personC = createTestPerson();
    personC.firstName = FIRST_NAME_2;
    personC.save();
    var mappedAndReducedByFirstName = Person.map(
        function(){emit(this.firstName, 1);}, "tempOutputCollection").reduce(
            function(k, values){var sum = 0; for(var v in values){sum += values[v];} return sum;}
        ).select();
    equal(2, mappedAndReducedByFirstName.Hans);
    equal(1, mappedAndReducedByFirstName.Herbert);
};

exports.testBasicQuerying = function () {
    person = createTestPerson();
    person.save();
    person = createTestPerson();
    person.firstName = FIRST_NAME_2;
    person.ssn = SSN_2;
    person.save();
    isTrue(Person.all()[0] instanceof Storable &&
            Person.all()[0] instanceof Person);
    equal(2, Person.all().length);
    equal(LAST_NAME, Person.all()[0].lastName);
    var testQuery = Person.query().equals('lastName', LAST_NAME);
    equal(2, testQuery.select().length);
    var queriedPerson = Person.query().equals('firstName', FIRST_NAME_1).
            select()[0];
    isTrue(queriedPerson instanceof Storable &&
            queriedPerson instanceof Person);
    equal(1, Person.query().equals('firstName', FIRST_NAME_1).select().
            length);
    equal(FIRST_NAME_1, Person.query().equals('firstName', FIRST_NAME_1).
            select('firstName')[0]);
    equal(2, Person.query().equals('lastName', LAST_NAME).select().
            length);
    equal(SSN_2, Person.query().equals('lastName', LAST_NAME).
            equals('firstName', FIRST_NAME_2).select('ssn')[0]);
    testGreaterLessQuerying();
    testOrderByQuerying();
    testSliceQuerying();
};

function testGreaterLessQuerying() {
    equal(2, Person.query().greater('birthYear', BIRTH_YEAR - 1).select().
            length);
    equal(0, Person.query().greater('birthYear', BIRTH_YEAR + 1).select().
            length);
    equal(2, Person.query().less('birthYear', BIRTH_YEAR + 1).select().
            length);
    equal(0, Person.query().less('birthYear', BIRTH_YEAR - 1).select().
            length);
    equal(2, Person.query().greaterEquals('birthYear', BIRTH_YEAR).
            select().length);
    equal(2, Person.query().greaterEquals('birthYear', BIRTH_YEAR - 1).
            select().length);
    equal(0, Person.query().greaterEquals('birthYear', BIRTH_YEAR + 1).
            select().length);
    equal(2, Person.query().lessEquals('birthYear', BIRTH_YEAR).select().
            length);
    equal(2, Person.query().lessEquals('birthYear', BIRTH_YEAR + 1).
            select().length);
    equal(0, Person.query().lessEquals('birthYear', BIRTH_YEAR - 1).
            select().length);
    equal(2, Person.query().greater('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    equal(0, Person.query().greater('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    equal(2, Person.query().less('birthDate', new Date(BIRTH_DATE_MILLIS +
            1000)).select().length);
    equal(0, Person.query().less('birthDate', new Date(BIRTH_DATE_MILLIS)
            ).select().length);
    equal(2, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    equal(2, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    equal(0, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS + 1000)).select().length);
    equal(2, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    equal(2, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS + 1000)).select().length);
    equal(0, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    equal(LAST_NAME, Person.query().equals('lastName', LAST_NAME).
            greater('birthDate', new Date(BIRTH_DATE_MILLIS - 1000)).
            less('birthYear', BIRTH_YEAR + 1).select('lastName')[0]);
}

function testOrderByQuerying() {
    equal(2, Person.query().orderBy('ssn').select().length);
    equal(SSN_1, Person.query().orderBy('ssn').select('ssn')[0]);
    equal(2, Person.query().orderBy('-ssn').select().length);
    equal(SSN_2, Person.query().orderBy('-ssn').select('ssn')[0]);
    equal(2, Person.query().equals('lastName', LAST_NAME).
            orderBy('firstName').select().length);
    equal(FIRST_NAME_1, Person.query().equals('lastName', LAST_NAME).
            orderBy('firstName').select('firstName')[0]);
    equal(FIRST_NAME_2, Person.query().equals('lastName', LAST_NAME).
            orderBy('firstName').select('firstName')[1]);
    equal(2, Person.query().equals('lastName', LAST_NAME).
            orderBy('-firstName').select().length);
    equal(FIRST_NAME_2, Person.query().equals('lastName', LAST_NAME).
            orderBy('-firstName').select('firstName')[0]);
    equal(FIRST_NAME_1, Person.query().equals('lastName', LAST_NAME).
            orderBy('-firstName').select('firstName')[1]);
}

function testSliceQuerying() {
    person = createTestPerson();
    person.ssn = SSN_3;
    person.save();
    person = createTestPerson();
    person.ssn = SSN_4;
    person.save();
    equal(4, Person.all().length);
    equal(2, Person.query().limit(2).select().length);
    equal(LAST_NAME, Person.query().limit(2).select('lastName')[0]);
    equal(2, Person.query().equals('lastName', LAST_NAME).
            limit(2).select().length);
    equal(FIRST_NAME_1, Person.query().equals('lastName', LAST_NAME).
            limit(2).select('firstName')[0]);
    equal(SSN_2, Person.query().equals('lastName', LAST_NAME).offset(1).
            select('ssn')[0]);
    equal(2, Person.query().equals('lastName', LAST_NAME).offset(1).
            limit(2).select().length);
    equal(SSN_3, arrays.peek(Person.query().equals('lastName', LAST_NAME).offset(1).
            limit(2).select('ssn')));
    equal(3, Person.query().equals('lastName', LAST_NAME).range(1, 3).
            select().length);
    equal(SSN_4, arrays.peek(Person.query().equals('lastName', LAST_NAME).range(1, 3).
            select('ssn')));
    throws(function () Person.query().offset(-1).select()[0]);
    equal(0, Person.query().offset(4).select().length);
    isUndefined(Person.query().offset(4).select()[0]);
    equal(0, Person.query().range(4, 7).select().length);
    isUndefined(Person.query().range(4, 7).select()[0]);
    equal(1, Person.query().range(3, 7).select().length);
    equal(SSN_4, Person.query().range(3, 7).select('ssn')[0]);
    throws(function () Person.query().offset(-1), Error);
    throws(function () Person.query().range(-2, 3), Error);
}

function createTestPerson() {
    return new Person({firstName: FIRST_NAME_1, lastName: LAST_NAME,
            birthDate: new Date(BIRTH_DATE_MILLIS), birthYear: BIRTH_YEAR,
            ssn: SSN_1, vitae: VITAE, address : null, phoneNumbers : []});
}

function assertPerson() {
    isNotNull(person);
    isTrue(person instanceof Storable &&
            person instanceof Person);
}

if (require.main == module.id) {
    require('test').run(exports);
}
