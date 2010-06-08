// Run w/, e.g.: $ ringo test/all

var log = require('ringo/logging').getLogger(module.id);

include('ringo/unittest');
var {Store} = require('ringo/storage/mongodbstore');
var store = new Store('localhost', 27017, "rist");

var personId, person;
var Person = store.defineEntity('Person');
var Address = store.defineEntity('Address');
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
    assertEqual(FIRST_NAME_1, person.firstName);
    assertEqual(LAST_NAME, person.lastName);
    assertEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    assertEqual(BIRTH_YEAR, person.birthYear);
    assertEqual(VITAE, person.vitae);
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
    assertEqual(FIRST_NAME_2, person.firstName);
    assertEqual(LAST_NAME, person.lastName);
    assertEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    assertEqual(BIRTH_YEAR, person.birthYear);
    assertEqual(VITAE, person.vitae);
};

exports.testPersistDeletion = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    personId = person._id;
    person.remove();
    person = Person.get(personId);
    assertNull(person);
    assertEqual(0, Person.all().length);
};

exports.testPersistOneToOneRelations = function () {
    person = createTestPerson();
    person.save();
    var address = new Address();
    address.city = "Vienna";
    address.street = "Börsegasse 11";
    person.address = address;
    person.save();
    assertEqual(Address.all()[0], Person.all()[0].address);
};

exports.testPersistRelations = function () {
    person = createTestPerson();
    person.save();
    var addressA = new Address();
    addressA.city = "Vienna";
    addressA.street = "Börsegasse";
    var addressB = new Address();
    addressB.city = "Vienna";
    addressB.street = "Marxergasse";
    person.addresses = [addressA, addressB];
    person.save();
    var personsAddresses = Person.all()[0].addresses;
    assertEqual(2, personsAddresses.length);
    assertTrue(personsAddresses[0] != personsAddresses[1] && personsAddresses[0] == addressA || personsAddresses[0] == addressB && personsAddresses[1] == addressA || personsAddresses[1] == addressB)
};

exports.testMapReduce = function () {
    var personA = createTestPerson();
    personA.save();
    var personB = createTestPerson();
    personB.save();
    var personC = createTestPerson();
    personC.firstName = FIRST_NAME_2;
    personC.save();
    var mappedAndReducedByFirstName = Person.map(function(){emit(this.firstName, 1);}).reduce(function(k, values){var sum = 0; for(var v in values){sum += values[v];} return sum;}).select();
    assertEqual(2, mappedAndReducedByFirstName.Hans);
    assertEqual(1, mappedAndReducedByFirstName.Herbert);
};

exports.testBasicQuerying = function () {
    person = createTestPerson();
    person.save();
    person = createTestPerson();
    person.firstName = FIRST_NAME_2;
    person.ssn = SSN_2;
    person.save();
    assertTrue(Person.all()[0] instanceof Storable &&
            Person.all()[0] instanceof Person);
    assertEqual(2, Person.all().length);
    assertEqual(LAST_NAME, Person.all()[0].lastName);
    var testQuery = Person.query().equals('lastName', LAST_NAME);
    assertEqual(2, testQuery.select().length);
    var queriedPerson = Person.query().equals('firstName', FIRST_NAME_1).
            select()[0];
    assertTrue(queriedPerson instanceof Storable &&
            queriedPerson instanceof Person);
    assertEqual(1, Person.query().equals('firstName', FIRST_NAME_1).select().
            length);
    assertEqual(FIRST_NAME_1, Person.query().equals('firstName', FIRST_NAME_1).
            select('firstName')[0]);
    assertEqual(2, Person.query().equals('lastName', LAST_NAME).select().
            length);
    assertEqual(SSN_2, Person.query().equals('lastName', LAST_NAME).
            equals('firstName', FIRST_NAME_2).select('ssn')[0]);
    testGreaterLessQuerying();
    testOrderByQuerying();
    testSliceQuerying();
};

function testGreaterLessQuerying() {
    assertEqual(2, Person.query().greater('birthYear', BIRTH_YEAR - 1).select().
            length);
    assertEqual(0, Person.query().greater('birthYear', BIRTH_YEAR + 1).select().
            length);
    assertEqual(2, Person.query().less('birthYear', BIRTH_YEAR + 1).select().
            length);
    assertEqual(0, Person.query().less('birthYear', BIRTH_YEAR - 1).select().
            length);
    assertEqual(2, Person.query().greaterEquals('birthYear', BIRTH_YEAR).
            select().length);
    assertEqual(2, Person.query().greaterEquals('birthYear', BIRTH_YEAR - 1).
            select().length);
    assertEqual(0, Person.query().greaterEquals('birthYear', BIRTH_YEAR + 1).
            select().length);
    assertEqual(2, Person.query().lessEquals('birthYear', BIRTH_YEAR).select().
            length);
    assertEqual(2, Person.query().lessEquals('birthYear', BIRTH_YEAR + 1).
            select().length);
    assertEqual(0, Person.query().lessEquals('birthYear', BIRTH_YEAR - 1).
            select().length);
    assertEqual(2, Person.query().greater('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    assertEqual(0, Person.query().greater('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    assertEqual(2, Person.query().less('birthDate', new Date(BIRTH_DATE_MILLIS +
            1000)).select().length);
    assertEqual(0, Person.query().less('birthDate', new Date(BIRTH_DATE_MILLIS)
            ).select().length);
    assertEqual(2, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    assertEqual(2, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    assertEqual(0, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS + 1000)).select().length);
    assertEqual(2, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    assertEqual(2, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS + 1000)).select().length);
    assertEqual(0, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    assertEqual(LAST_NAME, Person.query().equals('lastName', LAST_NAME).
            greater('birthDate', new Date(BIRTH_DATE_MILLIS - 1000)).
            less('birthYear', BIRTH_YEAR + 1).select('lastName')[0]);
}

function testOrderByQuerying() {
    assertEqual(2, Person.query().orderBy('ssn').select().length);
    assertEqual(SSN_1, Person.query().orderBy('ssn').select('ssn')[0]);
    assertEqual(2, Person.query().orderBy('-ssn').select().length);
    assertEqual(SSN_2, Person.query().orderBy('-ssn').select('ssn')[0]);
    assertEqual(2, Person.query().equals('lastName', LAST_NAME).
            orderBy('firstName').select().length);
    assertEqual(FIRST_NAME_1, Person.query().equals('lastName', LAST_NAME).
            orderBy('firstName').select('firstName')[0]);
    assertEqual(FIRST_NAME_2, Person.query().equals('lastName', LAST_NAME).
            orderBy('firstName').select('firstName')[1]);
    assertEqual(2, Person.query().equals('lastName', LAST_NAME).
            orderBy('-firstName').select().length);
    assertEqual(FIRST_NAME_2, Person.query().equals('lastName', LAST_NAME).
            orderBy('-firstName').select('firstName')[0]);
    assertEqual(FIRST_NAME_1, Person.query().equals('lastName', LAST_NAME).
            orderBy('-firstName').select('firstName')[1]);
}

function testSliceQuerying() {
    person = createTestPerson();
    person.ssn = SSN_3;
    person.save();
    person = createTestPerson();
    person.ssn = SSN_4;
    person.save();
    assertEqual(4, Person.all().length);
    assertEqual(2, Person.query().limit(2).select().length);
    assertEqual(LAST_NAME, Person.query().limit(2).select('lastName')[0]);
    assertEqual(2, Person.query().equals('lastName', LAST_NAME).
            limit(2).select().length);
    assertEqual(FIRST_NAME_1, Person.query().equals('lastName', LAST_NAME).
            limit(2).select('firstName')[0]);
    assertEqual(SSN_2, Person.query().equals('lastName', LAST_NAME).offset(1).
            select('ssn')[0]);
    assertEqual(2, Person.query().equals('lastName', LAST_NAME).offset(1).
            limit(2).select().length);
    assertEqual(SSN_3, Person.query().equals('lastName', LAST_NAME).offset(1).
            limit(2).select('ssn').peek());
    assertEqual(3, Person.query().equals('lastName', LAST_NAME).range(1, 3).
            select().length);
    assertEqual(SSN_4, Person.query().equals('lastName', LAST_NAME).range(1, 3).
            select('ssn').peek());
    assertThrows(function () Person.query().offset(-1).select()[0]);
    assertEqual(0, Person.query().offset(4).select().length);
    assertUndefined(Person.query().offset(4).select()[0]);
    assertEqual(0, Person.query().range(4, 7).select().length);
    assertUndefined(Person.query().range(4, 7).select()[0]);
    assertEqual(1, Person.query().range(3, 7).select().length);
    assertEqual(SSN_4, Person.query().range(3, 7).select('ssn')[0]);
    assertThrows(function () Person.query().offset(-1), Error);
    assertThrows(function () Person.query().range(-2, 3), Error);
}

function createTestPerson() {
    return new Person({firstName: FIRST_NAME_1, lastName: LAST_NAME,
            birthDate: new Date(BIRTH_DATE_MILLIS), birthYear: BIRTH_YEAR,
            ssn: SSN_1, vitae: VITAE});
}

function assertPerson() {
    assertNotNull(person);
    assertTrue(person instanceof Storable &&
            person instanceof Person);
}

if (require.main == module.id) {
    require('ringo/unittest').run(exports);
}
