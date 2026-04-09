import 'dart:async';

import 'package:postgres/postgres.dart';

void main() async {
  var db = await Connection.open(
    Endpoint(
      host: '192.168.133.13',
      port: 5432,
      database: 'test',
      username: 'sisadmin',
      password: 's1sadm1n',
    ),
  );
  await db.execute('DROP TABLE IF EXISTS "public"."naturalPerson" CASCADE');
  await db.execute('DROP TABLE IF EXISTS "public"."legalPerson" CASCADE');
  await db.execute('''
CREATE TABLE IF NOT EXISTS "public"."naturalPerson" (
  "id" serial8 PRIMARY KEY,
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "email" varchar(255) COLLATE "pg_catalog"."default" 
);
''');
  await db.execute('''
CREATE TABLE IF NOT EXISTS "public"."legalPerson" (
  "idPerson" int8 PRIMARY KEY,
  "socialSecurityNumber" varchar(12) COLLATE "pg_catalog"."default",
   CONSTRAINT "ssn" UNIQUE ("socialSecurityNumber")
);
''');
  await db.execute('''
INSERT INTO "naturalPerson" (name,email) VALUES ('John Doe', 'johndoe@gmail.com');
''');
  await db.execute('''
INSERT INTO "legalPerson" ("idPerson","socialSecurityNumber") VALUES ('1', '856-45-6789');
''');
  var repository = NaturalPersonRepository();

  Timer.periodic(Duration(seconds: 3), (t) async {
    var idPerson;
    try {
      await db.runTx((ctx) async {
        idPerson = await repository.insert(
            NaturalPerson(name: 'John Doe 2', email: 'johndoe2@gmail.com'),
            ctx);

        await ctx.execute(
            '''INSERT INTO "legalPerson" ("idPerson","socialSecurityNumber") VALUES ('$idPerson', '956-45-6789');''');
      });
    } catch (e) {
      print(e);
    }
  });

  //exit(0);
}

class NaturalPerson {
  final String name;
  final String email;

  NaturalPerson({required this.name, required this.email});

  Map<String, dynamic> toMap() {
    return {
      'name': name,
      'email': email,
    };
  }

  factory NaturalPerson.fromMap(Map<String, dynamic> map) {
    return NaturalPerson(
      name: map['name'] ?? '',
      email: map['email'] ?? '',
    );
  }
}

class LegalPerson extends NaturalPerson {
  final int idPerson;
  final String socialSecurityNumber;

  LegalPerson(
      {required String name,
      required String email,
      required this.idPerson,
      required this.socialSecurityNumber})
      : super(name: name, email: email);
}

class NaturalPersonRepository {
  Future<int> insert(NaturalPerson person, Session ctx) async {
    var result = await ctx.execute(
        '''INSERT INTO "naturalPerson" (name,email) VALUES ('${person.name}', '${person.email}') RETURNING id;''');
    var idPerson = result.first.first;
    return idPerson as int;
  }
}

bool isValidSSN(String? str) {
  var regex =
      '^(?!666|000|9\\d{2})\\d{3}' + '-(?!00)\\d{2}-' + '(?!0{4})\\d{4}\$';
  var p = RegExp(regex);

  if (str == null) {
    return false;
  }

  return p.hasMatch(str);
}
