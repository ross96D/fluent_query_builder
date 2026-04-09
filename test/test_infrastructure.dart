import 'package:fluent_query_builder/fluent_query_builder.dart';
import 'package:postgres/postgres.dart' as pg;
import 'package:test/test.dart';

import 'constants.dart';

DbLayer get db => _db;
late DbLayer _db;

var connectionInfo = DBConnectionInfo(
    host: 'localhost',
    database: 'banco_teste',
    driver: ConnectionDriver.pgsql,
    port: dbPort,
    username: 'postgres',
    password: 'postgre',
    charset: 'utf8',
    schemes: ['public']);

Future<void> _ensureDatabaseExists() async {
  final adminConn = await pg.Connection.open(
    pg.Endpoint(
      host: connectionInfo.host,
      port: connectionInfo.port,
      database: 'postgres',
      username: connectionInfo.username,
      password: connectionInfo.password,
    ),
    settings: pg.ConnectionSettings(sslMode: pg.SslMode.disable),
  );

  try {
    await adminConn
        .execute(
      "SELECT 1 FROM pg_database WHERE datname = '${connectionInfo.database}'",
    )
        .then((result) async {
      if (result.isEmpty) {
        await adminConn.execute(
          'CREATE DATABASE ${connectionInfo.database} WITH OWNER "${connectionInfo.username}" TEMPLATE=template0 ENCODING \'UTF8\'',
        );
      }
    });
  } catch (e) {
    print('CREATE DATABASE check/create: $e');
  } finally {
    await adminConn.close();
  }
}

void initializeTest() {
  setUp(() async {
    await _ensureDatabaseExists();

    _db = DbLayer(connectionInfo);
    await _db.connect();

    await _db.raw('DROP TABLE IF EXISTS pessoas').exec();
    await _db
        .raw(
            'CREATE TABLE pessoas (id serial, nome VARCHAR(200),telefone VARCHAR(200),cep VARCHAR(200));')
        .exec();
    await _db.insert().into('pessoas').setAll(
        {'nome': 'Isaque', 'telefone': '99701-5305', 'cep': '54654'}).exec();
  });

  tearDown(() async {
    await _db.close();
  });
}
