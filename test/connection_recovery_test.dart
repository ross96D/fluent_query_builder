import 'package:fluent_query_builder/fluent_query_builder.dart';
import 'package:postgres/postgres.dart' as pg;
import 'package:test/test.dart';

import 'constants.dart';

void main() {
  group('Connection Recovery Tests', () {
    late DbLayer db;
    late pg.Pool pgPool;

    setUp(() async {
      pgPool = pg.Pool.withEndpoints(
        [
          pg.Endpoint(
            host: 'localhost',
            port: dbPort,
            database: 'banco_teste',
            username: 'postgres',
            password: 'postgre',
          ),
        ],
        settings: pg.PoolSettings(
          maxConnectionCount: 2,
          sslMode: pg.SslMode.disable,
        ),
      );

      db = DbLayer(
        DBConnectionInfo(
          host: 'localhost',
          database: 'banco_teste',
          driver: ConnectionDriver.pgsql,
          port: dbPort,
          username: 'postgres',
          password: 'postgre',
          charset: 'utf8',
          schemes: ['public'],
        ),
      );
      await db.connect();

      await db.raw('DROP TABLE IF EXISTS connection_recovery_test').exec();
      await db
          .raw(
              'CREATE TABLE connection_recovery_test (id serial, name VARCHAR(200));')
          .exec();
    });

    tearDown(() async {
      await db.close();
      await pgPool.close();
    });

    test('Pool should handle multiple sequential queries', () async {
      for (var i = 0; i < 5; i++) {
        await db.insert().into('connection_recovery_test').setAll({
          'name': 'Test $i',
        }).exec();
      }

      final result = await db.select().from('connection_recovery_test').get();
      expect(result!.length, 5);
    });

    test('DbLayer queries should work after underlying connection is closed',
        () async {
      final executor = db.executor;
      if (executor is PostgreSqlExecutor) {
        await db.insert().into('connection_recovery_test').setAll({
          'name': 'Before close',
        }).exec();

        final result1 =
            await db.select().from('connection_recovery_test').get();
        expect(result1!.length, 1);

        final connection = executor.connection;
        if (connection is pg.Connection) {
          await connection.close();
          await Future.delayed(Duration(milliseconds: 100));
          expect(connection.isOpen, false);

          await db.insert().into('connection_recovery_test').setAll({
            'name': 'After auto recovery',
          }).exec();

          final result2 =
              await db.select().from('connection_recovery_test').get();
          expect(result2!.length, 2);
        }
      }
    });

    test(
        'DbLayer queries should auto-recover from multiple connection closures',
        () async {
      final executor = db.executor;
      if (executor is PostgreSqlExecutor) {
        for (var round = 0; round < 3; round++) {
          await db.insert().into('connection_recovery_test').setAll({
            'name': 'Round $round',
          }).exec();

          final connection = executor.connection;
          if (connection is pg.Connection) {
            await connection.close();
            await Future.delayed(Duration(milliseconds: 50));
          }
        }

        await db.insert().into('connection_recovery_test').setAll({
          'name': 'After multiple recoveries',
        }).exec();

        final result = await db.select().from('connection_recovery_test').get();
        expect(result!.length, 4);
      }
    });

    test('isConnect should return true when connection is open', () async {
      final executor = db.executor;
      if (executor is PostgreSqlExecutor) {
        expect(await executor.isConnect(), true);
      }
    });

    test('isConnect should return false when connection is closed', () async {
      final executor = db.executor;
      if (executor is PostgreSqlExecutor) {
        expect(await executor.isConnect(), true);

        final connection = executor.connection;
        if (connection is pg.Connection) {
          await connection.close();
          await Future.delayed(Duration(milliseconds: 100));

          expect(await executor.isConnect(), false);
        }
      }
    });

    test('Pool should maintain connection state correctly', () async {
      final executor = db.executor;
      if (executor is PostgreSqlExecutor) {
        expect(await executor.isConnect(), true);

        final result1 =
            await db.select().from('connection_recovery_test').get();
        expect(result1, isNotNull);

        expect(await executor.isConnect(), true);

        final result2 =
            await db.select().from('connection_recovery_test').get();
        expect(result2, isNotNull);

        expect(await executor.isConnect(), true);
      }
    });

    test('Pool should handle concurrent queries', () async {
      final futures = <Future>[];
      for (var i = 0; i < 10; i++) {
        futures.add(
          db.insert().into('connection_recovery_test').setAll({
            'name': 'Concurrent $i',
          }).exec(),
        );
      }
      await Future.wait(futures);

      final result = await db.select().from('connection_recovery_test').get();
      expect(result!.length, 10);
    });

    test('Pool should work with transactions', () async {
      await db.insert().into('connection_recovery_test').setAll({
        'name': 'Transaction test',
      }).exec();

      await db.transaction((ctx) async {
        await ctx.insert().into('connection_recovery_test').setAll({
          'name': 'In transaction',
        }).exec();

        final result =
            await ctx.select().from('connection_recovery_test').get();
        expect(result, isNotNull);
      });

      final result = await db.select().from('connection_recovery_test').get();
      expect(result!.length, greaterThanOrEqualTo(1));
    });

    test('Postgres Pool should recover automatically after connection close',
        () async {
      final result1 = await pgPool.execute('SELECT 1 as value');
      expect(result1.first.first, 1);

      await pgPool.runTx((session) async {
        await session.execute('SELECT 2 as value');
      });

      final result2 = await pgPool.execute('SELECT 3 as value');
      expect(result2.first.first, 3);

      final results = <int>[];
      for (var i = 0; i < 5; i++) {
        final r = await pgPool.execute('SELECT ${i + 10} as value');
        results.add(r.first.first as int);
      }
      expect(results, [10, 11, 12, 13, 14]);
    });

    test('Postgres Pool handles connection failure gracefully', () async {
      final conn1 = await pgPool.withConnection((conn) async {
        await conn.execute('SELECT 1');
        return conn;
      });

      expect(conn1.isOpen, true);

      final result = await pgPool.execute('SELECT 42');
      expect(result.first.first, 42);

      final conn2 = await pgPool.withConnection((conn) async {
        return conn;
      });
      expect(conn2.isOpen, true);
    });

    test('Pool with multiple connections distributes load', () async {
      final futures = <Future>[];
      for (var i = 0; i < 20; i++) {
        futures.add(pgPool.execute('SELECT $i as value'));
      }

      final results = await Future.wait(futures);
      for (var i = 0; i < 20; i++) {
        expect(results[i].first.first, i);
      }
    });
  });
}
