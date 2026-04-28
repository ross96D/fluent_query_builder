import 'dart:async';
import 'package:postgres/postgres.dart' as pg;

import '../../fluent_query_builder.dart';
import 'query_executor.dart';
import 'package:logging/logging.dart';

class PostgreSqlExecutor extends QueryExecutor<pg.Session> {
  @override
  pg.Session? connection;

  final Logger? logger;
  DBConnectionInfo connectionInfo;
  bool _openAsPool = true;

  PostgreSqlExecutor(this.connectionInfo, {this.logger, this.connection});

  Future<void> reconnect() async {
    await open(usePool: _openAsPool);
  }

  String get schemesString => connectionInfo.schemes!.map((i) => '"$i"').toList().join(', ');

  @override
  Future<void> open({bool usePool = true}) async {
    _openAsPool = usePool;
    final endpoint = pg.Endpoint(
      host: connectionInfo.host,
      port: connectionInfo.port,
      database: connectionInfo.database,
      username: connectionInfo.username,
      password: connectionInfo.password,
    );

    if (usePool) {
      connection = pg.Pool.withEndpoints([endpoint],
          settings: pg.PoolSettings(
            queryTimeout: Duration(seconds: connectionInfo.timeoutInSeconds),
            sslMode: connectionInfo.useSSL == true ? pg.SslMode.require : pg.SslMode.disable,
          ));
    } else {
      connection = await pg.Connection.open(
        endpoint,
        settings: pg.ConnectionSettings(
          queryTimeout: Duration(seconds: connectionInfo.timeoutInSeconds),
          sslMode: connectionInfo.useSSL == true ? pg.SslMode.require : pg.SslMode.disable,
        ),
      );
    }

    if (connectionInfo.enablePsqlAutoSetSearchPath == true &&
        connectionInfo.schemes?.isNotEmpty == true) {
      await query('set search_path to $schemesString;');
    }
  }

  @override
  Future<void> close() async {
    await (connection as pg.SessionExecutor).close();
  }

  @override
  Future<bool> reconnectIfNecessary() async {
    try {
      await connection!.execute('select true');
      return true;
    } catch (e) {
      if ('$e'.contains('Cannot write to socket') ||
          '$e'.contains('database connection closing') ||
          '$e'.contains('connection is not open')) {
        await reconnect();
        return true;
      }
      rethrow;
    }
  }

  @override
  Future<bool> isConnect() async {
    try {
      await connection!.execute('select true');
      return true;
    } catch (e) {
      return false;
    }
  }

  @override
  Future<List<List>> query(String query,
      {Map<String, dynamic>? substitutionValues, List<String?>? returningFields}) async {
    if (returningFields?.isNotEmpty == true) {
      var fields = returningFields!.join(', ');
      var returning = 'RETURNING $fields';
      query = '$query $returning';
    }

    logger?.fine('Query: $query');
    logger?.fine('Values: $substitutionValues');

    List<List> results;

    try {
      pg.Result pgResult;
      if (substitutionValues != null && substitutionValues.isNotEmpty) {
        pgResult = await connection!.execute(
          pg.Sql.named(query),
          parameters: substitutionValues,
          timeout: Duration(seconds: connectionInfo.timeoutInSeconds),
        );
      } else {
        pgResult = await connection!.execute(
          query,
          timeout: Duration(seconds: connectionInfo.timeoutInSeconds),
        );
      }
      results = pgResult.map((row) => row.toList()).toList();
    } catch (e) {
      if (connectionInfo.reconnectIfConnectionIsNotOpen == true &&
              '$e'.contains('connection is not open') ||
          '$e'.contains('database connection closing')) {
        await reconnect();
        pg.Result pgResult;
        if (substitutionValues != null && substitutionValues.isNotEmpty) {
          pgResult = await connection!.execute(
            pg.Sql.named(query),
            parameters: substitutionValues,
          );
        } else {
          pgResult = await connection!.execute(query);
        }
        results = pgResult.map((row) => row.toList()).toList();
      } else {
        rethrow;
      }
    }

    return results;
  }

  @override
  Future<List<Map<String, dynamic>>> getAsMap(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    var rows = await getAsMapWithMeta(query, substitutionValues: substitutionValues);

    final result = <Map<String, dynamic>>[];
    if (rows.isNotEmpty) {
      for (var item in rows) {
        result.add(item['columnMap'] ?? <String, dynamic>{});
      }
    }
    return result;
  }

  @override
  Future<int> execute(String query, {Map<String, dynamic>? substitutionValues}) async {
    logger?.fine('Query: $query');
    logger?.fine('Values: $substitutionValues');

    try {
      pg.Result result;
      if (substitutionValues != null && substitutionValues.isNotEmpty) {
        result = await connection!.execute(
          pg.Sql.named(query),
          parameters: substitutionValues,
          timeout: Duration(seconds: connectionInfo.timeoutInSeconds),
        );
      } else {
        result = await connection!.execute(
          query,
          timeout: Duration(seconds: connectionInfo.timeoutInSeconds),
        );
      }
      return result.affectedRows;
    } catch (e) {
      if (connectionInfo.reconnectIfConnectionIsNotOpen == true &&
              '$e'.contains('connection is not open') ||
          '$e'.contains('database connection closing')) {
        await reconnect();
        pg.Result result;
        if (substitutionValues != null && substitutionValues.isNotEmpty) {
          result = await connection!.execute(
            pg.Sql.named(query),
            parameters: substitutionValues,
          );
        } else {
          result = await connection!.execute(query);
        }
        return result.affectedRows;
      } else {
        rethrow;
      }
    }
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> getAsMapWithMeta(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    logger?.fine('Query: $query');
    logger?.fine('Values: $substitutionValues');

    var results = <Map<String, Map<String, dynamic>>>[];
    try {
      pg.Result pgResult;
      if (substitutionValues != null && substitutionValues.isNotEmpty) {
        pgResult = await connection!.execute(
          pg.Sql.named(query),
          parameters: substitutionValues,
          timeout: Duration(seconds: connectionInfo.timeoutInSeconds),
        );
      } else {
        pgResult = await connection!.execute(
          query,
          timeout: Duration(seconds: connectionInfo.timeoutInSeconds),
        );
      }

      for (var row in pgResult) {
        var rowMap = <String, Map<String, dynamic>>{};
        var columnMap = <String, dynamic>{};
        var tableMap = <String, dynamic>{};

        for (var i = 0; i < row.schema.columns.length; i++) {
          var column = row.schema.columns[i];
          var columnName = column.columnName ?? 'col$i';
          var tableOid = column.tableOid;
          columnMap[columnName] = row[i];
          if (tableOid != null) {
            tableMap['"$tableOid"."$columnName"'] = row[i];
          }
        }
        rowMap['columnMap'] = columnMap;
        rowMap['tableMap'] = tableMap;
        results.add(rowMap);
      }
    } catch (e) {
      if (connectionInfo.reconnectIfConnectionIsNotOpen == true &&
              '$e'.contains('connection is not open') ||
          '$e'.contains('database connection closing')) {
        await reconnect();
        pg.Result pgResult;
        if (substitutionValues != null && substitutionValues.isNotEmpty) {
          pgResult = await connection!.execute(
            pg.Sql.named(query),
            parameters: substitutionValues,
          );
        } else {
          pgResult = await connection!.execute(query);
        }

        for (var row in pgResult) {
          var rowMap = <String, Map<String, dynamic>>{};
          var columnMap = <String, dynamic>{};
          var tableMap = <String, dynamic>{};

          for (var i = 0; i < row.schema.columns.length; i++) {
            var column = row.schema.columns[i];
            var columnName = column.columnName ?? 'col$i';
            var tableOid = column.tableOid;
            columnMap[columnName] = row[i];
            if (tableOid != null) {
              tableMap['"$tableOid"."$columnName"'] = row[i];
            }
          }
          rowMap['columnMap'] = columnMap;
          rowMap['tableMap'] = tableMap;
          results.add(rowMap);
        }
      } else {
        rethrow;
      }
    }
    return results;
  }

  Future<dynamic> simpleTransaction(Future<dynamic> Function(QueryExecutor) f) async {
    logger?.fine('Entering simpleTransaction');
    if (connection == null) {
      return await f(this);
    }

    var returnValue;
    await (connection as pg.SessionExecutor).runTx((ctx) async {
      try {
        logger?.fine('Entering transaction');
        var tx = PostgreSqlExecutor(connectionInfo, logger: logger, connection: ctx);
        returnValue = await f(tx);
      } catch (e) {
        rethrow;
      } finally {
        logger?.fine('Exiting transaction');
      }
    });
    return returnValue;
  }

  @override
  Future<QueryExecutor> startTransaction() async {
    await connection!.execute('begin');
    return this;
  }

  @override
  Future<void> commit() async {
    await connection!
        .execute('commit', timeout: Duration(seconds: connectionInfo.timeoutInSeconds));
  }

  @override
  Future<void> rollback() async {}

  @override
  Future<T?> transaction<T>(FutureOr<T> Function(QueryExecutor) f) async {
    if (connection == null) return f(this);

    T? returnValue;
    await (connection as pg.SessionExecutor).runTx((ctx) async {
      try {
        logger?.fine('Entering transaction');
        var tx = PostgreSqlExecutor(connectionInfo, logger: logger, connection: ctx);
        returnValue = await f(tx);
      } catch (e) {
        rethrow;
      } finally {
        logger?.fine('Exiting transaction');
      }
    });
    return returnValue;
  }

  @override
  Future<dynamic> transaction2(Future<dynamic> Function(QueryExecutor) queryBlock,
      {int? commitTimeoutInSeconds}) async {
    var re = await (connection as pg.SessionExecutor).runTx((ctx) async {
      var tx = PostgreSqlExecutor(connectionInfo, logger: logger, connection: ctx);
      await queryBlock(tx);
    });
    return re;
  }
}
