class ValidationException implements Exception {
  final String msg;
  const ValidationException(this.msg);
  @override String toString() => 'ValidationException: $msg';
}