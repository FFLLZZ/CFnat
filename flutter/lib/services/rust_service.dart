import 'dart:async';
import 'package:flutter/material.dart';
import '../bridge_generated.dart/lib.dart' as rust;
import 'app_service.dart';

export '../bridge_generated.dart/frb_generated.dart' show RustLib;

/// 桥接 startService 的签名别名，供构造注入使用
typedef RustStartCall = Future<rust.ActionResult> Function({
  String? ipFile,
  List<String>? ipContent,
  String? http,
  BigInt? delayLimit,
  double? tlr,
  int? ips,
  int? threads,
  int? tlsPort,
  int? httpPort,
  int? maxStickySlots,
  String? addr,
  List<String>? colo,
});

class RustService extends AppService {
  StatusData? _status;
  ConfigData? _config;
  bool _initialized = false;
  String? _error;
  Timer? _pollTimer;

  /// 桥接 API 是静态生成的、无法替换，因此按 ApiService 的注入约定
  /// 暴露这些入口：生产用默认实现，测试可注入受控 Future
  final Future<rust.StatusInfo> Function() _statusFetcher;
  final Future<rust.ConfigInfo> Function() _configFetcher;
  final RustStartCall _startCall;
  final Future<rust.ActionResult> Function() _stopCall;

  RustService({
    Future<rust.StatusInfo> Function()? statusFetcher,
    Future<rust.ConfigInfo> Function()? configFetcher,
    RustStartCall? startCall,
    Future<rust.ActionResult> Function()? stopCall,
  })  : _statusFetcher = statusFetcher ?? rust.getStatus,
        _configFetcher = configFetcher ?? rust.getConfig,
        _startCall = startCall ?? rust.startService,
        _stopCall = stopCall ?? rust.stopService;

  @override
  StatusData? get status => _status;
  @override
  ConfigData? get config => _config;
  bool get initialized => _initialized;
  @override
  String? get lastError => _error;
  @override
  bool get connected => _initialized;
  @override
  bool get isRunning => _status?.running ?? false;

  Future<void> initialize() async {
    try {
      await fetchConfig();
      if (isDisposed) return;
      _initialized = true;
      _startPolling();
      notifyIfAlive();
    } catch (e) {
      _error = e.toString();
      debugPrint('Rust init error: $_error');
      notifyIfAlive();
    }
  }

  void _startPolling() {
    _pollTimer?.cancel();
    _pollTimer = Timer.periodic(const Duration(seconds: 1), (_) async {
      if (_initialized && !isDisposed) {
        await fetchStatus();
      }
    });
  }

  @override
  void dispose() {
    markDisposed();
    _pollTimer?.cancel();
    super.dispose();
  }

  @override
  Future<void> fetchStatus() async {
    if (isDisposed) return;
    try {
      final result = await _statusFetcher();
      if (isDisposed) return;
      _status = StatusData(
        running: result.running,
        uptimeSecs: result.uptimeSecs.toInt(),
        primaryCount: result.primaryCount,
        primaryTarget: result.primaryTarget,
        backupCount: result.backupCount,
        backupTarget: result.backupTarget,
        stickyIps: result.stickyIps,
        primaryIps: result.primaryIps.map((e) => IpInfo(
          ip: e.ip,
          colo: e.colo,
          delay: e.delay,
          loss: e.loss,
          samples: e.samples,
        )).toList(),
        backupIps: result.backupIps.map((e) => IpInfo(
          ip: e.ip,
          colo: e.colo,
          delay: e.delay,
          loss: e.loss,
          samples: e.samples,
        )).toList(),
      );
      notifyIfAlive();
    } catch (e) {
      debugPrint('获取状态失败: $e');
    }
  }

  @override
  Future<void> fetchConfig() async {
    if (isDisposed) return;
    try {
      final result = await _configFetcher();
      if (isDisposed) return;
      _config = ConfigData(
        addr: result.addr,
        delayLimit: result.delayLimit.toInt(),
        tlr: result.tlr,
        ips: result.ips,
        threads: result.threads,
        tlsPort: result.tlsPort,
        httpPort: result.httpPort,
        http: result.http,
        ipFile: result.ipFile,
        maxStickySlots: result.maxStickySlots,
        colo: result.colo,
      );
      notifyIfAlive();
    } catch (e) {
      debugPrint('获取配置失败: $e');
    }
  }

  @override
  Future<bool> startService({
    String? ipFile,
    List<String>? ipContent,
    String? http,
    int? delayLimit,
    double? tlr,
    int? ips,
    int? threads,
    int? tlsPort,
    int? httpPort,
    List<String>? colo,
    String? addr,
    int? maxStickySlots,
  }) async {
    // 每次动作先清空上一次的原因，避免把旧错误当成本次结果展示
    _error = null;
    try {
      final result = await _startCall(
        ipFile: ipFile,
        ipContent: ipContent,
        http: http,
        delayLimit: delayLimit != null ? BigInt.from(delayLimit) : null,
        tlr: tlr,
        ips: ips,
        threads: threads,
        tlsPort: tlsPort,
        httpPort: httpPort,
        maxStickySlots: maxStickySlots,
        addr: addr,
        colo: colo,
      );
      if (result.success) {
        await fetchConfig();
      } else {
        _error = result.message;
        debugPrint('启动服务失败[${result.category}]: ${result.message}');
      }
      return result.success;
    } catch (e) {
      _error = e.toString();
      debugPrint('启动服务失败: $e');
      return false;
    }
  }

  @override
  Future<bool> stopService() async {
    _error = null;
    try {
      final result = await _stopCall();
      if (result.success) {
        _status = StatusData.stopped();
        notifyIfAlive();
      } else {
        _error = result.message;
        debugPrint('停止服务失败[${result.category}]: ${result.message}');
      }
      return result.success;
    } catch (e) {
      _error = e.toString();
      debugPrint('停止服务失败: $e');
      return false;
    }
  }

  @override
  Future<List<LogEntry>> fetchLogs() async {
    try {
      final logs = await rust.getLogs();
      return logs.map((e) => LogEntry(
        timestamp: e.timestamp,
        level: e.level,
        message: e.message,
      )).toList();
    } catch (e) {
      debugPrint('获取日志失败: $e');
      return [];
    }
  }

  @override
  Future<bool> clearLogs() async {
    try {
      await rust.clearLogs();
      return true;
    } catch (e) {
      debugPrint('清空日志失败: $e');
      return false;
    }
  }
}