// lib/services/network/connectivity_service.dart
import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/foundation.dart';
import 'package:logging/logging.dart';
import 'package:pinepods_mobile/services/pinepods/login_service.dart';

/// App-wide online/offline state used to drive the offline experience (#935):
/// falling back to the local Downloads library and playing on-device episodes
/// when the server is completely unreachable (airplane mode, or a LAN/VPN-only
/// self-hosted server with no connectivity).
///
/// "Offline" here means the *server* is unreachable, which for a private server
/// is the common case when there is no internet. We therefore combine the
/// device link state ([isOnline], from `connectivity_plus`) with a fast probe
/// of the configured server ([serverReachable]).
///
/// This is a [ChangeNotifier] singleton so widgets can rebuild on transitions
/// (e.g. an offline-aware Home that recovers when the server comes back).
class ConnectivityService extends ChangeNotifier {
  ConnectivityService._();

  static final ConnectivityService instance = ConnectivityService._();

  static final _log = Logger('ConnectivityService');

  /// The reachability probe is deliberately short so the offline UI appears
  /// promptly instead of waiting out the 15-20s timeouts on the normal API
  /// calls. [PinepodsLoginService.checkServer] has its own longer internal timeout, so
  /// we wrap it to cap the wait.
  static const Duration _probeTimeout = Duration(seconds: 3);

  bool _isOnline = true;
  bool _serverReachable = true;
  bool _subscribed = false;
  String? Function()? _serverUrlProvider;
  StreamSubscription<List<ConnectivityResult>>? _sub;

  /// True when the device reports at least one active network interface.
  bool get isOnline => _isOnline;

  /// True when the configured server answered the reachability probe.
  bool get serverReachable => _serverReachable;

  /// True when we should behave as offline: no network link, or the server did
  /// not answer the probe. This is the flag the UI should consult.
  bool get isOffline => !_isOnline || !_serverReachable;

  /// Wire up the service. [serverUrlProvider] returns the current server URL
  /// (e.g. `settingsBloc.currentSettings.pinepodsServer`) so the probe always
  /// targets the live configuration. Safe to call more than once; the
  /// connectivity subscription is only created on the first call. Returns the
  /// first reachability probe so callers can await the initial state.
  Future<void> init(String? Function() serverUrlProvider) {
    _serverUrlProvider = serverUrlProvider;
    if (!_subscribed) {
      _subscribed = true;
      _sub = Connectivity().onConnectivityChanged.listen(
        (_) => refresh(),
        onError: (Object e) => _log.fine('connectivity stream error: $e'),
      );
    }
    return refresh();
  }

  /// Re-evaluate connectivity + server reachability and notify listeners on any
  /// change. Never throws.
  Future<void> refresh() async {
    bool online;
    try {
      final results = await Connectivity().checkConnectivity();
      online = results.any((r) => r != ConnectivityResult.none);
    } catch (_) {
      // If we cannot determine the link, assume online and let the server probe
      // decide — this avoids falsely forcing offline mode.
      online = true;
    }

    var reachable = false;
    if (online) {
      final url = _serverUrlProvider?.call();
      if (url != null && url.isNotEmpty) {
        try {
          final result =
              await PinepodsLoginService.checkServer(url).timeout(_probeTimeout);
          reachable = result.isPinepods;
        } catch (e) {
          _log.fine('server reachability probe failed: $e');
          reachable = false;
        }
      }
    }

    if (online != _isOnline || reachable != _serverReachable) {
      _isOnline = online;
      _serverReachable = reachable;
      _log.info('connectivity changed: online=$online reachable=$reachable');
      notifyListeners();
    }
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }
}
