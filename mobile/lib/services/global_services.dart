// lib/services/global_services.dart
import 'package:pinepods_mobile/services/nowplaying/nowplaying_service.dart';
import 'package:pinepods_mobile/services/offline/offline_action_queue.dart';
import 'package:pinepods_mobile/services/pinepods/pinepods_audio_service.dart';
import 'package:pinepods_mobile/services/pinepods/pinepods_service.dart';

/// Global service access point for the app
class GlobalServices {
  static PinepodsAudioService? _pinepodsAudioService;
  static PinepodsService? _pinepodsService;
  static OfflineActionQueue? _offlineActionQueue;
  static NowPlayingService? _nowPlayingService;

  /// Set the global services (called from PinepodsPodcastApp)
  static void initialize({
    required PinepodsAudioService pinepodsAudioService,
    required PinepodsService pinepodsService,
    OfflineActionQueue? offlineActionQueue,
    NowPlayingService? nowPlayingService,
  }) {
    _pinepodsAudioService = pinepodsAudioService;
    _pinepodsService = pinepodsService;
    _offlineActionQueue = offlineActionQueue;
    _nowPlayingService = nowPlayingService;
  }

  /// Get the global now-playing (cross-device awareness) service instance
  static NowPlayingService? get nowPlayingService => _nowPlayingService;

  /// Get the global offline action queue instance
  static OfflineActionQueue? get offlineActionQueue => _offlineActionQueue;
  
  /// Update global service credentials (called when user logs in or settings change)
  static void setCredentials(String server, String apiKey) {
    _pinepodsService?.setCredentials(server, apiKey);
  }
  
  /// Get the global PinepodsAudioService instance
  static PinepodsAudioService? get pinepodsAudioService => _pinepodsAudioService;
  
  /// Get the global PinepodsService instance
  static PinepodsService? get pinepodsService => _pinepodsService;
  
  /// Clear services (for testing or cleanup)
  static void clear() {
    _pinepodsAudioService = null;
    _pinepodsService = null;
    _offlineActionQueue = null;
  }
}