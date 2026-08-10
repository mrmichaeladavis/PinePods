// lib/ui/widgets/lazy_network_image.dart
import 'dart:io';

import 'package:flutter/material.dart';

class LazyNetworkImage extends StatefulWidget {
  final String imageUrl;

  /// Absolute path to a locally-cached copy of the image. When set and the file
  /// exists on disk, it is rendered instead of [imageUrl] so artwork shows with
  /// no network (offline, #935). Falls back to [imageUrl] if the file is
  /// missing.
  final String? localImagePath;
  final double width;
  final double height;
  final BoxFit fit;
  final Widget? placeholder;
  final Widget? errorWidget;
  final BorderRadius? borderRadius;

  const LazyNetworkImage({
    super.key,
    required this.imageUrl,
    this.localImagePath,
    required this.width,
    required this.height,
    this.fit = BoxFit.cover,
    this.placeholder,
    this.errorWidget,
    this.borderRadius,
  });

  @override
  State<LazyNetworkImage> createState() => _LazyNetworkImageState();
}

class _LazyNetworkImageState extends State<LazyNetworkImage> {
  bool _shouldLoad = false;
  
  Widget get _defaultPlaceholder => Container(
    width: widget.width,
    height: widget.height,
    decoration: BoxDecoration(
      color: Colors.grey[200],
      borderRadius: widget.borderRadius,
    ),
    child: const Icon(
      Icons.music_note,
      color: Colors.grey,
      size: 24,
    ),
  );

  Widget get _defaultErrorWidget => Container(
    width: widget.width,
    height: widget.height,
    decoration: BoxDecoration(
      color: Colors.grey[300],
      borderRadius: widget.borderRadius,
    ),
    child: const Icon(
      Icons.broken_image,
      color: Colors.grey,
      size: 24,
    ),
  );

  @override
  void initState() {
    super.initState();
    // Delay loading slightly to allow the widget to be built first
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (mounted) {
        setState(() {
          _shouldLoad = true;
        });
      }
    });
  }

  /// The locally-cached file to render, or null when there isn't one on disk.
  File? get _localFile {
    final path = widget.localImagePath;
    if (path == null || path.isEmpty) return null;
    final file = File(path);
    return file.existsSync() ? file : null;
  }

  @override
  Widget build(BuildContext context) {
    final localFile = _localFile;
    if (localFile != null) {
      // A cached copy exists — render it directly (no network, no lazy gate).
      return ClipRRect(
        borderRadius: widget.borderRadius ?? BorderRadius.zero,
        child: Image.file(
          localFile,
          width: widget.width,
          height: widget.height,
          fit: widget.fit,
          cacheWidth: (widget.width * 2).round(),
          cacheHeight: (widget.height * 2).round(),
          errorBuilder: (context, error, stackTrace) {
            return widget.errorWidget ?? _defaultErrorWidget;
          },
        ),
      );
    }

    return ClipRRect(
      borderRadius: widget.borderRadius ?? BorderRadius.zero,
      child: _shouldLoad && widget.imageUrl.isNotEmpty
        ? Image.network(
            widget.imageUrl,
            width: widget.width,
            height: widget.height,
            fit: widget.fit,
            cacheWidth: (widget.width * 2).round(), // 2x for better quality on high-DPI
            cacheHeight: (widget.height * 2).round(),
            errorBuilder: (context, error, stackTrace) {
              return widget.errorWidget ?? _defaultErrorWidget;
            },
            loadingBuilder: (context, child, loadingProgress) {
              if (loadingProgress == null) return child;
              
              return Container(
                width: widget.width,
                height: widget.height,
                decoration: BoxDecoration(
                  color: Colors.grey[100],
                  borderRadius: widget.borderRadius,
                ),
                child: Center(
                  child: SizedBox(
                    width: widget.width * 0.4,
                    height: widget.height * 0.4,
                    child: CircularProgressIndicator(
                      strokeWidth: 2,
                      value: loadingProgress.expectedTotalBytes != null
                        ? loadingProgress.cumulativeBytesLoaded /
                            loadingProgress.expectedTotalBytes!
                        : null,
                    ),
                  ),
                ),
              );
            },
          )
        : widget.placeholder ?? _defaultPlaceholder,
    );
  }
}