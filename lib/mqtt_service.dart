import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:mqtt_client/mqtt_browser_client.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';

class SensorReading {
  final String topic;
  final String value;

  SensorReading({required this.topic, required this.value});
}

class MqttService {
  final String brokerUrl;
  final int brokerPort;
  final int? webSocketPort;
  final String username;
  final String password;
  final String clientId;

  late final MqttClient _client;
  final Set<String> _pendingSubscriptions = <String>{};
  final List<_PendingPublish> _pendingPublishes = <_PendingPublish>[];

  final StreamController<SensorReading> _sensorStreamController =
      StreamController<SensorReading>.broadcast();
  final StreamController<bool> _connectionStreamController =
      StreamController<bool>.broadcast();

  Stream<SensorReading> get sensorUpdates => _sensorStreamController.stream;
  Stream<bool> get connectionStatus => _connectionStreamController.stream;
  
  bool get _hasCredentials =>
      username.trim().isNotEmpty && password.trim().isNotEmpty;

  bool get isConnected => _client.connectionStatus?.state == MqttConnectionState.connected;

  MqttService({
    required this.brokerUrl,
    this.brokerPort = 8883,
    this.webSocketPort,
    required this.username,
    required this.password,
    String? clientId,
  }) : clientId = clientId ?? 'acuario-client-${DateTime.now().millisecondsSinceEpoch}' {
    
    if (kIsWeb) {
      // Flutter Web usa WebSockets; 8883 es TLS nativo y no sirve en navegador.
      final resolvedWebSocketPort = webSocketPort ??
          (brokerPort == 8883 ? 8884 : brokerPort);
      final websocketScheme = resolvedWebSocketPort == 8000 ? 'ws' : 'wss';
      final websocketUrl =
          '$websocketScheme://$brokerUrl:$resolvedWebSocketPort/mqtt';
      _client = MqttBrowserClient(websocketUrl, this.clientId)
        ..port = resolvedWebSocketPort
        ..setProtocolV311()
        ..keepAlivePeriod = 20
        ..autoReconnect = true
        ..logging(on: true) // Activado para depurar la conexión
        ..websocketProtocols = <String>['mqtt', 'mqttv3.1']
        ..onConnected = _onConnected
        ..onDisconnected = _onDisconnected
        ..onAutoReconnect = _onAutoReconnect
        ..onAutoReconnected = _onAutoReconnected;
    } else {
      // 2. CONFIGURACIÓN PARA MÓVIL (Android/iOS) - Usa MQTT Nativo TLS
      _client = MqttServerClient.withPort(brokerUrl, this.clientId, brokerPort)
        ..useWebSocket = false // CAMBIO CLAVE: No usar WebSockets en móvil
        ..secure = true // Usar TLS seguro
        ..setProtocolV311() // Protocolo estable para HiveMQ
        ..keepAlivePeriod = 20
        ..autoReconnect = true
        ..logging(on: true) // Activado para depurar la conexión
        ..onConnected = _onConnected
        ..onDisconnected = _onDisconnected
        ..onAutoReconnect = _onAutoReconnect
        ..onAutoReconnected = _onAutoReconnected;
    }

    var connectMessage = MqttConnectMessage()
        .withClientIdentifier(this.clientId)
        .startClean() // Iniciar una sesión limpia
        .withWillQos(MqttQos.atLeastOnce);

    if (_hasCredentials) {
      connectMessage = connectMessage.authenticateAs(username, password);
    }

    _client.connectionMessage = connectMessage;
  }

  Future<bool> connect() async {
    try {
      print('MQTT: Intentando conectar a $brokerUrl ...');
      print('MQTT: Client ID: $clientId');

      if (!_hasCredentials) {
        print('MQTT: Conexión anónima habilitada (Broker Público).');
      }

      // Las credenciales ya van en connectionMessage.authenticateAs(...).
      await _client.connect();
    } on Exception catch (e) {
      print('MQTT: Error grave de conexión: $e');
      _client.disconnect();
      _connectionStreamController.add(false);
      return false;
    }

    if (_client.connectionStatus?.state != MqttConnectionState.connected) {
      final status = _client.connectionStatus;
      print('MQTT: Conexión rechazada por el Broker, estado: ${status?.state}, código: ${status?.returnCode}');
      _client.disconnect();
      _connectionStreamController.add(false);
      return false;
    }

    _client.updates?.listen(_onMessage);
    return true;
  }

  void subscribeToSensorTopics({List<String>? topics}) {
    final sensorTopics = topics ??
        <String>[
          'acuario/sensores/estado',
          'acuario/actuadores/bomba/estado', // Agregado para escuchar la bomba
        ];

    if (!isConnected) {
      _pendingSubscriptions.addAll(sensorTopics);
      print('MQTT: Conexion pendiente, suscripciones en cola: $sensorTopics');
      return;
    }

    for (final topic in sensorTopics) {
      _client.subscribe(topic, MqttQos.atLeastOnce);
      print('MQTT: Suscrito a $topic');
    }
  }

  void publishMessage({required String topic, required String message}) {
    if (!isConnected) {
      _pendingPublishes.add(_PendingPublish(topic: topic, message: message));
      print('MQTT: Conexion pendiente, publicacion en cola para $topic.');
      return;
    }

    final builder = MqttClientPayloadBuilder();
    builder.addString(message);

    _client.publishMessage(topic, MqttQos.atLeastOnce, builder.payload!);
    print('MQTT: Publicado en $topic -> $message');
  }

  void disconnect() {
    if (isConnected) {
      _client.disconnect();
    }
    _connectionStreamController.add(false);
  }

  void dispose() {
    disconnect();
    _sensorStreamController.close();
    _connectionStreamController.close();
  }

  void _onConnected() {
    print('MQTT: Apretón de manos EXITOSO con HiveMQ.');
    _flushPendingSubscriptions();
    _flushPendingPublishes();
    _connectionStreamController.add(true);
  }

  void _onDisconnected() {
    print('MQTT: Conexión cerrada. Revisa internet o credenciales.');
    _connectionStreamController.add(false);
  }

  void _onAutoReconnect() {
    print('MQTT: Reintentando conexión automáticamente...');
  }

  void _onAutoReconnected() {
    print('MQTT: Reconexión automática exitosa.');
    _flushPendingSubscriptions();
    _flushPendingPublishes();
    _connectionStreamController.add(true);
  }

  void _flushPendingSubscriptions() {
    if (!isConnected || _pendingSubscriptions.isEmpty) {
      return;
    }

    final queued = List<String>.from(_pendingSubscriptions);
    _pendingSubscriptions.clear();
    for (final topic in queued) {
      _client.subscribe(topic, MqttQos.atLeastOnce);
      print('MQTT: Suscripcion en cola activada -> $topic');
    }
  }

  void _flushPendingPublishes() {
    if (!isConnected || _pendingPublishes.isEmpty) {
      return;
    }

    final queued = List<_PendingPublish>.from(_pendingPublishes);
    _pendingPublishes.clear();
    for (final pending in queued) {
      final builder = MqttClientPayloadBuilder();
      builder.addString(pending.message);
      _client.publishMessage(
        pending.topic,
        MqttQos.atLeastOnce,
        builder.payload!,
      );
      print('MQTT: Publicacion en cola enviada -> ${pending.topic}');
    }
  }

  void _onMessage(List<MqttReceivedMessage<MqttMessage>> events) {
    if (events.isEmpty) return;

    final publishMessage = events.first.payload as MqttPublishMessage;
    final rawPayload = MqttPublishPayload.bytesToStringAsString(
      publishMessage.payload.message,
    );
    final topic = events.first.topic;
    final payload = _extractSensorValue(rawPayload);

    print('MQTT: Mensaje recibido en $topic -> $payload');
    _sensorStreamController.add(SensorReading(topic: topic, value: payload));
  }

  String _extractSensorValue(String rawPayload) {
    final trimmed = rawPayload.trim();
    if (trimmed.isEmpty) return '--';

    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is Map<String, dynamic>) {
        const aggregateKeys = <String>{
          'temperatura', 'ph', 'turbidez', 'tds', 'nivel_agua', 'calidad_aire',
        };
        if (decoded.keys.any(aggregateKeys.contains)) {
          return jsonEncode(decoded);
        }
        final value = decoded['value'] ?? decoded['valor'] ?? decoded['reading'];
        if (value != null) return value.toString();
      }
    } catch (_) {
      // Ignorar errores de parseo si es un string simple como "ON" u "OFF"
    }

    return trimmed;
  }
}

class _PendingPublish {
  final String topic;
  final String message;

  const _PendingPublish({required this.topic, required this.message});
}