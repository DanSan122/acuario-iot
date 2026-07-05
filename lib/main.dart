import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';

import 'mqtt_service.dart';

void main() => runApp(const AquaMonitorApp());

class AquaMonitorApp extends StatelessWidget {
  const AquaMonitorApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      theme: ThemeData(scaffoldBackgroundColor: const Color(0xFFF4F8FB)),
      home: const MainLayout(),
    );
  }
}

class MainLayout extends StatefulWidget {
  const MainLayout({super.key});

  @override
  State<MainLayout> createState() => _MainLayoutState();
}

class _MainLayoutState extends State<MainLayout> {
  // HiveMQ Cloud (datos del ESP32): MQTT TLS 8883, WebSocket seguro 8884.
  static const String _brokerUrl =
      'b85248f0c8374a1283fa9d48e6ba0c36.s1.eu.hivemq.cloud';
  static const int _brokerPort = 8883;
  static const int _webSocketPort = 8884;
    final String _mqttClientId =
      'acuario-flutter-${DateTime.now().millisecondsSinceEpoch}';
  static const String _mqttUsername = 'admin_acuario';
  static const String _mqttPassword = 'yateolvide'; 
  static const String _topicEstadoSensores = 'acuario/sensores/estado';
  static const String _topicBombaEstado = 'acuario/actuadores/bomba/estado';
  static const String _topicBombaSet = 'acuario/actuadores/bomba/set';

  static const List<String> _sensorTopics = <String>[
    _topicEstadoSensores,
    _topicBombaEstado,
  ];

  int _currentIndex = 0;
  late final MqttService _mqttService;
  StreamSubscription<SensorReading>? _sensorSubscription;
  StreamSubscription<bool>? _connectionSubscription;

  final Map<String, String> _sensorValues = <String, String>{};

  bool _mqttConnected = false;
  bool _bombaEncendida = false;

  List<_ModuloItem> get _modules => <_ModuloItem>[
    _ModuloItem(
      title: 'Dashboard',
      icon: Icons.dashboard,
      page: DashboardPage(sensorValues: _sensorValues, mqttConnected: _mqttConnected),
    ),
    _ModuloItem(
      title: 'Analisis',
      icon: Icons.analytics,
      page: AnalyticsPage(mqttConnected: _mqttConnected),
    ),
    const _ModuloItem(title: 'Perfil', icon: Icons.person, page: ProfilePage()),
    const _ModuloItem(title: 'Mascotas', icon: Icons.pets, page: PetsPage()),
    _ModuloItem(
      title: 'Alimentacion',
      icon: Icons.restaurant,
      page: FeedingPage(
        bombaEncendida: _bombaEncendida,
        onBombaSwitchChanged: _onBombaSwitchChanged,
      ),
    ),
    _ModuloItem(
      title: 'Alertas',
      icon: Icons.notifications,
      page: AlertsPage(sensorValues: _sensorValues),
    ),
  ];

  @override
  void initState() {
    super.initState();

    _mqttService = MqttService(
      brokerUrl: _brokerUrl,
      brokerPort: _brokerPort,
      webSocketPort: _webSocketPort,
      clientId: _mqttClientId,
      username: _mqttUsername,
      password: _mqttPassword,
    );

    _sensorSubscription = _mqttService.sensorUpdates.listen((reading) {
      if (!mounted) {
        return;
      }

      final topic = _normalizeTopic(reading.topic);

      if (topic == _topicEstadoSensores) {
        _handleEstadoSensoresMessage(reading.value);
        return;
      }

      if (topic == _topicBombaEstado) {
        _handleBombaEstadoMessage(reading.value);
        return;
      }

      final updates = _extractSensorUpdates(reading);
      if (updates.isEmpty) {
        return;
      }

      setState(() {
        _sensorValues.addAll(updates);
      });
    });

    _connectionSubscription = _mqttService.connectionStatus.listen((connected) {
      if (!mounted) {
        return;
      }

      setState(() {
        _mqttConnected = connected;
      });

      if (connected) {
        _mqttService.subscribeToSensorTopics(topics: _sensorTopics);
      }
    });

    if (_isMqttConfigured()) {
      _connectMqtt();
    } else {
      debugPrint('MQTT: Configuracion pendiente. Actualiza broker, usuario y password en main.dart.');
    }
  }

  bool _isMqttConfigured() {
    return !_brokerUrl.startsWith('TU_') &&
        !_mqttUsername.startsWith('TU_') &&
        !_mqttPassword.startsWith('TU_');
  }

  Future<void> _connectMqtt() async {
    final connected = await _mqttService.connect();
    if (!mounted) {
      return;
    }

    setState(() {
      _mqttConnected = connected;
    });

    if (connected) {
      _mqttService.subscribeToSensorTopics(topics: _sensorTopics);
    }
  }

  String _normalizeTopic(String topic) {
    final normalized = topic.trim().toLowerCase();
    const aliases = <String, String>{
      'acuario/estado': _topicEstadoSensores,
      'acuario/sensores/temp': 'acuario/sensores/temperatura',
      'acuario/sensores/temperature': 'acuario/sensores/temperatura',
      'acuario/temperatura': 'acuario/sensores/temperatura',
      'acuario/ph': 'acuario/sensores/ph',
      'acuario/tds': 'acuario/sensores/tds',
    };
    return aliases[normalized] ?? normalized;
  }

  void _handleEstadoSensoresMessage(String payload) {
    try {
      final decoded = jsonDecode(payload);
      if (decoded is! Map<String, dynamic>) {
        debugPrint('MQTT: estado de sensores no es JSON objeto: $payload');
        return;
      }

      final temperatura = _asDouble(decoded['temperatura']);
      final ph = _asDouble(decoded['ph']);
      final turbidez = _asDouble(decoded['turbidez']);
      final tds = _asDouble(decoded['tds']);

      if (temperatura == null || ph == null || turbidez == null || tds == null) {
        debugPrint('MQTT: estado incompleto o invalido: $payload');
        return;
      }

      setState(() {
        _sensorValues['acuario/sensores/temperatura'] =
            temperatura.toStringAsFixed(2);
        _sensorValues['acuario/sensores/ph'] = ph.toStringAsFixed(2);
        _sensorValues['acuario/sensores/turbidez'] = turbidez.toStringAsFixed(2);
        _sensorValues['acuario/sensores/tds'] = tds.toStringAsFixed(2);
      });

      debugPrint(
        'MQTT estado -> temperatura=$temperatura, ph=$ph, turbidez=$turbidez, tds=$tds',
      );
    } catch (e) {
      debugPrint('MQTT: Error decodificando acuario/sensores/estado: $e');
    }
  }

  void _handleBombaEstadoMessage(String payload) {
    final estado = payload.trim().toUpperCase();
    if (estado != 'ON' && estado != 'OFF') {
      debugPrint('MQTT: Estado de bomba invalido: $payload');
      return;
    }

    setState(() {
      _bombaEncendida = estado == 'ON';
    });
  }

  double? _asDouble(dynamic value) {
    if (value is num) {
      return value.toDouble();
    }
    if (value is String) {
      return double.tryParse(value.trim());
    }
    return null;
  }

  void _onBombaSwitchChanged(bool nextValue) {
    final command = nextValue ? 'ON' : 'OFF';
    _mqttService.publishMessage(topic: _topicBombaSet, message: command);
    debugPrint('MQTT: Comando bomba enviado -> $command');
  }

  Map<String, String> _extractSensorUpdates(SensorReading reading) {
    final updates = <String, String>{};
    final normalizedTopic = _normalizeTopic(reading.topic);
    final payload = reading.value.trim();

    try {
      final decoded = jsonDecode(payload);
      if (decoded is Map<String, dynamic>) {
        final jsonKeyToTopic = <String, String>{
          'temperatura': 'acuario/sensores/temperatura',
          'ph': 'acuario/sensores/ph',
          'turbidez': 'acuario/sensores/turbidez',
          'tds': 'acuario/sensores/tds',
          'nivel_agua': 'acuario/sensores/nivel_agua',
          'calidad_aire': 'acuario/sensores/calidad_aire',
        };

        jsonKeyToTopic.forEach((jsonKey, topic) {
          final value = decoded[jsonKey];
          if (value != null) {
            updates[topic] = value.toString();
          }
        });

        if (updates.isNotEmpty) {
          return updates;
        }
      }
    } catch (_) {
      // Si el payload no es JSON, se procesa como valor simple por topico.
    }

    updates[normalizedTopic] = reading.value;
    return updates;
  }

  @override
  void dispose() {
    _sensorSubscription?.cancel();
    _connectionSubscription?.cancel();
    _mqttService.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final safeIndex = _currentIndex.clamp(0, _modules.length - 1);
    return Scaffold(
      body: Row(
        children: [
          Container(
            width: 250,
            color: Colors.white,
            child: Column(
              children: [
                const Padding(
                  padding: EdgeInsets.all(20),
                  child: Text(
                    'Quirisoft',
                    style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                  ),
                ),
                for (var i = 0; i < _modules.length; i++)
                  _menuItem(_modules[i].title, _modules[i].icon, i),
              ],
            ),
          ),
          Expanded(child: _modules[safeIndex].page),
        ],
      ),
    );
  }

  Widget _menuItem(String title, IconData icon, int index) {
    final isSelected = _currentIndex == index;
    return ListTile(
      leading: Icon(icon, color: isSelected ? Colors.blue : Colors.grey),
      title: Text(
        title,
        style: TextStyle(color: isSelected ? Colors.blue : Colors.grey),
      ),
      onTap: () => setState(() => _currentIndex = index),
      tileColor: isSelected ? Colors.blue.withValues(alpha: 0.1) : null,
    );
  }
}

class _ModuloItem {
  final String title;
  final IconData icon;
  final Widget page;

  const _ModuloItem({
    required this.title,
    required this.icon,
    required this.page,
  });
}

// MÓDULO 1: Dashboard 
class DashboardPage extends StatelessWidget {
  final Map<String, String> sensorValues;
  final bool mqttConnected;

  const DashboardPage({
    super.key,
    required this.sensorValues,
    required this.mqttConnected,
  });

  String _sensor(String topic, String fallback) {
    return sensorValues[topic] ?? fallback;
  }

  bool _isWarning(String topic) {
    final raw = sensorValues[topic];
    final value = raw != null ? double.tryParse(raw) : null;
    if (value == null) {
      return false;
    }

    switch (topic) {
      case 'acuario/sensores/temperatura':
        return value < 24 || value > 30;
      case 'acuario/sensores/ph':
        return value < 6.5 || value > 8.5;
      case 'acuario/sensores/turbidez':
        return value > 3;
      case 'acuario/sensores/tds':
        return value > 500;
      case 'acuario/sensores/nivel_agua':
        return value < 80;
      default:
        return false;
    }
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(30.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Barra superior del Dashboard
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              const Text('Pecera Principal #A1', style: TextStyle(color: Colors.grey)),
              Chip(
                label: Text(mqttConnected ? 'MQTT Conectado' : 'MQTT Desconectado'),
                backgroundColor: mqttConnected ? Colors.green.shade50 : Colors.red.shade50,
              ),
            ],
          ),
          const SizedBox(height: 20),
          const Text('Monitoreo de Sensores en Tiempo Real', 
              style: TextStyle(fontSize: 22, fontWeight: FontWeight.bold)),
          const SizedBox(height: 20),
          
          // Grid de sensores
          Expanded(
            child: GridView.count(
              crossAxisCount: 3,
              childAspectRatio: 1.4,
              crossAxisSpacing: 20,
              mainAxisSpacing: 20,
              children: [
                SensorCard(
                  title: 'Temperatura del Agua',
                  value: _sensor('acuario/sensores/temperatura', '--'),
                  unit: '°C',
                  isWarning: _isWarning('acuario/sensores/temperatura'),
                ),
                SensorCard(
                  title: 'Nivel de pH',
                  value: _sensor('acuario/sensores/ph', '--'),
                  unit: 'pH',
                  isWarning: _isWarning('acuario/sensores/ph'),
                ),
                SensorCard(
                  title: 'Turbidez',
                  value: _sensor('acuario/sensores/turbidez', '--'),
                  unit: 'NTU',
                  isWarning: _isWarning('acuario/sensores/turbidez'),
                ),
                SensorCard(
                  title: 'TDS (Sólidos Disueltos)',
                  value: _sensor('acuario/sensores/tds', '--'),
                  unit: 'ppm',
                  isWarning: _isWarning('acuario/sensores/tds'),
                ),
                SensorCard(
                  title: 'Nivel de Agua',
                  value: _sensor('acuario/sensores/nivel_agua', '--'),
                  unit: '%',
                  isWarning: _isWarning('acuario/sensores/nivel_agua'),
                ),
                SensorCard(
                  title: 'Calidad del Aire',
                  value: _sensor('acuario/sensores/calidad_aire', '--'),
                  unit: 'AQI',
                ),
              ],
            ),
          )
        ],
      ),
    );
  }
}

// Widget de tarjeta reutilizable
class SensorCard extends StatelessWidget {
  final String title, value, unit;
  final bool isWarning;
  const SensorCard({super.key, required this.title, required this.value, required this.unit, this.isWarning = false});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
      child: Column(
        children: [
          Row(children: [Icon(Icons.water_drop, color: Colors.blue), SizedBox(width: 8), Text(title)]),
          const Spacer(),
          Container(
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(shape: BoxShape.circle, border: Border.all(color: isWarning ? Colors.orange : Colors.teal, width: 6)),
            child: Column(children: [Text(value, style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold)), Text(unit)]),
          ),
          const Spacer(),
          Text(isWarning ? "Advertencia" : "Normal", style: TextStyle(color: isWarning ? Colors.orange : Colors.teal)),
        ],
      ),
    );
  }
}

class AnalyticsPage extends StatelessWidget {
  final bool mqttConnected;

  const AnalyticsPage({
    super.key,
    required this.mqttConnected,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(20.0),
      child: Row(
        children: [
          // PARTE IZQUIERDA: Cuadrícula de Gráficos
          Expanded(
            flex: 2,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text("Análisis de Calidad del Agua", 
                    style: TextStyle(fontSize: 22, fontWeight: FontWeight.bold, color: Color(0xFF006064))),
                const SizedBox(height: 20),
                Expanded(
                  child: GridView.count(
                    crossAxisCount: 2,
                    mainAxisSpacing: 20,
                    crossAxisSpacing: 20,
                    childAspectRatio: 1.5,
                    children: const [
                      _ChartCard(title: "Historial de pH"),
                      _ChartCard(title: "Tendencia de Temperatura"),
                      _ChartCard(title: "Análisis de Turbidez"),
                      _ChartCard(title: "Monitor de Nivel de Agua"),
                    ],
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(width: 20),
          
          // PARTE DERECHA: Alertas y Conectividad
          SizedBox(
            width: 350,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text("Alertas del Sistema", style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16)),
                const SizedBox(height: 10),
                AlertaItem(icon: Icons.warning_amber, title: "Alta Turbidez", subtitle: "Nivel de turbidez excedió el umbral en 3.2 NTU", time: "Hace 2 min", color: Colors.orange),
                AlertaItem(icon: Icons.error_outline, title: "Nivel de Agua Bajo", subtitle: "El nivel de agua cayó por debajo del 80%", time: "Hace 15 min", color: Colors.red),
                AlertaItem(icon: Icons.info_outline, title: "Filtro Limpiado", subtitle: "Mantenimiento del filtro completado", time: "Hace 1 hora", color: Colors.blue),
                AlertaItem(icon: Icons.info_outline, title: "pH Estabilizado", subtitle: "El nivel de pH volvió al rango normal", time: "Hace 2 horas", color: Colors.blue),
                
                const SizedBox(height: 30),
                const Text("Estado de Conectividad IoT", style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16)),
                const SizedBox(height: 10),
                const ConectividadItem(icon: Icons.memory, title: "Dispositivo ESP32", status: "192.168.1.100"),
                ConectividadItem(
                  icon: Icons.storage,
                  title: "MQTT HiveMQ",
                  status: mqttConnected ? "Conectado" : "Desconectado",
                ),
                const ConectividadItem(icon: Icons.cloud_queue, title: "Sincronización Firebase", status: "Tiempo Real"),
                const ConectividadItem(icon: Icons.wifi, title: "Señal WiFi", status: "-42 dBm"),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

// Widget auxiliar para tarjetas de gráficos
class _ChartCard extends StatelessWidget {
  final String title;
  const _ChartCard({required this.title});
  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
      child: Column(
        children: [
          Padding(padding: const EdgeInsets.all(15), child: Text(title, style: TextStyle(fontWeight: FontWeight.bold))),
          const Expanded(child: Center(child: Text("Espacio para Gráfico", style: TextStyle(color: Colors.grey)))),
        ],
      ),
    );
  }
}

class AlertaItem extends StatelessWidget {
  final IconData icon;
  final String title;
  final String subtitle;
  final String time;
  final Color color;

  const AlertaItem({
    super.key,
    required this.icon,
    required this.title,
    required this.subtitle,
    required this.time,
    required this.color,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(bottom: 10),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Icon(icon, color: color),
          const SizedBox(width: 10),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  title,
                  style: const TextStyle(fontWeight: FontWeight.bold),
                ),
                const SizedBox(height: 3),
                Text(
                  subtitle,
                  style: const TextStyle(color: Colors.black87),
                ),
                const SizedBox(height: 6),
                Text(
                  time,
                  style: const TextStyle(color: Colors.grey, fontSize: 12),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class ConectividadItem extends StatelessWidget {
  final IconData icon;
  final String title;
  final String status;

  const ConectividadItem({
    super.key,
    required this.icon,
    required this.title,
    required this.status,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(bottom: 10),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        children: [
          Icon(icon, color: const Color(0xFF006064)),
          const SizedBox(width: 10),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  title,
                  style: const TextStyle(fontWeight: FontWeight.bold),
                ),
                const SizedBox(height: 3),
                Text(
                  status,
                  style: const TextStyle(color: Colors.black87),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class ProfilePage extends StatelessWidget {
  const ProfilePage({super.key});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(25.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text("Perfil de Usuario", style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
          const Text("Información de tu cuenta y sistema de acuario", style: TextStyle(color: Colors.grey)),
          const SizedBox(height: 20),
          
          Row(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // COLUMNA IZQUIERDA: Info Usuario
              Expanded(
                flex: 2,
                child: Container(
                  padding: const EdgeInsets.all(25),
                  decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
                  child: Column(
                    children: [
                      Row(children: [
                        const CircleAvatar(radius: 30, backgroundColor: Colors.blue, child: Icon(Icons.person, color: Colors.white, size: 30)),
                        const SizedBox(width: 15),
                        const Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                          Text("Juan Pérez", style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                          Text("Propietario del Sistema", style: TextStyle(color: Colors.grey)),
                        ]),
                      ]),
                      const SizedBox(height: 20),
                      _InfoField(icon: Icons.email, title: "Correo Electrónico", value: "juan.perez@quirisoft.com"),
                      _InfoField(icon: Icons.phone, title: "Teléfono", value: "+1 (555) 123-4567"),
                      _InfoField(icon: Icons.location_on, title: "Ubicación", value: "Madrid, España"),
                      _InfoField(icon: Icons.calendar_today, title: "Miembro Desde", value: "15 de Enero, 2024"),
                    ],
                  ),
                ),
              ),
              const SizedBox(width: 20),
              
              // COLUMNA DERECHA: Estadísticas
              SizedBox(
                width: 300,
                child: Column(
                  children: [
                    _StatCard(title: "Acuarios Activos", value: "1", color: Colors.blue),
                    _StatCard(title: "Sensores Conectados", value: "6", color: Colors.green),
                    _StatCard(title: "Salud del Sistema", value: "98%", color: Colors.purple),
                  ],
                ),
              ),
            ],
          ),
          
          const SizedBox(height: 20),
          
          // INFO ACUARIO (Inferior)
          Container(
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceAround,
              children: [
                _AcuarioInfo(label: "Nombre", value: "Pecera Principal #A1"),
                _AcuarioInfo(label: "Capacidad", value: "200 Litros"),
                _AcuarioInfo(label: "Tipo", value: "Agua Dulce Tropical"),
                _AcuarioInfo(label: "Instalado", value: "20/01/2024"),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

// WIDGETS AUXILIARES
class _InfoField extends StatelessWidget {
  final IconData icon;
  final String title, value;
  const _InfoField({required this.icon, required this.title, required this.value});
  @override
  Widget build(BuildContext context) => ListTile(leading: Icon(icon), title: Text(title, style: TextStyle(fontSize: 12, color: Colors.grey)), subtitle: Text(value, style: TextStyle(fontWeight: FontWeight.bold)));
}

class _StatCard extends StatelessWidget {
  final String title, value;
  final Color color;
  const _StatCard({required this.title, required this.value, required this.color});
  @override
  Widget build(BuildContext context) => Container(
    margin: const EdgeInsets.only(bottom: 15),
    padding: const EdgeInsets.all(20),
    decoration: BoxDecoration(color: color, borderRadius: BorderRadius.circular(10)),
    child: Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [
      Text(title, style: const TextStyle(color: Colors.white)),
      Text(value, style: const TextStyle(color: Colors.white, fontSize: 20, fontWeight: FontWeight.bold)),
    ]),
  );
}

class _AcuarioInfo extends StatelessWidget {
  final String label, value;
  const _AcuarioInfo({required this.label, required this.value});
  @override
  Widget build(BuildContext context) => Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
    Text(label, style: const TextStyle(color: Colors.grey, fontSize: 12)),
    Text(value, style: const TextStyle(fontWeight: FontWeight.bold)),
  ]);
}

class PetsPage extends StatelessWidget {
  const PetsPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(25.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text("Gestión de Mascotas", style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
          const Text("Administra la información y salud de tus peces", style: TextStyle(color: Colors.grey)),
          const SizedBox(height: 20),
          
          // TARJETAS DE MASCOTAS
          SizedBox(
            height: 220,
            child: Row(
              children: [
                _PetCard(name: "Nemo", species: "Pez Payaso", color: "Naranja y Blanco", age: "2 años", health: "95%"),
                const SizedBox(width: 20),
                _PetCard(name: "Dory", species: "Pez Cirujano Azul", color: "Azul", age: "1.5 años", health: "92%"),
                const SizedBox(width: 20),
                _PetCard(name: "Bubbles", species: "Guppy", color: "Multicolor", age: "8 meses", health: "98%"),
              ],
            ),
          ),
          
          const SizedBox(height: 20),
          
          // SECCIÓN DE ALIMENTACIÓN
          const Text("Frecuencia de Alimentación", style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
          const SizedBox(height: 15),
          Container(
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
            child: const Row(
              mainAxisAlignment: MainAxisAlignment.spaceAround,
              children: [
                _FeedInfo(label: "Alimentaciones Diarias", value: "3 veces"),
                _FeedInfo(label: "Última Alimentación", value: "Hace 2h"),
                _FeedInfo(label: "Próxima Comida", value: "En 4h"),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

// WIDGET TARJETA DE MASCOTA
class _PetCard extends StatelessWidget {
  final String name, species, color, age, health;
  const _PetCard({required this.name, required this.species, required this.color, required this.age, required this.health});

  @override
  Widget build(BuildContext context) => Expanded(
    child: Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
      child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
        Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [
          const CircleAvatar(backgroundColor: Colors.blue, child: Icon(Icons.water, color: Colors.white)),
          Container(padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4), decoration: BoxDecoration(color: Colors.green.withOpacity(0.1), borderRadius: BorderRadius.circular(10)), child: Text("Salud: $health", style: const TextStyle(fontSize: 12, color: Colors.green))),
        ]),
        const SizedBox(height: 10),
        Text(name, style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
        Text(species, style: const TextStyle(color: Colors.grey)),
        const Spacer(),
        Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [Text("Color: $color"), Text("Edad: $age")]),
        const SizedBox(height: 10),
        LinearProgressIndicator(value: 0.8, backgroundColor: Colors.grey.shade200),
      ]),
    ),
  );
}

// WIDGET INFO ALIMENTACIÓN
class _FeedInfo extends StatelessWidget {
  final String label, value;
  const _FeedInfo({required this.label, required this.value});
  @override
  Widget build(BuildContext context) => Column(children: [
    Text(label, style: const TextStyle(color: Colors.grey)),
    Text(value, style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
  ]);
}

class FeedingPage extends StatelessWidget {
  final bool bombaEncendida;
  final ValueChanged<bool> onBombaSwitchChanged;

  const FeedingPage({
    super.key,
    required this.bombaEncendida,
    required this.onBombaSwitchChanged,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(25.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text("Tiempo de Alimentación", style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
          const Text("Programa y gestiona la alimentación automática", style: TextStyle(color: Colors.grey)),
          const SizedBox(height: 20),
          
          // BOTÓN DE ACCIÓN MANUAL
          Container(
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(color: Colors.orange.shade700, borderRadius: BorderRadius.circular(15)),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                const Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                  Text("Alimentación Manual", style: TextStyle(color: Colors.white, fontSize: 18, fontWeight: FontWeight.bold)),
                  Text("Alimentar a los peces ahora", style: TextStyle(color: Colors.white70)),
                ]),
                ElevatedButton.icon(
                  onPressed: () {},
                  icon: const Icon(Icons.play_arrow, color: Colors.orange),
                  label: const Text("Alimentar Ahora", style: TextStyle(color: Colors.orange)),
                  style: ElevatedButton.styleFrom(backgroundColor: Colors.white),
                )
              ],
            ),
          ),
          
          const SizedBox(height: 20),
          
          // LISTA DE HORARIOS
          Expanded(
            child: Container(
              padding: const EdgeInsets.all(20),
              decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
              child: Column(
                children: [
                  Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [
                    const Row(children: [Icon(Icons.schedule, color: Colors.blue), SizedBox(width: 10), Text("Horarios Programados", style: TextStyle(fontWeight: FontWeight.bold))]),
                    TextButton.icon(onPressed: () {}, icon: const Icon(Icons.add), label: const Text("Agregar")),
                  ]),
                  const SizedBox(height: 15),
                  const _ScheduleItem(time: "08:00", title: "Alimentación 1"),
                  const _ScheduleItem(time: "14:00", title: "Alimentación 2"),
                  const _ScheduleItem(time: "20:00", title: "Alimentación 3"),
                  _PumpSwitchItem(
                    isOn: bombaEncendida,
                    onChanged: onBombaSwitchChanged,
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

// WIDGET ITEM DE HORARIO
class _ScheduleItem extends StatelessWidget {
  final String time, title;
  const _ScheduleItem({required this.time, required this.title});

  @override
  Widget build(BuildContext context) => Container(
    margin: const EdgeInsets.only(bottom: 15),
    padding: const EdgeInsets.all(15),
    decoration: BoxDecoration(color: Colors.blue.shade50, borderRadius: BorderRadius.circular(10)),
    child: Row(children: [
      Container(padding: const EdgeInsets.all(10), decoration: BoxDecoration(color: Colors.blue, borderRadius: BorderRadius.circular(8)), child: Text(time, style: const TextStyle(color: Colors.white, fontWeight: FontWeight.bold))),
      const SizedBox(width: 15),
      Column(crossAxisAlignment: CrossAxisAlignment.start, children: [Text(title, style: const TextStyle(fontWeight: FontWeight.bold)), const Text("Todos los días", style: TextStyle(fontSize: 12, color: Colors.grey))]),
      const Spacer(),
      Switch(value: true, onChanged: null),
      const Text("Activo", style: TextStyle(color: Colors.green)),
    ]),
  );
}

class _PumpSwitchItem extends StatelessWidget {
  final bool isOn;
  final ValueChanged<bool> onChanged;

  const _PumpSwitchItem({
    required this.isOn,
    required this.onChanged,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(top: 12),
      padding: const EdgeInsets.all(15),
      decoration: BoxDecoration(
        color: Colors.teal.shade50,
        borderRadius: BorderRadius.circular(10),
      ),
      child: Row(
        children: [
          const Icon(Icons.water, color: Colors.teal),
          const SizedBox(width: 10),
          const Expanded(
            child: Text(
              'Bomba de recirculacion',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
          ),
          Text(isOn ? 'ON' : 'OFF'),
          const SizedBox(width: 8),
          Switch(
            value: isOn,
            onChanged: onChanged,
          ),
        ],
      ),
    );
  }
}

class AlertsPage extends StatelessWidget {
  final Map<String, String> sensorValues;

  const AlertsPage({
    super.key,
    required this.sensorValues,
  });

  double? _asDouble(String topic) {
    final raw = sensorValues[topic];
    if (raw == null) {
      return null;
    }
    return double.tryParse(raw);
  }

  List<_AlertData> _buildAlerts() {
    final alerts = <_AlertData>[];
    final turbidez = _asDouble('acuario/sensores/turbidez');
    final nivelAgua = _asDouble('acuario/sensores/nivel_agua');
    final ph = _asDouble('acuario/sensores/ph');
    final temperatura = _asDouble('acuario/sensores/temperatura');

    if (turbidez != null && turbidez > 3) {
      alerts.add(
        _AlertData(
          title: 'Alta Turbidez',
          desc: 'Nivel de turbidez excedio el umbral: ${turbidez.toStringAsFixed(1)} NTU',
          time: 'En vivo',
          color: Colors.yellow.shade100,
        ),
      );
    }

    if (nivelAgua != null && nivelAgua < 80) {
      alerts.add(
        _AlertData(
          title: 'Nivel de Agua Bajo',
          desc: 'El nivel de agua cayo por debajo del 80%: ${nivelAgua.toStringAsFixed(1)}%',
          time: 'En vivo',
          color: Colors.red.shade50,
          isCritical: true,
        ),
      );
    }

    if (ph != null && (ph < 6.5 || ph > 8.5)) {
      alerts.add(
        _AlertData(
          title: 'pH fuera de rango',
          desc: 'El pH actual es ${ph.toStringAsFixed(2)} (rango recomendado: 6.5-8.5)',
          time: 'En vivo',
          color: Colors.orange.shade100,
        ),
      );
    }

    if (temperatura != null && (temperatura < 24 || temperatura > 30)) {
      alerts.add(
        _AlertData(
          title: 'Temperatura fuera de rango',
          desc: 'Temperatura actual: ${temperatura.toStringAsFixed(1)} °C',
          time: 'En vivo',
          color: Colors.orange.shade100,
        ),
      );
    }

    if (alerts.isEmpty) {
      alerts.add(
        _AlertData(
          title: 'Sin alertas activas',
          desc: 'Todos los parametros se mantienen en rangos normales.',
          time: 'Ahora',
          color: Colors.green.shade50,
        ),
      );
    }

    return alerts;
  }

  @override
  Widget build(BuildContext context) {
    final alerts = _buildAlerts();
    final activeCount = alerts.where((a) => a.title != 'Sin alertas activas').length;
    final hasCritical = alerts.any((a) => a.isCritical);

    return Padding(
      padding: const EdgeInsets.all(25.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // CABECERA CON BOTONES
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              const Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                Text("Alertas y Notificaciones", style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold)),
                Text("Monitorea las alertas activas y el historial", style: TextStyle(color: Colors.grey)),
              ]),
              Row(children: [
                OutlinedButton.icon(onPressed: () {}, icon: const Icon(Icons.notifications_active), label: const Text("Activar")),
                const SizedBox(width: 10),
                OutlinedButton.icon(onPressed: () {}, icon: const Icon(Icons.notifications_off), label: const Text("Silenciar")),
              ])
            ],
          ),
          const SizedBox(height: 20),
          
          // BANNER CRÍTICO
          Container(
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(
              color: hasCritical ? Colors.red.shade300 : Colors.green.shade400,
              borderRadius: BorderRadius.circular(10),
            ),
            child: Row(children: [
              const Icon(Icons.warning, color: Colors.white, size: 30),
              const SizedBox(width: 15),
              Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                Text(
                  hasCritical ? 'ALERTA CRITICA' : 'Sistema Estable',
                  style: const TextStyle(color: Colors.white, fontWeight: FontWeight.bold, fontSize: 18),
                ),
                Text(
                  hasCritical
                      ? 'Se detectaron alertas criticas que requieren atencion inmediata.'
                      : 'No hay alertas criticas activas en este momento.',
                  style: const TextStyle(color: Colors.white),
                ),
              ])
            ]),
          ),
          
          const SizedBox(height: 20),
          
          // LISTADO DE ALERTAS
          Expanded(
            child: Container(
              padding: const EdgeInsets.all(20),
              decoration: BoxDecoration(color: Colors.white, borderRadius: BorderRadius.circular(15)),
              child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [
                  const Text('Alertas Activas', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 18)),
                  Chip(
                    label: Text(
                      '$activeCount Activas',
                      style: TextStyle(color: activeCount > 0 ? Colors.red : Colors.green),
                    ),
                  ),
                ]),
                const SizedBox(height: 15),
                for (final alert in alerts) ...[
                  _AlertItem(
                    title: alert.title,
                    desc: alert.desc,
                    time: alert.time,
                    color: alert.color,
                    isCritical: alert.isCritical,
                  ),
                  const SizedBox(height: 15),
                ],
              ]),
            ),
          ),
        ],
      ),
    );
  }
}

class _AlertData {
  final String title;
  final String desc;
  final String time;
  final Color color;
  final bool isCritical;

  const _AlertData({
    required this.title,
    required this.desc,
    required this.time,
    required this.color,
    this.isCritical = false,
  });
}

// WIDGET ITEM DE ALERTA
class _AlertItem extends StatelessWidget {
  final String title, desc, time;
  final Color color;
  final bool isCritical;

  const _AlertItem({required this.title, required this.desc, required this.time, required this.color, this.isCritical = false});

  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.all(20),
    decoration: BoxDecoration(color: color, borderRadius: BorderRadius.circular(10), border: Border.all(color: Colors.grey.shade200)),
    child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
      Row(mainAxisAlignment: MainAxisAlignment.spaceBetween, children: [
        Row(children: [Icon(isCritical ? Icons.cancel : Icons.warning_amber), const SizedBox(width: 10), Text(title, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 16))]),
        Row(children: [if(isCritical) const Chip(label: Text("CRÍTICO", style: TextStyle(color: Colors.white)), backgroundColor: Colors.red), const SizedBox(width: 10), Text(time, style: const TextStyle(color: Colors.grey))]),
      ]),
      Text(desc),
      const SizedBox(height: 10),
      Row(children: [
        ElevatedButton(onPressed: () {}, child: const Text("Ver Detalles")),
        const SizedBox(width: 10),
        ElevatedButton(onPressed: () {}, style: ElevatedButton.styleFrom(backgroundColor: Colors.blue), child: const Text("Resolver", style: TextStyle(color: Colors.white))),
      ])
    ]),
  );
}