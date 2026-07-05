import 'dart:convert';

class ResultadoAnalisis {
  final bool datosValidos;
  final Map<String, double> valores;
  final double phCorregido;
  final String actuadorSugerido;
  final List<String> actuadoresSugeridos;
  final List<String> alertasCriticas;
  final String? error;

  const ResultadoAnalisis({
    required this.datosValidos,
    required this.valores,
    required this.phCorregido,
    required this.actuadorSugerido,
    required this.actuadoresSugeridos,
    required this.alertasCriticas,
    this.error,
  });

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'datosValidos': datosValidos,
      'valores': valores,
      'phCorregido': phCorregido,
      'actuadorSugerido': actuadorSugerido,
      'actuadoresSugeridos': actuadoresSugeridos,
      'alertasCriticas': alertasCriticas,
      'error': error,
    };
  }
}

class ModuloAnalisis {
  static const double _tempMin = 26.0;
  static const double _tempMax = 30.0;
  static const double _phMin = 5.0;
  static const double _phMax = 7.8;
  static const double _turbidezMax = 15.0;
  static const double _tdsMax = 300.0;

  /// 1) Deserializa el mensaje MQTT y convierte los campos esperados a double.
  /// Lanza [FormatException] cuando el JSON es nulo, vacio o invalido.
  Map<String, double> deserializarMensaje(String? mensajeJson) {
    if (mensajeJson == null || mensajeJson.trim().isEmpty) {
      throw const FormatException('Mensaje MQTT nulo o vacio');
    }

    final dynamic decoded = jsonDecode(mensajeJson);
    if (decoded is! Map<String, dynamic>) {
      throw const FormatException('El mensaje MQTT no es un objeto JSON valido');
    }

    final temperatura = _toDouble(decoded['temperatura']);
    final ph = _toDouble(decoded['ph']);
    final turbidez = _toDouble(decoded['turbidez']);
    final tds = _toDouble(decoded['tds']);

    if (temperatura == null || ph == null || turbidez == null || tds == null) {
      throw const FormatException(
        'Faltan campos requeridos o no son numericos: temperatura, ph, turbidez, tds',
      );
    }

    return <String, double>{
      'temperatura': temperatura,
      'ph': ph,
      'turbidez': turbidez,
      'tds': tds,
    };
  }

  /// 4) Metodo principal del modulo: parsea, corrige, evalua y sugiere automatizacion.
  ResultadoAnalisis analizarMensaje(String? mensajeJson) {
    try {
      final valores = deserializarMensaje(mensajeJson);

      final temperatura = valores['temperatura']!;
      final phMedido = valores['ph']!;
      final turbidez = valores['turbidez']!;
      final tds = valores['tds']!;

      // 2) pH corregido por temperatura.
      final phCorregido = _compensarPh(phMedido, temperatura);

      final alertas = <String>[];
      final actuadores = <String>[];

      // 3) Evaluacion de umbrales biologicos para pez disco.
      if (temperatura < _tempMin) {
        alertas.add('ALERTA: Temperatura baja');
        actuadores.add('calentador_ON');
      } else if (temperatura > _tempMax) {
        alertas.add('ALERTA: Temperatura alta');
        actuadores.add('enfriador_ON');
      }

      if (phCorregido < _phMin) {
        alertas.add('ALERTA: pH acido');
        actuadores.add('dosificador_base_ON');
      } else if (phCorregido > _phMax) {
        alertas.add('ALERTA: pH alcalino');
        actuadores.add('dosificador_acido_ON');
      }

      if (turbidez >= _turbidezMax) {
        alertas.add('ALERTA: Turbidez elevada');
        actuadores.add('filtro_ON');
      }

      if (tds >= _tdsMax) {
        alertas.add('ALERTA: TDS elevado');
        actuadores.add('recambio_agua_ON');
      }

      return ResultadoAnalisis(
        datosValidos: true,
        valores: valores,
        phCorregido: phCorregido,
        actuadorSugerido: actuadores.isEmpty ? 'ninguno' : actuadores.first,
        actuadoresSugeridos: actuadores,
        alertasCriticas: alertas,
      );
    } catch (e) {
      // Manejo basico para mensajes mal formados o incompletos.
      return ResultadoAnalisis(
        datosValidos: false,
        valores: const <String, double>{},
        phCorregido: 0.0,
        actuadorSugerido: 'ninguno',
        actuadoresSugeridos: const <String>[],
        alertasCriticas: <String>['ALERTA: Error al procesar mensaje MQTT'],
        error: e.toString(),
      );
    }
  }

  /// 2) Compensacion termica del pH.
  double _compensarPh(double phMedido, double temperatura) {
    return phMedido + 0.03 * (temperatura - 25.0);
  }

  double? _toDouble(dynamic value) {
    if (value is num) {
      return value.toDouble();
    }
    if (value is String) {
      return double.tryParse(value.trim());
    }
    return null;
  }
}
