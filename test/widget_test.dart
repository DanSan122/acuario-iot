import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:acuario_iot/main.dart';

void main() {
  testWidgets('Dashboard title is displayed',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.text('Monitoreo de Sensores en Tiempo Real'), findsOneWidget);
  });

  testWidgets('Sidebar status text is displayed',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.text('🟢 Todos los sistemas activos'), findsOneWidget);
  });

  testWidgets('All sensor cards are displayed with expected labels',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.text('Temperatura'), findsOneWidget);
    expect(find.text('27'), findsOneWidget);
    expect(find.text('°C'), findsOneWidget);

    expect(find.text('Nivel de pH'), findsOneWidget);
    expect(find.text('7.2'), findsOneWidget);

    expect(find.text('Turbidez'), findsOneWidget);
    expect(find.text('3'), findsOneWidget);
    expect(find.text('NTU'), findsOneWidget);

    expect(find.text('TDS'), findsOneWidget);
    expect(find.text('465'), findsOneWidget);
    expect(find.text('ppm'), findsOneWidget);
  });

  testWidgets('Top status badges are displayed',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.text('WiFi'), findsOneWidget);
    expect(find.text('MQTT'), findsOneWidget);
  });

  testWidgets('Sidebar menu items are displayed',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.text('Dashboard'), findsOneWidget);
    expect(find.text('Perfil'), findsOneWidget);
    expect(find.text('Configuración'), findsOneWidget);
  });

  testWidgets('Sensor cards use sensor icon',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.byIcon(Icons.sensors), findsNWidgets(6));
  });

  testWidgets('Sidebar and menu icons are displayed',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.byIcon(Icons.dashboard), findsOneWidget);
    expect(find.byIcon(Icons.person), findsOneWidget);
    expect(find.byIcon(Icons.settings), findsOneWidget);
  });

  testWidgets('Brand and tank header are displayed',
      (WidgetTester tester) async {
    await tester.pumpWidget(const AquaMonitorApp());

    expect(find.text('💧 Quirisoft'), findsOneWidget);
    expect(find.text('10:40 | Pecera Principal #A1'), findsOneWidget);
  });
}
