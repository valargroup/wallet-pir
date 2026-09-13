@Tags(['private-validation'])
library;

import 'dart:convert';
import 'dart:io';
import 'package:flutter_test/flutter_test.dart';
import 'package:zakura_bindings/zakura_bindings.dart';

// Explicit operator invocation only. Reads a phrase at runtime; reports no
// mnemonic, key, address, script, amount, transaction ID or exception message.
void main() {
  final env = Platform.environment;
  test(
    'isolated private recovery diagnostic',
    () async {
      final report = File(env['ZAKURA_VALIDATION_REPORT']!);
      final facts = <String, Object?>{'accepted': false, 'stage': 'load'};
      void save() => report.writeAsStringSync('${jsonEncode(facts)}\n');
      NativeBindings? wallet;
      var opened = false;
      try {
        save();
        wallet = await NativeBindings.load(
          libraryPath: env['ZAKURA_BRIDGE_LIBRARY']!,
        );
        final info = await wallet.buildInfo();
        facts['send_enabled'] = info.sendEnabled;
        if (info.sendEnabled) throw StateError('sending enabled');
        facts['stage'] = 'mnemonic-check';
        save();
        var phrase = File(env['ZAKURA_PHRASE_FILE']!).readAsStringSync().trim();
        final valid = await wallet.validateMnemonic(phrase);
        facts['mnemonic_valid'] = valid;
        save();
        if (!valid) throw StateError('invalid mnemonic');
        final lwd =
            env['ZAKURA_LIGHTWALLETD'] ?? 'https://us.zec.stardust.rest:443';
        facts['stage'] = 'network-check';
        save();
        final network = await wallet.networkIdentity(lightwalletdUrl: lwd);
        if (!network.isMainnet) throw StateError('wrong network');
        facts['network_tip'] = network.blockHeight;
        final directory = Directory(env['ZAKURA_VALIDATION_PROFILE']!);
        if (directory.existsSync()) throw StateError('profile exists');
        directory.createSync();
        File('${directory.path}/profile.json').writeAsStringSync(
          jsonEncode({
            'namespace': 'org.valargroup.zakura-recovery-beta',
            'mode': 'shadow',
            'purpose':
                'private M3 library diagnostic; not a release-app result',
          }),
        );
        await wallet.open(
          directory: directory.path,
          lightwalletdUrl: lwd,
          mainnet: true,
          transparentFiltersUrl: 'https://enhance-pir.valargroup.dev',
          transparentShardsUrl: 'https://transparent-pir.valargroup.dev',
        );
        opened = true;
        final birthday = int.parse(env['ZAKURA_BIRTHDAY'] ?? '0');
        facts['birthday'] = birthday;
        facts['stage'] = 'import';
        save();
        final id = await wallet.importAccount(
          phrase: phrase,
          birthday: birthday,
        );
        phrase = '';
        facts['imported'] = true;
        facts['stage'] = 'sync';
        save();
        await wallet.startSync();
        final deadline = DateTime.now().add(
          Duration(
            seconds: int.parse(env['ZAKURA_VALIDATION_SECONDS'] ?? '120'),
          ),
        );
        while (DateTime.now().isBefore(deadline)) {
          await Future<void>.delayed(const Duration(seconds: 2));
          final p = await wallet.progress();
          final b = await wallet.balance(id);
          facts.addAll({
            'phase': p.phase.name,
            'scanned_to': p.scannedTo,
            'sync_failed': p.failed,
            'completion': b.coverage.completion,
            'anchor': b.coverage.anchorHeight,
            'covered': b.coverage.coveredThrough,
            'pending': b.coverage.pendingPages,
            'unresolved': b.coverage.unresolvedSpends,
            'synchronized': b.coverage.synchronized,
          });
          save();
          if (p.failed || b.coverage.synchronized) break;
        }
        await wallet.stopSync();
        await wallet.close();
        opened = false;
        facts['stage'] = 'closed';
        facts['diagnostic_completed'] = true;
        save();
      } catch (_) {
        facts['diagnostic_completed'] = false;
        facts['error'] =
            'operation failed; private exception detail suppressed';
        save();
        fail('private validation operation failed; see sanitized stage report');
      } finally {
        if (opened && wallet != null) {
          try {
            await wallet.stopSync();
          } catch (_) {}
          try {
            await wallet.close();
          } catch (_) {}
        }
      }
    },
    skip: env['ZAKURA_PRIVATE_VALIDATION'] != '1',
    timeout: const Timeout(Duration(minutes: 8)),
  );
}
