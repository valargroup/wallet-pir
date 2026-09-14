import 'package:test/test.dart';
import 'package:zakura_client/zakura_client.dart';

void main() {
  test('M0 records incomplete-reason presentation near tip', () {
    for (final reason in ['query-budget', 'chain-unknown:110', 'sync-in-progress']) {
      final coverage = TransparentCoverage(coveredThrough: 100, anchorHeight: 100, completion: reason);
      final text = coverage.statusAgainst(105);
      expect(coverage.synchronized, isFalse);
      expect(text, 'Transparent coverage through block 100');
      print('M0_STATUS reason=$reason synchronized=${coverage.synchronized} displayed=$text');
    }
  });
}
