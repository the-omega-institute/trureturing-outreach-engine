"""Live Python behavior is the oracle; workflow text is not a frozen fixture."""
from pathlib import Path
import unittest
from unittest.mock import patch

import generate


class ReproducibleVectors(unittest.TestCase):
    def test_unicode_sources(self):
        import unicode_vectors
        root = Path(__file__).resolve().parents[1] / 'crates/outreach-core/src'
        for name, source in unicode_vectors.sources(unicode_vectors.vectors()).items():
            self.assertEqual((root / name).read_text(), source)

    def test_vectors_match_the_live_python_engine_byte_for_byte(self):
        root = Path(__file__).resolve().parent
        # Guard the owner decision: regenerating committed synthetic vectors
        # must not read workflow/source text (or touch a data repository).
        with patch.object(Path, 'open', side_effect=AssertionError('Generator attempted file I/O')):
            generated = generate.generate()
        self.assertEqual({p.name for p in root.glob('*.json')}, set(generated))
        for name, raw in generated.items():
            with self.subTest(family=name):
                self.assertEqual((root / name).read_bytes(), raw)


if __name__ == '__main__':
    unittest.main()
