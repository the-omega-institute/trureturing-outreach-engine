"""Creator spelling changes only at the Zenodo boundary, including readback."""
import copy
import tempfile
import unittest
from pathlib import Path
import zipfile

from managed.publication import notes
from test_workflow import FakeZenodo, metadata

w = notes.w


class ZenodoCreatorTests(unittest.TestCase):
    def test_publish_normalises_names_without_changing_selected_metadata(self):
        note = metadata()
        note['creators'] = [{'name': 'Blaire Mercer', 'affiliation': 'Example'},
                            {'name': 'Reed, Avery'}, {'name': 'Ada Mary Example'}, {'name': 'Mononym'}]
        original = copy.deepcopy(note)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'metadata.json').write_bytes(w.encode(note))
            (root / 'note.pdf').write_bytes(b'%PDF-1.4\nexample')
            with zipfile.ZipFile(root / 'source.zip', 'w') as archive:
                archive.writestr('note.tex', 'Example theorem')
            output = root / 'prepared'
            w.prepare(root / 'metadata.json', root / 'note.pdf', root / 'source.zip', output)
            package = w.read(output / 'note-v1.json')
            selected = w.verify_package(package, output)
            zenodo = FakeZenodo()
            state = w.publish(package, output, None, lambda _: None, zenodo)
            self.assertEqual([c['name'] for c in zenodo.records[1]['metadata']['creators']],
                             ['Mercer, Blaire', 'Reed, Avery', 'Example, Ada Mary', 'Mononym'])
            self.assertEqual(zenodo.records[1]['metadata']['creators'][0]['affiliation'], 'Example')
            self.assertEqual(w.verify_package(package, output), selected)
            self.assertEqual(note, original)
            self.assertEqual(state['publication']['authors'], ', '.join(c['name'] for c in note['creators']))
            calls = list(zenodo.calls)
            w.publish(package, output, state, lambda _: None, zenodo)
            self.assertEqual(zenodo.calls, calls)

    def test_readback_accepts_legacy_published_creator_form_without_rewriting_it(self):
        note = metadata(); note['creators'] = [{'name': 'Blaire Mercer'}]
        record = {'metadata': {**w.zenodo_metadata(note), 'creators': [{'name': 'Mercer, Blaire'}]}}
        original = copy.deepcopy(record)
        w.check_remote_metadata(record, note)
        self.assertEqual(record, original)
        record['metadata']['creators'][0]['name'] = 'Blaire Mercer'
        w.check_remote_metadata(record, note)
        record['metadata']['creators'][0]['name'] = 'Another Person'
        with self.assertRaisesRegex(ValueError, 'metadata differs'):
            w.check_remote_metadata(record, note)
