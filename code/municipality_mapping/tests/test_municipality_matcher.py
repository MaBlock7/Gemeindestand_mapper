import unittest
from municipality_mapping import CONFIG
from municipality_mapping import MunicipalityNameMatcher


class TestMunicipalityNameMatcher(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.matcher = MunicipalityNameMatcher(config=CONFIG)

    def test_normalize_text(self):
        """Test text normalization function"""
        self.assertEqual(self.matcher.normalize_text('Muri b. Bern'), 'muri bei bern')
        self.assertEqual(self.matcher.normalize_text('Rickenbach ZH'), 'rickenbach (zh)')
        self.assertEqual(self.matcher.normalize_text('Rickenbach (ZH)'), 'rickenbach (zh)')
        self.assertEqual(self.matcher.normalize_text('Zürich (Rickenbach ZH)'), 'zuerich (rickenbach (zh))')
        self.assertEqual(self.matcher.normalize_text('Zürich (Rickenbach (ZH))'), 'zuerich (rickenbach (zh))')
        self.assertEqual(self.matcher.normalize_text('New York (NY'), 'new york (ny)')
        self.assertEqual(self.matcher.normalize_text('Yverdon-les-Bain'), 'yverdon les bain')
        self.assertEqual(self.matcher.normalize_text('Buchs ZH'), 'buchs (zh)')
        self.assertEqual(self.matcher.normalize_text('St.Gallen'), 'st gallen')
        self.assertEqual(self.matcher.normalize_text('St. Gallen'), 'st gallen')

    def test_preprocess_officials(self):
        """Test preprocessing of official municipality data"""
        self.matcher.preprocess_officials()
        self.assertTrue('normalized' in self.matcher.officials.columns)

    def test_match_name_exact(self):
        """Test exact name matching"""
        match = self.matcher.match_name(self.matcher.normalize_text('Buchs ZH'), single_name_use=True)
        self.assertEqual(match[0], 83)  # Match by id
        self.assertEqual(match[1], 'Buchs (ZH)')  # Match by name
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('St.Gallen'), single_name_use=True)
        self.assertEqual(match[0], 3203)  # Match by id
        self.assertEqual(match[1], 'St. Gallen')  # Match by name
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Geneva'), single_name_use=True)
        self.assertEqual(match[0], 6621)  # Match by id
        self.assertEqual(match[1], 'Geneva')  # Match by name
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Berlin'), single_name_use=True)
        self.assertEqual(match[0], 0)  # Match by id
        self.assertEqual(match[2], 0.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Berlin (DE)'), single_name_use=True)
        self.assertEqual(match[0], -1)  # Match by id
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Paris FRA'), single_name_use=True)
        self.assertEqual(match[0], -1)  # Match by id
        self.assertEqual(match[2], 1.0)  # Confidence

    def test_match_name_fuzzy(self):
        """Test fuzzy name matching"""
        match = self.matcher.match_name(self.matcher.normalize_text('Appenzel'), single_name_use=True)
        self.assertEqual(match[0], 3101)  # Match by id after correcting

if __name__ == '__main__':
    unittest.main()
