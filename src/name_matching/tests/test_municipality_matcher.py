import unittest
import pandas as pd
from municipality_matcher import MunicipalityMatcher  # Assuming you save your class as 'municipality_matcher.py'

class TestMunicipalityMatcher(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        # Sample data
        official_municipalities = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'name': ['Buchs (ZH)', 'St. Gallen', 'Geneva', 'Appenzel']
        })
        self.matcher = MunicipalityMatcher(official_municipalities, id_col='id', name_col='name', config={
            'foreign_country_codes': ['US', 'DE'],
            'additional_foreign_indicators': ['Allemagne'],
            'false_positives': ['Berlin'],
            'common_mistakes': {'St.Gallen': 'St. Gallen'},
        })

    def test_normalize_text(self):
        """Test text normalization function"""
        self.assertEqual(self.matcher.normalize_text('Muri b. Bern'), 'muri bei bern')
        self.assertEqual(self.matcher.normalize_text('Rickenbach ZH'), 'rickenbach (zh)')
        self.assertEqual(self.matcher.normalize_text('Rickenbach (ZH)'), 'rickenbach (zh)')
        self.assertEqual(self.matcher.normalize_text('Zürich (Rickenbach ZH)'), 'zuerich (rickenbach (zh))')
        self.assertEqual(self.matcher.normalize_text('Zürich (Rickenbach (ZH))'), 'zuerich (rickenbach (zh))')
        self.assertEqual(self.matcher.normalize_text('New York (NY'), 'new york (ny)')
        self.assertEqual(self.matcher.normalize_text('Yverdon-les-Bain)', 'yverdon les bain'))

    def test_preprocess_officials(self):
        """Test preprocessing of official municipality data"""
        self.matcher.preprocess_officials()
        self.assertTrue('normalized' in self.matcher.officials.columns)

    def test_match_name_exact(self):
        """Test exact name matching"""
        match = self.matcher.match_name(self.matcher.normalize_text('Buchs ZH'))
        self.assertEqual(match[0], 1)  # Match by id
        self.assertEqual(match[1], 'Buchs (ZH)')  # Match by name
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('St.Gallen'))
        self.assertEqual(match[0], 2)  # Match by id
        self.assertEqual(match[1], 'St. Gallen')  # Match by name
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Geneva'))
        self.assertEqual(match[0], 3)  # Match by id
        self.assertEqual(match[1], 'Geneva')  # Match by name
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Berlin'))
        self.assertEqual(match[0], 0)  # Match by id
        self.assertEqual(match[2], 0.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Berlin (DE)'))
        self.assertEqual(match[0], -1)  # Match by id
        self.assertEqual(match[2], 1.0)  # Confidence

        match = self.matcher.match_name(self.matcher.normalize_text('Paris FRA'))
        self.assertEqual(match[0], -1)  # Match by id
        self.assertEqual(match[2], 1.0)  # Confidence

    def test_match_name_fuzzy(self):
        """Test fuzzy name matching"""
        match = self.matcher.match_name('Appenzel')
        self.assertEqual(match[0], 4)  # Match by id after correcting

if __name__ == '__main__':
    unittest.main()