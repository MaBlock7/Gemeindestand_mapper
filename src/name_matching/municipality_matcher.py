import pandas as pd
from thefuzz import fuzz
from rapidfuzz import fuzz as rfuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import jellyfish
import re
import unicodedata
from collections import defaultdict
from tqdm import tqdm


class MunicipalityNameMatcher:
    def __init__(self, official_municipalities: pd.DataFrame, id_col: str, name_col: str, config: dict):
        """
        Initialize with official municipality data

        Args:
            official_municipalities: DataFrame with columns ['id', 'name']
        """
        assert id_col in official_municipalities.columns, "The id columns is invalide, please provide the correct column name!"
        assert name_col in official_municipalities.columns, "The name columns is invalide, please provide the correct column name!"

        self.officials = official_municipalities.rename(columns={id_col: 'matched_id', name_col: 'matched_name'})
        self.foreign_country_codes = {code.lower(): (-1, None, 1.0) for code in config.get('foreign_country_codes', [])}
        self.additional_foreign_indicators = {self.normalize_text(name): (-1, None, 1.0)  for name in config.get('additional_foreign_indicators', [])}
        self.false_positives = set([self.normalize_text(name) for name in config.get('false_positives', [])])
        self.common_mistakes = {self.normalize_text(k): self.normalize_text(v) for k, v in config.get('common_mistakes', {}).items()}

        self.preprocess_officials()

    def preprocess_officials(self):
        """Preprocess official names for better matching"""
        # Create normalized versions
        self.officials['normalized'] = self.officials['matched_name'].apply(self.normalize_text)
        self.officials['no_brackets'] = self.officials['normalized'].str.replace(r'\(|\)', '', regex=True)
        self.officials['confidence'] = 1.0

        # Create exact match lookup dictionaries
        self.exact_matches = dict(zip(self.officials['no_brackets'], zip(self.officials['matched_id'], self.officials['matched_name'], self.officials['confidence'])))

        # Create TF-IDF matrix for official names
        self.tfidf = TfidfVectorizer(analyzer='char', ngram_range=(2, 3))
        self.tfidf_matrix = self.tfidf.fit_transform(self.officials['normalized'])

        # Create ngram index
        self.ngram_index = self.create_ngram_index(self.officials['normalized'])

    @staticmethod
    def normalize_text(text):
        """
        Enhanced text normalization for German, French, and Italian characters
        """
        while text.count('(') > text.count(')'):  # Check if there is a missing closing bracket
            text += ')'
        text = re.sub(r'(?<!\s)\(', ' (', text)  # Adds whitespace before '(' if there is none
        text = re.sub(r'\b([A-Z]{2})\b', r'(\1)', text)  # ZH -> (ZH), (ZH ...)-> ((ZH) ...), (... ZH) -> (... (ZH)), (ZH) -> ((ZH)), 
        text = re.sub(r'\((\([A-Z]{2}\))\)', r'\1', text)  # Turns ((ZH)) back into (ZH)
        text = re.sub(r'^à\s+', '', text)  # Removes à at the beginning of a string
        text = re.sub(r'[^)\w\s]+$', '', text)  # Removes any trailing punctuation
        text = re.sub(r'\s+b$', '', text)  # Removes any trailing b

        # Convert to lowercase
        text = text.lower()

        # Replace German umlaute
        text = text.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')

        # Remove accents and special characters (except those handled manually above)
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )

        # Replace Abbreviations and hyphen
        replacements = {            
            ' b.': ' bei', 's.': 'san', ' v. d.': ' von der', ' a. d.': ' an der', ' a.': ' am',
            ' u.': ' und', ' z.': ' zur', 'st-': 'saint-', 'dev-': 'devant-', ' avec': '',
            "'": " ", "-": " ",
        }

        # Apply replacements
        for old, new in replacements.items():
            text = text.replace(old, new)

        # Remove any remaining non-English letters and special characters
        text = re.sub(r'[^a-z\s()]', '', text)

        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    @staticmethod
    def create_ngram_index(texts, n=3):
        """Create ngram index for faster initial filtering"""
        index = defaultdict(set)
        for idx, text in enumerate(texts):
            ngrams = {text[i:i+n] for i in range(len(text)-n+1)}
            for ngram in ngrams:
                index[ngram].add(idx)
        return index

    def get_candidates(self, query_normalized, threshold=0.3):
        """Get candidate matches using ngram filtering"""
        query_ngrams = {query_normalized[i:i+3] for i in range(len(query_normalized)-2)}

        # Get indices of candidates that share ngrams
        candidate_indices = set()
        for ngram in query_ngrams:
            candidate_indices.update(self.ngram_index.get(ngram, set()))

        # Filter candidates by quick ratio threshold
        candidates = self.officials.iloc[list(candidate_indices)]
        candidates = candidates[
            candidates['normalized'].apply(
                lambda x: rfuzz.QRatio(query_normalized, x) > threshold
            )
        ]
        return candidates

    def match_name(self, query_normalized, threshold=0.85):
        """
        Match a single name against official municipalities using multiple techniques

        Returns:
            tuple: (best_match_id, best_match_name, confidence_score)
        """
        # Fix common mistakes
        query_normalized = self.common_mistakes.get(query_normalized, query_normalized)

        # If there are brackets, try to find an exact match
        matches = re.findall(r'\((.*?)\)', query_normalized)
        if matches:
            # First try to find a match from the values contained in brackets
            for match in matches:
                stripped_match = match.replace('(', '').replace(')', '')
                for lookup in [self.foreign_country_codes, self.additional_foreign_indicators, self.exact_matches]:
                    result = lookup.get(stripped_match)
                    if result:
                        return result

            # Else try to find exact match for the parts not in brackets
            remaining_part = re.sub(r'\(.*?\)', '', query_normalized).strip()
            result = self.exact_matches.get(remaining_part)
            if result:
                return result

        # If no exact match, proceed with fuzzy matching
        candidates = self.get_candidates(query_normalized)
        if len(candidates) == 0:
            return None, None, 0.0

        # Calculate various similarity scores
        scores = []
        for _, candidate in candidates.iterrows():
            # TF-IDF cosine similarity
            query_tfidf = self.tfidf.transform([query_normalized])
            tfidf_sim = cosine_similarity(
                query_tfidf, 
                self.tfidf_matrix[candidate.name]
            )[0][0]

            # Levenshtein ratio
            lev_ratio = fuzz.ratio(query_normalized, candidate['normalized']) / 100

            # Partial ratio
            part_ratio = fuzz.partial_ratio(query_normalized, candidate['normalized']) / 100

            # Token sort ratio (handles word reordering)
            token_sort = fuzz.token_sort_ratio(
                query_normalized, 
                candidate['normalized']
            ) / 100

            # Jaro-Winkler similarity (gives more weight to matching prefixes)
            jaro_sim = jellyfish.jaro_winkler_similarity(
                query_normalized, 
                candidate['normalized']
            )

            # Combine scores with weights
            combined_score = (
                0.4 * tfidf_sim +
                0.3 * lev_ratio +
                0.1 * part_ratio +
                0.1 * token_sort +
                0.1 * jaro_sim
            )

            scores.append((
                candidate['matched_id'],
                candidate['matched_name'],
                combined_score
            ))

        # Get best match
        best_match = max(scores, key=lambda x: x[2])

        # Return None if below threshold
        if best_match[2] < threshold:
            if re.match('|'.join([r'\b\(fr\)\b', r'\b\(lu\)\b', r'\b\(be\)\b', r'\b\(gr\)\b', r'\b\(ar\)\b', r'\b\(ge\)\b', r'\b\(sg\)\b', r'\bfra\b', r'\bdeu\b']), query_normalized):
                return -1, None, 1.0
            else:
                return 0, None, 0.0
        elif (best_match[2] >= threshold) and (best_match[1] in self.false_positives):
            return -1, None, 1.0
        else:
            return best_match

    def match_dataframe(self, query_df, query_column, threshold=0.85):
        """Match multiple names in parallel"""
        # Try exact matches via normalized names
        query_df['normalized'] = query_df[query_column].apply(self.normalize_text)
        query_df = query_df.drop_duplicates(subset=['normalized'])

        merged_df = query_df.merge(self.officials, on='normalized', how='left')

        exact_matches = merged_df[~merged_df.matched_id.isna()].copy()
        exact_matches['matched_name'] = exact_matches.normalized
        print(f"Found {len(exact_matches)} exact matches!")

        # Apply advanced matching to unmatched entries
        no_matches = merged_df[merged_df.matched_id.isna()][[query_column, 'normalized']].copy()

        # Apply the function
        def match_apply(row):
            match = self.match_name(row.normalized, threshold)
            return pd.Series({'matched_id': match[0], 'matched_name': match[1], 'confidence': match[2]})

        tqdm.pandas(desc=f'Matching {len(no_matches)} names')
        no_matches[['matched_id', 'matched_name', 'confidence']] = no_matches.progress_apply(match_apply, axis=1)

        return pd.concat([exact_matches, no_matches], axis=0)
