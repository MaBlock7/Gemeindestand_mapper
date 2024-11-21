import re
import unicodedata
from collections import defaultdict
from typing import Union

import jellyfish
import pandas as pd
from thefuzz import fuzz
from rapidfuzz import fuzz as rfuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

from .base_class import BaseMunicipalityData


class MunicipalityNameMatcher(BaseMunicipalityData):
    """
    A class for managing and mapping Swiss municipality (Gemeinde) data.

    This class provides methods to normalize, preprocess, and match
    municipality names to a standardized list of official Swiss
    municipalities. It leverages fuzzy matching, TF-IDF similarity,
    and other techniques to ensure accurate matches.

    :ivar officials: A DataFrame containing official municipality data, with normalized names and additional metadata.
    :ivar foreign_country_codes: A dictionary mapping foreign country codes to a default unmatched value.
    :ivar additional_foreign_indicators: A dictionary mapping foreign indicators to a default unmatched value.
    :ivar false_positives: A set of normalized names that should not be matched, even if they have high similarity scores.
    :ivar common_mistakes: A dictionary mapping common input mistakes to corrected forms for improved matching.
    :ivar exact_matches: A dictionary of exact matches based on normalized names without brackets.
    :ivar tfidf: A TF-IDF vectorizer for calculating text similarity.
    :ivar tfidf_matrix: A sparse matrix containing the TF-IDF features of official municipality names.
    :ivar ngram_index: A dictionary-based index for efficient candidate retrieval using n-grams.
    """
    def __init__(self, config: dict, start_date: str = "01-01-2010"):
        """
        Initialize with official municipality data.

        :param official_municipalities: DataFrame with columns containing municipality data.
        :type official_municipalities: pd.DataFrame
        :param id_col: The column name for municipality IDs in the DataFrame.
        :type id_col: str
        :param name_col: The column name for municipality names in the DataFrame.
        :type name_col: str
        :param config: Configuration dictionary for additional rules and corrections.
        :type config: dict
        """
        super().__init__(which_data='name_matching', start_date=start_date)

        self.foreign_country_codes = {
            code.lower(): (-1, None, 1.0)
            for code in config.get('foreign_country_codes', [])
        }
        self.additional_foreign_indicators = {
            self.normalize_text(name): (-1, None, 1.0)
            for name in config.get('additional_foreign_indicators', [])
        }
        self.false_positives = {
            self.normalize_text(name) for name in config.get('false_positives', [])
        }
        self.common_mistakes = {
            self.normalize_text(k): self.normalize_text(v)
            for k, v in config.get('common_mistakes', {}).items()
        }

        self.preprocess_officials()

    def preprocess_officials(self) -> None:
        """
        Preprocess official municipality names for better matching.

        This method normalizes official municipality names, prepares
        confidence scores, creates lookup dictionaries for exact
        matches, and builds a TF-IDF matrix and n-gram index for
        efficient name matching.

        :return: None
        """
        # Normalize official municipality names
        self.officials['normalized'] = self.officials['matched_name'].apply(
            self.normalize_text
        )
        self.officials['no_brackets'] = (
            self.officials['normalized'].str.replace(
                r'\(|\)', '', regex=True)
        )
        self.officials['confidence'] = 1.0

        # Create exact match lookup dictionaries
        self.exact_matches = dict(
            zip(
                self.officials['no_brackets'],
                zip(
                    self.officials['matched_id'],
                    self.officials['matched_name'],
                    self.officials['confidence'],
                ),
            )
        )

        # Create TF-IDF matrix for official names
        self.tfidf = TfidfVectorizer(analyzer='char', ngram_range=(2, 3))
        self.tfidf_matrix = self.tfidf.fit_transform(self.officials['normalized'])

        # Create ngram index
        self.ngram_index = self.create_ngram_index(self.officials['normalized'])

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize text to enhance matching accuracy.

        This method handles issues like missing brackets, extra spaces,
        special characters, and accents. It also replaces abbreviations
        and German umlauts for better text matching.

        :param text: The text to normalize.
        :type text: str
        :return: Normalized text.
        :rtype: str
        """
        # Handle mismatched brackets by appending a closing bracket
        while text.count('(') > text.count(')'):
            text += ')'

        # Add whitespace before '(' if missing
        text = re.sub(r'(?<!\s)\(', ' (', text)

        # Replace standalone uppercase abbreviations (e.g., "ZH" -> "(ZH)")
        text = re.sub(r'\b([A-Z]{2})\b', r'(\1)', text)

        # Simplify nested brackets ((ZH)) -> (ZH)
        text = re.sub(r'\((\([A-Z]{2}\))\)', r'\1', text)

        # Remove leading "à" and trailing punctuation or unwanted characters
        text = re.sub(r'^à\s+', '', text)
        text = re.sub(r'[^)\w\s]+$', '', text)

        # Remove trailing "b"
        text = re.sub(r'\s+b$', '', text)

        # Convert to lowercase
        text = text.lower()

        # Replace German umlauts
        text = text.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')

        # Remove accents and special characters except those handled above
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )

        # Replace abbreviations and hyphens with full forms or spaces
        replacements = {
            ' b.': ' bei',
            's.': 'san',
            ' v. d.': ' von der',
            ' a. d.': ' an der',
            ' a.': ' am',
            ' u.': ' und',
            ' z.': ' zur',
            'st-': 'saint-',
            'dev-': 'devant-',
            ' avec': '',
            "'": " ",
            "-": " ",
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        # Remove any remaining non-English letters and special characters
        text = re.sub(r'[^a-z\s()]', '', text)

        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    @staticmethod
    def create_ngram_index(texts: list[str], n: int = 3) -> dict[str, set]:
        """
        Create an n-gram index for faster candidate filtering.

        :param texts: List of normalized texts.
        :type texts: List[str]
        :param n: The n-gram size.
        :type n: int
        :return: An n-gram index mapping n-grams to text indices.
        :rtype: Dict[str, set]
        """
        index = defaultdict(set)
        for idx, text in enumerate(texts):
            ngrams = {text[i:i+n] for i in range(len(text)-n+1)}
            for ngram in ngrams:
                index[ngram].add(idx)
        return index

    def get_candidates(
        self,
        query_normalized: str,
        threshold: float = 0.3
    ) -> pd.DataFrame:
        """
        This method identifies potential matches for a query string
        by filtering official municipality names based on shared n-grams
        and a similarity threshold.

        :param query_normalized: The normalized query string.
        :type query_normalized: str
        :param threshold: Minimum similarity score to include a candidate.
        :type threshold: float
        :return: A DataFrame of candidate matches.
        :rtype: pd.DataFrame
        """
        # Generate n-grams from the normalized query string
        query_ngrams = {
            query_normalized[i:i + 3] for i in range(len(query_normalized) - 2)
        }

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

    def match_name(
        self,
        query_normalized: str,
        threshold: float = 0.85
    ) -> tuple[Union[int, None], Union[str, None], float]:
        """
        Match a single name against official municipalities.

        This method attempts to find an exact or fuzzy match for the
        given query string using techniques like TF-IDF similarity,
        Levenshtein ratio, and others.

        :param query_normalized: The normalized query string.
        :type query_normalized: str
        :param threshold: Minimum confidence score to accept a match.
        :type threshold: float
        :return: Tuple containing matched ID, matched name, and confidence score.
        :rtype: Tuple[Union[int, None], Union[str, None], float]
        """
        # Fix common mistakes
        query_normalized = self.common_mistakes.get(query_normalized, query_normalized)

        # Try to find an exact match if the query contains brackets
        matches = re.findall(r'\((.*?)\)', query_normalized)
        if matches:
            for match in matches:
                stripped_match = match.replace('(', '').replace(')', '')
                for lookup in [
                    self.foreign_country_codes,
                    self.additional_foreign_indicators,
                    self.exact_matches
                ]:
                    result = lookup.get(stripped_match)
                    if result:
                        return result

            # Check the remaining part outside the brackets for an exact match
            remaining_part = re.sub(r'\(.*?\)', '', query_normalized).strip()
            result = self.exact_matches.get(remaining_part)
            if result:
                return result

        # Perform fuzzy matching if no exact match is found
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
            lev_ratio = (
                fuzz.ratio(query_normalized, candidate['normalized']) / 100
            )
            # Partial ratio
            part_ratio = (
                fuzz.partial_ratio(query_normalized, candidate['normalized']) / 100
            )
            # Token sort ratio (handles word reordering)
            token_sort = (
                fuzz.token_sort_ratio(query_normalized, candidate['normalized']) / 100
            )
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

        # Find the best match
        best_match = max(scores, key=lambda x: x[2])

        # Handle matches below the threshold
        if best_match[2] < threshold:
            # Check if query matches common patterns for unmatched cases
            unmatched_patterns = [
                r'\b\(fr\)\b', r'\b\(lu\)\b', r'\b\(be\)\b', r'\b\(gr\)\b',
                r'\b\(ar\)\b', r'\b\(ge\)\b', r'\b\(sg\)\b', r'\bfra\b', r'\bdeu\b'
            ]
            if re.match('|'.join(unmatched_patterns), query_normalized):
                return -1, None, 1.0
            return 0, None, 0.0

        # Handle false positives
        if best_match[1] in self.false_positives:
            return -1, None, 1.0

        return best_match

    def match_dataframe(
        self,
        query_df: pd.DataFrame,
        query_column: str,
        threshold: float = 0.85
    ) -> pd.DataFrame:
        """
        Match multiple names in a DataFrame against official municipalities.

        This method normalizes input names, performs exact matches, and applies
        advanced fuzzy matching for unmatched entries. It returns a DataFrame
        with matched IDs, names, and confidence scores.

        :param query_df: DataFrame containing the names to be matched.
        :type query_df: pd.DataFrame
        :param query_column: The column name containing query strings.
        :type query_column: str
        :param threshold: Minimum confidence score to accept a match.
        :type threshold: float
        :return: DataFrame with matched IDs, names, and confidence scores.
        :rtype: pd.DataFrame
        """
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
            return pd.Series({
                'matched_id': match[0],
                'matched_name': match[1],
                'confidence': match[2]
            })

        tqdm.pandas(desc=f'Matching {len(no_matches)} names')
        no_matches[['matched_id', 'matched_name', 'confidence']] = (
            no_matches.progress_apply(match_apply, axis=1)
        )

        return pd.concat([exact_matches, no_matches], axis=0)
