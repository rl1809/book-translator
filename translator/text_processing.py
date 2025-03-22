import re
import unicodedata

import jieba
from typing import List, Tuple
import string
from deep_translator import GoogleTranslator

REPLACEMENTS = {
    "chị rể": "anh rể",
    "ngoại bà": "bà ngoại",
}

IGNORE_PREFIX = [
    "https://",
]

IGNORE_WORDS_IN_TRANSLATION = [
    'BẢN DỊCH',
    'NỘI DUNG ĐOẠN VĂN'
]


def preprocess_downloaded_text(text: str) -> str:
    """
    Normalizes line spacing in a chapter file and:
    1. Removes lines with ignored prefixes
    2. Removes lines containing Vietnamese text
    3. Removes all lines after and including any line containing "ps:"
    """
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Process lines with Vietnamese and "ps:" detection
    cleaned_lines = []
    for line in text.splitlines():
        if "ps" in line.lower():
            break

        # Skip lines with ignored prefixes
        if any(prefix in line for prefix in IGNORE_PREFIX):
            continue

        # Skip lines containing Vietnamese characters
        if contains_vietnamese(line):
            continue

        cleaned_lines.append(line)

    # Join the remaining lines
    return "\n".join(cleaned_lines)


def contains_vietnamese(text: str) -> bool:
    """
    Detects if a string contains Vietnamese characters.
    Vietnamese uses characters in ranges U+00C0-U+00FF (Latin-1 Supplement with diacritical marks)
    and U+0102-U+0103, U+0110-U+0111, U+0128-U+0129, U+0168-U+0169, U+01A0-U+01A3, U+01AF-U+01B0, U+1EA0-U+1EF9 (Vietnamese-specific)
    """
    # Check for Vietnamese-specific Unicode character ranges
    vietnamese_pattern = re.compile(
        r'[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýÿ]|[ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝ]|[ăâđêôơưĂÂĐÊÔƠƯ]')
    return bool(vietnamese_pattern.search(text))



def detect_untranslated_chinese(text: str) -> Tuple[bool, float]:
    """Detects Chinese characters, returns if present and ratio."""
    chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
    total_chars = len(text)
    ratio = (len(chinese_chars) / total_chars) * 100 if total_chars > 0 else 0
    return (len(chinese_chars) > 0), ratio


def extract_potential_names(words: List[str]) -> List[List[str]]:
    """Extract sequences of potential name parts (capitalized words)."""
    potential_names: List[List[str]] = []
    current_name: List[str] = []
    for word in words:
        if is_potential_name_part(word):
            current_name.append(word)
        else:
            if current_name:
                potential_names.append(current_name)
                current_name = []
    if current_name:
        potential_names.append(current_name)
    return potential_names


def is_potential_name_part(word: str) -> bool:
    """Check if a word could be part of a name (Capitalized, alphabetic)."""
    return word[0].isalpha() and word[0].isupper() if word else False


def clean_name_string(name_joined: str) -> str:
    """Remove punctuation and special chars from name strings for consistency."""
    allowed_punct = "-'"
    punct_to_remove = ''.join([p for p in string.punctuation if p not in allowed_punct])
    return name_joined.translate(str.maketrans('', '', punct_to_remove)).strip()


def is_valid_name(name: List[str]) -> bool:
    """Validate name length and basic punctuation rules."""
    if not (2 <= len(name) <= 4):
        return False
    name_joined = "".join(name)
    cleaned_name = clean_name_string(name_joined)
    if cleaned_name != name_joined.translate(str.maketrans('', '', string.punctuation)):
        return False
    return True


def get_unique_names_from_text(text: str) -> dict[str, int]:
    """Extract unique names, count occurrences, return dict."""
    name_counts: dict[str, int] = {}
    words = text.split()
    potential_names_list = extract_potential_names(words)

    for name_parts in potential_names_list:
        if is_valid_name(name_parts):
            cleaned_name = clean_name_string(" ".join(name_parts))
            name_counts[cleaned_name] = name_counts.get(cleaned_name, 0) + 1
    return name_counts



def split_text_into_chunks(text: str, chunk_size: int) -> List[str]:
    """Split text into chunks of max chunk_size, trying to respect line breaks."""
    chunks: List[str] = []
    current_chunk = ""

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        if len(line) > chunk_size:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = ""
            for i in range(0, len(line), chunk_size):
                chunks.append(line[i:i + chunk_size])
            continue

        separator = "\n" if current_chunk else ""
        if len(current_chunk) + len(separator) + len(line) <= chunk_size:
            current_chunk += separator + line
        else:
            chunks.append(current_chunk)
            current_chunk = line

    if current_chunk:
        chunks.append(current_chunk)
    return chunks

def normalize_translation(translation_content: str) -> str:
    """Normalizes line spacing and applies replacements in a chapter file."""
    lines = translation_content.splitlines()
    normalized_lines = []

    for line in lines:
        stripped_line = line.strip()
        if not stripped_line:
            continue  # Skip empty lines

        has_ignore_words = False
        for word in IGNORE_WORDS_IN_TRANSLATION:
            if word in stripped_line:
                has_ignore_words = True
                break

        if has_ignore_words:
            continue

        if all(c == '*' for c in stripped_line):
            normalized_lines.append(stripped_line)
            continue

        # Normalize spaces and underscores
        processed_line = stripped_line.replace('_', ' ')
        processed_line = re.sub(r'\s{2,}', ' ', processed_line)
        processed_line = processed_line.replace('**', '')

        # Apply each replacement rule
        for pattern, replacement in REPLACEMENTS.items():
            regex = re.compile(re.escape(pattern), flags=re.IGNORECASE)
            processed_line = regex.sub(
                lambda match: replacement[0].upper() + replacement[1:]
                if match.group()[0].isupper()
                else replacement,
                processed_line
            )

        normalized_lines.append(processed_line)

    return "\n\n".join(normalized_lines)

def tokenize_chinese_text(text):
    """
    Tokenizes Chinese text using the jieba library.

    Args:
        text: The Chinese text string to be tokenized.

    Returns:
        A list of tokens (words).  Returns an empty list if input is invalid.
    """
    if not isinstance(text, str):
        print("Error: Input must be a string.")
        return []  # Or raise a TypeError, depending on your needs

    seg_list = jieba.cut(text)  # Use jieba.cut for tokenization
    return list(seg_list)  # Convert the generator to a list


def add_underscore(text, is_chinese=True):
    if detect_underscore(text):
        return text
    lines = text.splitlines()
    normalized_lines = []
    for line in lines:
        line = line.strip()
        if is_chinese:
            normalized_lines.append('_'.join(tokenize_chinese_text(line)))
        else:
            normalized_lines.append('_'.join(line.split(" ")))
    return "\n".join(normalized_lines)


def detect_underscore(text):
    lines = text.splitlines()
    underscore_pattern = re.compile(r'_\w+_')

    for line in lines:
        if underscore_pattern.search(line):
            return True

    return False

def remove_underscore(text: str) -> str:
    """Normalizes line spacing in a chapter file."""
    lines = text.splitlines()
    normalized_lines = []
    for line in lines:
        line = line.replace('_', '')
        normalized_lines.append(line)

    return "\n".join(normalized_lines)


def translate_long_text(text: str, src: str, dest: str, chunk_size: int = 1024) -> str:
    """
    Splits the input text into chunks, translates each chunk synchronously,
    and then combines the translations.
    """
    chunks = split_text_into_chunks(text, chunk_size)
    translator = GoogleTranslator(source=src, target=dest)
    translated_chunks = []
    for chunk in chunks:
        translated = translator.translate(chunk)
        translated_chunks.append(translated)
    return "\n".join(translated_chunks)



def normalize_unicode_text(text: str) -> str:
    """
    Normalizes Unicode text to Normalization Form Canonical Composition (NFC).
    """
    return unicodedata.normalize('NFC', text)

def extract_chinese_words_from_text(text: str) -> List[str]:
    """
    Extracts all Chinese words or phrases from the given text.
    
    Args:
        text: The text to extract Chinese words from.
        
    Returns:
        A list of all Chinese words or phrases found in the text.
    """
    # Regular expression to find consecutive Chinese characters
    chinese_word_pattern = re.compile(r'[\u4e00-\u9fff]+')
    
    # Find all matches in the text
    chinese_words = chinese_word_pattern.findall(text)
    
    return chinese_words

def replace_chinese_words_with_vietnamese(text: str, chinese_vietnamese_map: dict[str, str]) -> str:
    """
    Replaces Chinese words in text with their Vietnamese translations.
    
    Processes the mapping from longest keys to shortest to avoid partial replacements.
    
    Args:
        text: The text containing Chinese words to be replaced
        chinese_vietnamese_map: Dictionary mapping Chinese words to Vietnamese translations
        
    Returns:
        Text with Chinese words replaced by their Vietnamese translations
    """
    if not text or not chinese_vietnamese_map:
        return text

    # Sort dictionary keys by length (longest first) to avoid partial replacements
    sorted_keys = sorted(chinese_vietnamese_map.keys(), key=len, reverse=True)

    for chinese_word in sorted_keys:
        vietnamese_translation = chinese_vietnamese_map.get(chinese_word)
        if vietnamese_translation:
            # Add spaces around the Vietnamese translation
            padded_translation = f" {vietnamese_translation} "
            text = text.replace(chinese_word, padded_translation)

    text = re.sub(r' +', ' ', text)
    text = text.strip()


    return text
