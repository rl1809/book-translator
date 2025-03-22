import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
import re
import fcntl
import time
from threading import Lock
import os

from config.models import GEMINI_PRO_MODEL_CONFIG
from config.prompts import PromptStyle
from epub.generator import EPUBGenerator
from logger import logging_utils
from translator import text_processing
from config import settings

from translator.helper import is_in_chapter_range, sanitize_path_name
from translator.text_processing import get_unique_names_from_text, add_underscore, extract_chinese_words_from_text, \
    replace_chinese_words_with_vietnamese


class FileHandler:
    """Handles file operations: creation, loading, saving, path management."""

    def __init__(self, book_dir: Path, start_chapter: Optional[int], end_chapter: Optional[int]):
        self.book_dir = book_dir
        self._ensure_directory_structure()
        self._progress_lock = Lock()  # Lock for progress file operations


    def _ensure_directory_structure(self) -> None:
        """Create necessary directories if they don't exist."""
        for key in ["prompt_files", "translation_responses", "translated_chapters", "epub"]:
            dir_path = self.get_path(key)
            dir_path.mkdir(parents=True, exist_ok=True)


    def get_progress_path(self) -> Path:
        return self.book_dir / f"progress.json"


    def get_path(self, key: str) -> Path:
        """Retrieve a path object for a given key."""
        return self.book_dir.joinpath(key)


    def list_files_in_dir(self, dir_path: Path, pattern: str = '*') -> List[Path]:
        """List files in a subdirectory based on a glob pattern."""
        if not dir_path.is_dir():
            logging.warning(f"Directory not found: {dir_path}")
            return []
        return list(dir_path.glob(pattern))


    def delete_file(self, filename: str, sub_dir_key: str) -> bool:
        """Delete a specific file, return True if successful, False otherwise."""
        file_path = self.get_path(sub_dir_key) / filename
        if file_path.exists() and file_path.is_file():
            try:
                file_path.unlink() # More modern and Pathlib-centric way to delete
                logging.info(f"Deleted file: {filename}")
                return True
            except Exception as e:
                logging_utils.log_exception(e, f"Error deleting file: {file_path}")
                return False
        else:
            logging.warning(f"File not found, cannot delete: {file_path}")
            return False


    def _initiate_progress(self) -> Dict:
        """Initialize a new progress dictionary."""
        return {
            "last_batch_time": 0,
            "last_batch_size": 0,
            "failed_translations": {}
        }


    def _safe_read_json(self, file_path: Path) -> Optional[Dict]:
        """Safely read a JSON file with file locking."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Acquire a shared (read) lock
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    return json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            logging.error(f"Error reading JSON file {file_path}: {e}")
            return None


    def _safe_write_json(self, file_path: Path, data: Dict) -> bool:
        """Safely write a JSON file with file locking and atomic write."""
        temp_path = file_path.with_suffix('.json.tmp')
        try:
            # Write to temporary file first
            with open(temp_path, 'w', encoding='utf-8') as f:
                # Acquire an exclusive (write) lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, f, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            # Atomic rename
            temp_path.replace(file_path)
            return True
        except Exception as e:
            logging.error(f"Error writing JSON file {file_path}: {e}")
            if temp_path.exists():
                temp_path.unlink()
            return False


    def load_progress(self) -> Dict:
        """Load and return progress data from progress.json, initialize if not exists."""
        progress_file_path = self.get_progress_path()
        
        with self._progress_lock:
            try:
                # Try to read existing progress file
                data = self._safe_read_json(progress_file_path)
                if data is not None:
                    return data

                # If file doesn't exist or is corrupt, initialize new progress
                logging.info("Progress file not found or corrupt, initializing new progress.")
                new_progress = self._initiate_progress()
                self._safe_write_json(progress_file_path, new_progress)
                return new_progress

            except Exception as e:
                logging_utils.log_exception(e, "Error loading progress file.")
                new_progress = self._initiate_progress()
                self._safe_write_json(progress_file_path, new_progress)
                return new_progress


    def save_progress(self, progress_data: Dict) -> None:
        """Save progress data to progress.json with proper locking."""
        progress_file_path = self.get_progress_path()
        
        with self._progress_lock:
            try:
                # Ensure the data is valid before saving
                if not isinstance(progress_data, dict):
                    raise ValueError("Progress data must be a dictionary")
                
                # Ensure required fields exist
                if "last_batch_time" not in progress_data:
                    progress_data["last_batch_time"] = 0
                if "last_batch_size" not in progress_data:
                    progress_data["last_batch_size"] = 0
                if "failed_translations" not in progress_data:
                    progress_data["failed_translations"] = {}

                # Save with retries
                max_retries = 3
                for attempt in range(max_retries):
                    if self._safe_write_json(progress_file_path, progress_data):
                        return
                    if attempt < max_retries - 1:
                        time.sleep(0.1)  # Small delay before retry
                
                raise Exception("Failed to save progress after multiple attempts")
                
            except Exception as e:
                logging_utils.log_exception(e, "Error saving progress to file.")
                # Try to recover by saving a minimal valid progress file
                try:
                    self._safe_write_json(progress_file_path, self._initiate_progress())
                except Exception as recover_error:
                    logging.error(f"Failed to recover progress file: {recover_error}")

    def save_content_to_file(self, content: str, filename: str, sub_dir_key: str) -> Path:
        """Save content to a file within a specified subdirectory, return Path."""
        file_path = self.get_path(sub_dir_key) / filename
        try:
            file_path.write_text(content, encoding='utf-8')
            logging.debug(f"File saved: {file_path}") # Debug level logging
            return file_path
        except Exception as e:
            logging_utils.log_exception(e, f"Error saving file: {file_path}")
            raise # Re-raise exception after logging


    def load_content_from_file(self, filename: str, sub_dir_key: str) -> Optional[str]:
        """Load content from a file, return None if file not found or error."""
        file_path = self.get_path(sub_dir_key) / filename
        try:
            return file_path.read_text(encoding='utf-8')
        except FileNotFoundError:
            logging.warning(f"File not found: {file_path}") # Warning, not error
            return None
        except Exception as e:
            logging_utils.log_exception(e, f"Error reading file: {file_path}")
            return None


    def is_translation_complete(self, start_chapter: Optional[int] = None, end_chapter: Optional[int] = None) -> bool:
        """Check if all expected translations in specified chapter range are completed.
        A translation is considered complete if:
        1. It has a corresponding file in translation_responses directory OR
        2. It is marked as a failed translation AND has been retried
        """
        prompts_dir = self.get_path("prompt_files")
        responses_dir = self.get_path("translation_responses")

        # Generate display strings for logging
        start_str = str(start_chapter) if start_chapter is not None else 'begin'
        end_str = str(end_chapter) if end_chapter is not None else 'end'

        # Get filtered prompts and responses
        prompt_files = {
            p.stem for p in prompts_dir.glob("*.txt")
            if is_in_chapter_range(p.name, start_chapter, end_chapter)
        }

        response_files = {
            r.stem
            for r in responses_dir.glob("*.txt")
            if is_in_chapter_range(r.name, start_chapter, end_chapter)
        }

        # Load progress data to check for failed translations
        progress_data = self.load_progress()
        failed_translations = progress_data.get("failed_translations", {})
        

        untranslated_prompts = set()
        for prompt_file in prompt_files:
            if prompt_file not in response_files:
                untranslated_prompts.add(prompt_file)
            else:
                failure_info = failed_translations.get(f"{prompt_file}.txt")
                if failure_info and not failure_info.get("retried", False):
                    untranslated_prompts.add(prompt_file)
        
        if untranslated_prompts:
            logging.info(f"Remaining translations in chapters {start_str}-{end_str}: {len(untranslated_prompts)}")
            return False

        logging.info(f"All translations completed for chapters {start_str}-{end_str}")
        return True


    def combine_chapter_translations(self, start_chapter: Optional[int] = None, end_chapter: Optional[int] = None) -> None:
        """Combines translated prompt files for each chapter (in book_dir)."""
        translated_responses_dir = self.get_path("translation_responses")
        translated_chapters_dir = self.get_path("translated_chapters")

        response_files = [
            p for p in translated_responses_dir.glob("*.txt")
            if is_in_chapter_range(p.name, start_chapter, end_chapter)
        ]

        chapter_files = {}
        for filename in response_files:
            match = re.match(r"(.*)_\d+\.txt", filename.name)
            if match:
                chapter_name = match.group(1)
                if chapter_name not in chapter_files:
                    chapter_files[chapter_name] = []
                chapter_files[chapter_name].append(filename)

        # Combine files for each chapter
        for chapter_name in sorted(chapter_files.keys()):
            # Optionally, sort the files for each chapter
            files = sorted(chapter_files[chapter_name])

            # Use the translated_chapters_dir that we got earlier
            output_path = translated_chapters_dir / f"{chapter_name}.txt"
            try:
                with open(output_path, "w", encoding="utf-8") as outfile:
                    for file_path in files:
                        try:
                            # file_path is already a Path object
                            with open(file_path, "r", encoding="utf-8") as infile:
                                content = infile.read()
                                outfile.write(content + "\n")  # Add newline between prompts
                        except Exception as e:
                            logging.error(f"Error reading file {file_path}: {e}")
                    logging.info(f"Combined chapter translation: {chapter_name}")
            except OSError as e:
                logging.error(f"Error writing to {output_path}: {e}")

        logging.info("Combine chapter translations complete")

    def create_prompt_files_from_chapters(self, start_chapter: Optional[int] = None,
                                          end_chapter: Optional[int] = None) -> None:
        """Create prompt files from downloaded chapters, but only for chapters without existing prompt files."""
        download_dir = self.get_path("input_chapters")
        prompt_dir = self.get_path("prompt_files")
        prompt_count = 0
        new_chapter_count = 0

        chapter_files = [
            p for p in download_dir.glob("*.txt")
            if is_in_chapter_range(p.name, start_chapter, end_chapter)
        ]
        if not chapter_files:
            logging.warning(f"No chapter files found in: {download_dir}")
            return

        # Get existing prompt file prefixes (chapter names)
        existing_prompts = set()
        for prompt_file in prompt_dir.glob("*.txt"):
            # Extract chapter name from prompt filename (e.g., "chapter_0001_1.txt" -> "chapter_0001")
            match = re.match(r"(.*)_\d+\.txt", prompt_file.name)
            if match:
                existing_prompts.add(match.group(1))

        for chapter_file in chapter_files:
            # Skip if this chapter already has prompt files
            if chapter_file.stem in existing_prompts:
                logging.debug(f"Skipping {chapter_file.stem} - prompt files already exist")
                continue

            chapter_text = self.load_content_from_file(chapter_file.name, "input_chapters")
            if chapter_text:
                new_chapter_count += 1
                prompts = text_processing.split_text_into_chunks(chapter_text, settings.MAX_TOKENS_PER_PROMPT)
                for idx, prompt_text in enumerate(prompts):
                    prompt_filename = f"{chapter_file.stem}_{idx + 1}.txt"
                    self.save_content_to_file(add_underscore(prompt_text), prompt_filename, "prompt_files")
                    prompt_count += 1

        if new_chapter_count > 0:
            logging.info(f"Created {prompt_count} prompt files from {new_chapter_count} new chapters.")
        else:
            logging.info("No new chapters to process - all chapters already have prompt files.")
        return


    def load_prompt_file_content(self, prompt_filename: str) -> Optional[str]:
        """Load content of a prompt file, return None if not found."""
        return self.load_content_from_file(prompt_filename, "prompt_files")


    def delete_invalid_translations(self) -> int:
        """Deletes very short translation files, likely errors, returns count deleted."""
        responses_dir = self.get_path("translation_responses")
        deleted_count = 0
        files_to_check = self.list_files_in_dir(responses_dir, "*.txt")

        for file_path in files_to_check:
            try:
                content = self.load_content_from_file(file_path.name, "translation_responses")
                if content is None:  # File couldn't be read
                    if self.delete_file(file_path.name, "translation_responses"):
                        deleted_count += 1
                        logging.warning(f"Deleted unreadable translation: {file_path.name}")
                    continue
                if '[TRANSLATION FAILED]' in content:
                    continue
                
                original_content = self.load_content_from_file(file_path.name, "prompt_files")
                if original_content is None:  # No corresponding prompt file
                    if self.delete_file(file_path.name, "translation_responses"):
                        deleted_count += 1
                        logging.warning(f"Deleted translation with no prompt: {file_path.name}")
                    continue
                    
                reasons = []

                # Check 1: Short content (<=1 line)
                if len(content.splitlines()) <= 1 < len(original_content.splitlines()):
                    reasons.append("Short content")

                # Check 2: Repeated words (20+ consecutive repeats)
                if re.search(r'(\b\w+\b)(\W+\1){20,}', content, flags=re.IGNORECASE):
                    reasons.append("Repeated words")

                # Check 3: Repeated special characters (100+ consecutive)
                if re.search(r'[_\-=]{100,}', content):
                    reasons.append("Repeated special characters")
                    
                # Check 4: Very low content to prompt ratio
                if len(content) < len(original_content) * 0.3 and len(content.splitlines()) < len(original_content.splitlines()) * 0.5:
                    reasons.append("Suspicious length ratio")
                    
                # Check 5: Incomplete translation (ending mid-sentence)
                last_char = content.rstrip()[-1:] if content.rstrip() else ''
                if last_char and last_char not in '.!?。…':
                    # If the file ends without proper sentence termination AND is shorter than original
                    if len(content) < len(original_content) * 0.9:
                        reasons.append("Incomplete translation")

                if reasons:
                    if self.delete_file(file_path.name, "translation_responses"):
                        deleted_count += 1
                        logging.warning(f"Deleted likely invalid translation: {file_path.name} (Reasons: {', '.join(reasons)}).")
            except Exception as e:
                logging.error(f"Error checking translation file {file_path.name}: {str(e)}")
                # If we can't process the file, it's safer to delete it
                if self.delete_file(file_path.name, "translation_responses"):
                    deleted_count += 1
                    logging.warning(f"Deleted problematic translation file: {file_path.name}")
                
        if deleted_count > 0:
            logging.info(f"Deleted {deleted_count} potentially invalid translation files.")
        else:
            logging.info("No invalid translation files found.")
        return deleted_count

    def _process_text_file_for_names(self, filepath: Path) -> Optional[dict[str, int]]:
        """Process text file, extract names, handle encoding, return counts."""
        try:
            text = filepath.read_text(encoding='utf-8')
            return get_unique_names_from_text(text)
        except FileNotFoundError:
            logging.error(f"File not found: {filepath}")
            return None
        except Exception as e:
            logging_utils.log_exception(e, f"Error processing file for names: {filepath}")
            return None


    def _count_names_in_directory(self, directory_path: Path) -> dict[str, int]:
        """Process all translated text files in directory, aggregate name counts."""
        all_name_counts: dict[str, int] = {}
        if not directory_path.is_dir():
            logging.warning(f"Directory not found: {directory_path}")
            return all_name_counts

        for filepath in directory_path.glob('*.txt'):
            name_counts = self._process_text_file_for_names(filepath)
            if name_counts:
                for name, count in name_counts.items():
                    all_name_counts[name] = all_name_counts.get(name, 0) + count
        return {key: value for key, value in all_name_counts.items() if value>=20}


    def extract_and_count_names(self) -> None:
        """Orchestrates name extraction, counting, and saving."""
        logging.info("Starting name extraction and counting...")
        responses_dir = self.book_dir / "translation_responses"
        aggregated_names = self._count_names_in_directory(responses_dir)

        if aggregated_names:
            sorted_names = sorted(aggregated_names.items(), key=lambda item: item[1], reverse=True)
            sorted_names_dict = dict(sorted_names)
            output_path = self.book_dir / "names.json"
            try:
                output_path.write_text(json.dumps(sorted_names_dict, indent=4, ensure_ascii=False), encoding='utf-8')
                logging.info(f"Saved sorted names to: {output_path}")
            except Exception as e:
                logging_utils.log_exception(e, f"Error saving sorted names to JSON: {output_path}")
        else:
            logging.warning("No names found to extract.")


    def load_and_convert_names_to_string(self) -> Optional[str]:
        """Load names from JSON, format to string for instructions."""
        json_path = self.book_dir / "names.json"
        try:
            data = json.loads(json_path.read_text(encoding='utf-8'))
            if not isinstance(data, dict):
                logging.error(f"JSON file does not contain a dictionary: {json_path}")
                return None
            output_string = ""
            for name, count in data.items():
                output_string += f"{name} - {count}\n"
            return output_string
        except FileNotFoundError:
            logging.debug(f"File not found: {json_path}")
            return None
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON format in file: {json_path}")
            return None
        except Exception as e:
            logging_utils.log_exception(e, f"Error loading or converting names from JSON: {json_path}")
            return None

    def extract_chinese_words_to_file(self) -> [bool, Optional[Path]]:
        """
        Extract all Chinese words from all translation response files and save them to a JSON file.
            
        Returns:
            Path to the created Chinese words file, or None on failure
        """
        logging.info("Extracting Chinese words from translation responses...")


        translation_dir = self.get_path("translation_responses")
        translation_files = list(translation_dir.glob("*.txt"))

        if not translation_files:
            logging.warning("No translation files found for Chinese word extraction.")
            return None
            
        chinese_words_set = set()
        for file_path in translation_files:
            try:
                content = self.load_content_from_file(
                    file_path.name,
                    "translation_responses" if file_path in translation_files else "translated_chapters",
                )
                if content:
                    chinese_words = extract_chinese_words_from_text(content)
                    for word in chinese_words:
                        chinese_words_set.add(word)
            except Exception as e:
                logging.error(f"Error extracting Chinese words from {file_path.name}: {str(e)}")

        if not chinese_words_set:
            logging.info("No Chinese words found in translation files.")
            return False, None
        
        output_filepath = self.book_dir / "chinese_words.json"

        translated_text = ""
        try:
            from translator.core import TranslationManager
            translator = TranslationManager(model_config=GEMINI_PRO_MODEL_CONFIG)
            raw_text = '\n'.join(chinese_words_set)
            translated_text = translator.translate_text(
                text=raw_text,
                prompt_style=PromptStyle.Words,
            )
            translated_text = translated_text.replace("```", "").replace("json", "").strip()

            json_result = json.loads(translated_text)

            with open(output_filepath, "w", encoding="utf-8") as outfile:
                json.dump(json_result, outfile, ensure_ascii=False, indent=2)

            logging.info(f"Chinese words extracted and translated to: {output_filepath}")
            return True, output_filepath

        except json.JSONDecodeError:
            logging.error(f"Failed to parse JSON from translation response: {translated_text}")

        except Exception as e:
            logging.error(f"Error translating batch of Chinese words: {str(e)}")
            
        return False, None

    def replace_chinese_words_in_chapters(self, has_chinese: bool) -> int:
        """
        Replace Chinese words in all translated chapter files with their Vietnamese translations.
        Uses the previously created chinese_words.json mapping.
        
        Returns:
            Number of files processed
        """

        if not has_chinese:
            logging.warning("No Chinese words in translation. Skipping replacement.")
            return 0

        chinese_words_file = self.book_dir / "chinese_words.json"
        if not chinese_words_file.exists():
            logging.warning("Chinese words mapping file not found. Skipping replacement.")
            return 0
            
        try:
            # Load the Chinese-Vietnamese mapping
            with open(chinese_words_file, "r", encoding="utf-8") as f:
                chinese_vietnamese_map = json.load(f)
                
            if not chinese_vietnamese_map:
                logging.warning("Chinese words mapping is empty. Skipping replacement.")
                return 0
                
            logging.info(f"Loaded {len(chinese_vietnamese_map)} Chinese-Vietnamese word mappings")
            
            # Process all chapter files
            translated_responses_dir = self.get_path("translation_responses")
            translated_files = list(translated_responses_dir.glob("*.txt"))
            
            if not translated_files:
                logging.warning("No translated response files found to process.")
                return 0
                
            files_processed = 0
            for file_path in translated_files:
                try:
                    # Read the file content
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        
                    # Replace Chinese words with Vietnamese translations
                    updated_content = replace_chinese_words_with_vietnamese(content, chinese_vietnamese_map)
                    
                    # Write back the updated content
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(updated_content)
                        
                    files_processed += 1
                    
                except Exception as e:
                    logging.error(f"Error processing file {file_path.name}: {str(e)}")
                    
            logging.info(f"Replaced Chinese words in {files_processed} translated response files")
            return files_processed
            
        except Exception as e:
            logging.error(f"Error during Chinese word replacement: {str(e)}")
            return 0

    def generate_epub(self, book_title: str, book_author: str, cover_image: str) -> Optional[Path]:
        """Generate EPUB from combined translations, return path to EPUB or None on failure."""
        translated_chapters_dir = self.get_path("translated_chapters")
        chapter_files = sorted(translated_chapters_dir.glob("*.txt"))

        if not chapter_files:
            logging.warning("No translated files found to create EPUB.")
            return

        book_title = sanitize_path_name(book_title)
        output_filepath = self.get_path("epub") / f"{book_title}.epub"

        epub_generator = EPUBGenerator() # Instantiate generator
        try:
            epub_generator.create_epub_from_txt_files(
                chapter_files,
                title=book_title,
                author=book_author,
                cover_image=cover_image,
                output_filepath=output_filepath,
                language="vi",
                toc_title="Mục Lục"
            )
            logging.info(f"EPUB file created: {output_filepath}")
            return output_filepath
        except Exception as e:
            logging_utils.log_exception(e, "EPUB generation failed.")
            return None

    def get_chapter_status(self, start_chapter: Optional[int] = None, end_chapter: Optional[int] = None) -> Dict[
        str, Dict[str, any]]:

        prompts_dir = self.get_path("prompt_files")
        responses_dir = self.get_path("translation_responses")

        # Ensure directories exist
        prompts_dir.mkdir(parents=True, exist_ok=True)
        responses_dir.mkdir(parents=True, exist_ok=True)

        # Get all prompt files in the specified range
        prompt_files = [
            p for p in prompts_dir.glob("*.txt")
            if is_in_chapter_range(p.name, start_chapter, end_chapter)
        ]

        # Get all response files in the specified range
        response_files = [
            r for r in responses_dir.glob("*.txt")
            if is_in_chapter_range(r.name, start_chapter, end_chapter)
        ]

        # Group prompt files by chapter
        chapter_status = {}
        for file_path in prompt_files:
            match = re.match(r"(.*)_\d+\.txt", file_path.name)
            if match:
                chapter_name = match.group(1)
                if chapter_name not in chapter_status:
                    chapter_status[chapter_name] = {
                        "total_shards": 0,
                        "translated_shards": 0,
                        "failed_shards": 0,
                        "status": "Not Started",
                        "progress": 0.0,
                        "failed": False
                    }
                chapter_status[chapter_name]["total_shards"] += 1

        # First check progress.json for failed translations
        progress_data = self.load_progress()
        if "failed_translations" in progress_data:
            for filename, failure_info in progress_data["failed_translations"].items():
                match = re.match(r"(.*)_\d+\.txt", filename)
                if match:
                    chapter_name = match.group(1)
                    if chapter_name in chapter_status:
                        # Count as failed shard
                        chapter_status[chapter_name]["failed_shards"] += 1
                        chapter_status[chapter_name]["failed"] = True
                        # Store failure details
                        chapter_status[chapter_name]["failure_type"] = failure_info.get("failure_type", "generic")
                        chapter_status[chapter_name]["error"] = failure_info.get("error", "Unknown error")

        # Then count translated and failed shards from files
        for file_path in response_files:
            match = re.match(r"(.*)_\d+\.txt", file_path.name)
            if match:
                chapter_name = match.group(1)
                if chapter_name in chapter_status:
                    content = self.load_content_from_file(file_path.name, "translation_responses")
                    if content:
                        if "[TRANSLATION FAILED]" in content:
                            # Only count as failed if not already counted from progress.json
                            if not any(filename == file_path.name for filename in progress_data.get("failed_translations", {})):
                                chapter_status[chapter_name]["failed_shards"] += 1
                                chapter_status[chapter_name]["failed"] = True
                        else:
                            # Only count as translated if not marked as failed in progress.json
                            if not any(filename == file_path.name for filename in progress_data.get("failed_translations", {})):
                                chapter_status[chapter_name]["translated_shards"] += 1

        # Calculate progress and set status for each chapter
        for chapter_name, status in chapter_status.items():
            if status["total_shards"] > 0:
                # Calculate progress based on successful translations only
                total_processed = status["translated_shards"]
                status["progress"] = round((total_processed / status["total_shards"]) * 100, 1)

                # Set chapter status
                if status["failed"]:
                    # Any chapter with failed shards is marked as Incomplete
                    status["status"] = "Incomplete"
                elif status["translated_shards"] == status["total_shards"]:
                    status["status"] = "Translated"
                elif status["translated_shards"] > 0:
                    status["status"] = "Translating"

        # Sort chapters for better readability in logs
        return dict(sorted(chapter_status.items()))


class FileSplitter:
    def __init__(self, file_path, output_dir):
        self.file_path = file_path
        self.output_dir = output_dir
        self.chapters_dir = self.output_dir / "input_chapters"
        self.chapters_dir.mkdir(exist_ok=True)

    def split_chapters(self):
        with open(self.file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        chapters = content.split('\n\n')
        for i, chapter in enumerate(chapters, 1):
            if chapter.strip():
                chapter_file = self.chapters_dir / f"chapter_{i:04d}.txt"
                with open(chapter_file, 'w', encoding='utf-8') as cf:
                    cf.write(chapter.strip())
