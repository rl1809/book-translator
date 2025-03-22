import concurrent
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Dict, List, Optional

from google.generativeai import GenerativeModel
import google.generativeai as genai

from config import settings, prompts
from config.models import ModelConfig, GEMINI_FLASH_LITE_MODEL_CONFIG, GEMINI_PRO_MODEL_CONFIG
from config.prompts import PromptStyle
from config.settings import TRANSLATION_INTERVAL_SECONDS
from translator.file_handler import FileHandler
from translator.helper import is_in_chapter_range
from translator.text_processing import normalize_translation, detect_untranslated_chinese


@dataclass
class TranslationTask:
    """Dataclass to represent a translation task"""
    filename: str
    content: str


class TranslationManager:
    """Manages the translation process for a book, handling different types of translations and retries."""

    def __init__(self, model_config: ModelConfig, file_handler: Optional[FileHandler] = None):
        self._log_handlers = []
        self.primary_model = self._initialize_model(model_config)
        self.lite_model = self._initialize_model(GEMINI_FLASH_LITE_MODEL_CONFIG)
        self.pro_model = self._initialize_model(GEMINI_PRO_MODEL_CONFIG)
        self.primary_batch_size = model_config.BATCH_SIZE
        self.lite_batch_size = GEMINI_FLASH_LITE_MODEL_CONFIG.BATCH_SIZE
        self.pro_batch_size = GEMINI_PRO_MODEL_CONFIG.BATCH_SIZE
        self.file_handler = file_handler
        self._stop_requested = False  # Cancellation flag

    def _initialize_model(self, model_config: ModelConfig) -> GenerativeModel:
        """Initialize a Gemini model with the given configuration."""
        if not model_config.MODEL_NAME:
            raise ValueError("Model name must be provided")
        genai.configure(api_key=settings.get_api_key())
        model = genai.GenerativeModel(
            model_name=model_config.MODEL_NAME,
            generation_config=model_config.GENERATION_CONFIG,
            safety_settings=model_config.SAFETY_SETTINGS
        )
        logging.info("Successfully initialized model: %s", model_config.MODEL_NAME)
        return model

    def translate_book(
            self,
            prompt_style: PromptStyle = PromptStyle.Modern,
            start_chapter: Optional[int] = None,
            end_chapter: Optional[int] = None
    ) -> None:
        """Main method to handle the book translation process."""
        logging.info("Starting translation process for: %s (chapters %s-%s)",
                     self.file_handler.book_dir, start_chapter or 'begin', end_chapter or 'end')
        self._stop_requested = False
        
        self._handle_previous_cancellation()
        
        while not self._stop_requested and not self.file_handler.is_translation_complete(start_chapter, end_chapter):
            self._process_translation_phases(prompt_style, start_chapter, end_chapter)
            
            if self._stop_requested:
                logging.info("Translation process was cancelled by the user.")
                break

            self._perform_post_processing()

        self._finalize_translation(start_chapter, end_chapter)

    def _handle_previous_cancellation(self) -> None:
        """Handle any previous cancellation state in the progress data."""
        try:
            progress_data = self.file_handler.load_progress()
            if progress_data.get("clean_cancellation", False):
                logging.info("Detected previous clean cancellation")
                progress_data.pop("clean_cancellation", None)
                self.file_handler.save_progress(progress_data)
        except Exception as e:
            logging.warning(f"Failed to check cancellation status: {e}")

    def _process_translation_phases(
            self,
            prompt_style: PromptStyle,
            start_chapter: Optional[int],
            end_chapter: Optional[int]
    ) -> None:
        """Process all phases of translation including regular, Chinese-specific, and failed retries."""
        # Process regular translation tasks
        logging.info("--- Processing regular translation tasks ---")
        futures = self._process_regular_translation_batch(prompt_style, start_chapter, end_chapter, self.primary_batch_size)
        concurrent.futures.wait(futures)

        # Process Chinese-specific retries
        logging.info("--- Processing Chinese character specific retries ---")
        futures = self._process_chinese_retry_batch(prompt_style, start_chapter, end_chapter, self.lite_batch_size)
        concurrent.futures.wait(futures)

        # Process regular failed translation retries
        logging.info("--- Processing failed translation retries (regular failures) ---")
        futures = self._process_regular_translation_batch(prompt_style, start_chapter, end_chapter, self.pro_batch_size, is_retry=True)
        concurrent.futures.wait(futures)

    def _perform_post_processing(self) -> None:
        """Perform post-processing tasks after each translation phase."""
        self.file_handler.delete_invalid_translations()
        self.file_handler.extract_and_count_names()

    def _finalize_translation(self, start_chapter: Optional[int], end_chapter: Optional[int]) -> None:
        """Finalize the translation process by combining chapters."""
        if not self._stop_requested:
            self.file_handler.combine_chapter_translations(start_chapter=start_chapter, end_chapter=end_chapter)
            logging.info("Translation process completed for: %s", self.file_handler.book_dir)
        else:
            logging.info("Translation process stopped before completion.")

    def _process_regular_translation_batch(
        self,
        prompt_style: PromptStyle = PromptStyle.Modern,
        start_chapter: Optional[int] = None,
        end_chapter: Optional[int] = None,
        batch_size: Optional[int] = None,
        is_retry: bool = False,
    ) -> List[concurrent.futures.Future]:
        """Process a batch of regular translation tasks."""
        executor = ThreadPoolExecutor(max_workers=batch_size)
        futures = []
        
        tasks = self._prepare_regular_tasks(start_chapter, end_chapter, is_retry)
        if not tasks:
            logging.info("No tasks to process")
            return futures

        progress_data = self.file_handler.load_progress()
        retry_lock = Lock()

        batch_index = 0
        while tasks and not self._stop_requested:
            batch = self._prepare_batch(tasks, batch_size, is_retry, start_chapter, end_chapter)
            if not batch:
                break

            futures.extend(self._submit_batch_tasks(executor, batch, progress_data, retry_lock, prompt_style, is_retry, batch_index))
            batch_index += 1

        executor.shutdown(wait=False)
        return futures

    def _prepare_regular_tasks(
            self,
            start_chapter: Optional[int] = None,
            end_chapter: Optional[int] = None,
            is_retry: bool = False
    ) -> List[TranslationTask]:
        """Prepare regular translation tasks based on whether it's a retry or not."""
        if is_retry:
            return self._prepare_retry_tasks(start_chapter, end_chapter)
        return self._prepare_new_tasks(start_chapter, end_chapter)

    def _prepare_new_tasks(
            self,
            start_chapter: Optional[int] = None,
            end_chapter: Optional[int] = None
    ) -> List[TranslationTask]:
        """Prepare new translation tasks that haven't been processed yet."""
        prompts_dir = self.file_handler.get_path("prompt_files")
        responses_dir = self.file_handler.get_path("translation_responses")

        existing_responses = [f.stem for f in responses_dir.glob("*.txt")]

        tasks = [
            TranslationTask(f.name, self.file_handler.load_prompt_file_content(f.name))
            for f in prompts_dir.glob("*.txt")
            if (f.stem not in existing_responses and
                is_in_chapter_range(f.name, start_chapter, end_chapter))
        ]

        return sorted(tasks, key=lambda t: t.filename)

    def _prepare_batch(
            self,
            tasks: List[TranslationTask],
            batch_size: int,
            is_retry: bool,
            start_chapter: Optional[int],
            end_chapter: Optional[int]
    ) -> List[TranslationTask]:
        """Prepare a batch of tasks for processing."""
        self._enforce_rate_limit(self.file_handler.load_progress(), len(tasks), batch_size)
        batch = tasks[:batch_size]
        
        if not is_retry and self._has_processed_tasks(batch):
            tasks = self._prepare_new_tasks(start_chapter, end_chapter)
            batch = tasks[:batch_size]
            
        tasks[:] = tasks[batch_size:]  # Remove processed tasks
        return batch

    def _submit_batch_tasks(
            self,
            executor: ThreadPoolExecutor,
            batch: List[TranslationTask],
            progress_data: Dict,
            retry_lock: Lock,
            prompt_style: PromptStyle,
            is_retry: bool,
            batch_index: int
    ) -> List[concurrent.futures.Future]:
        """Submit a batch of tasks for processing."""
        logging.info("Processing batch %d with %d tasks", batch_index, len(batch))
        logging.info(f"Tasks in this batch: {[task.filename for task in batch]}")

        batch_futures = [
            executor.submit(
                self._process_regular_task,
                task,
                progress_data,
                retry_lock,
                prompt_style,
                is_retry,
            )
            for task in batch
        ]

        progress_data.update({
            "last_batch_time": time.time(),
            "last_batch_size": len(batch)
        })
        self.file_handler.save_progress(progress_data)

        return batch_futures

    def _has_processed_tasks(
            self,
            batch: List[TranslationTask],
    ) -> bool:
        """Check if any tasks in the batch have already been processed."""
        responses_dir = self.file_handler.get_path("translation_responses")
        existing_responses = [f.name for f in responses_dir.glob("*.txt")]

        processed_tasks = [
            task for task in batch if task.filename in existing_responses
        ]
        return len(processed_tasks) > 0

    def _process_regular_task(
            self,
            task: TranslationTask,
            progress_data: Dict,
            retry_lock: Lock,
            prompt_style: PromptStyle,
            is_retry: bool = False,
    ) -> None:
        """Process a regular translation task."""
        if self._stop_requested:
            logging.info("Translation task %s cancelled.", task.filename)
            return

        model = self._select_model_for_task(is_retry)
        self._mark_task_as_retried(task.filename, progress_data, retry_lock, is_retry)
                    
        try:
            translated_text = self._translate(
                model=model,
                raw_text=task.content,
                prompt_style=prompt_style,
            )
            if not translated_text:
                logging.error("Error processing %s", task.filename)
                return
            
            self._handle_chinese_characters(task, translated_text, progress_data, retry_lock)
                
        except Exception as e:
            self._handle_translation_error(task.filename, str(e), progress_data, retry_lock)

    def _select_model_for_task(self, is_retry: bool) -> GenerativeModel:
        """Select the appropriate model for the translation task."""
        if is_retry:
            return self.pro_model
        return self.primary_model

    def _mark_task_as_retried(
            self,
            filename: str,
            progress_data: Dict,
            retry_lock: Lock,
            is_retry: bool
    ) -> None:
        """Mark a task as retried in the progress data."""
        if is_retry:
            with retry_lock:
                if "failed_translations" in progress_data and filename in progress_data["failed_translations"]:
                    progress_data["failed_translations"][filename]["retried"] = True
                    self.file_handler.save_progress(progress_data)

    def _handle_chinese_characters(
            self,
            task: TranslationTask,
            translated_text: str,
            progress_data: Dict,
            retry_lock: Lock
    ) -> None:
        """Handle the presence of Chinese characters in the translated text."""
        has_chinese, ratio = detect_untranslated_chinese(translated_text)
        
        if not has_chinese or ratio <= 0.5:
            # No Chinese characters or negligible amount - handle as success
            self._handle_translation_success(task, translated_text, progress_data, retry_lock)
        elif has_chinese and ratio <= 10:
            # Some Chinese characters (≤10%) - store content but mark as failed
            self._handle_partial_chinese_translation(task, translated_text, ratio, progress_data, retry_lock)
        else:
            # Excessive Chinese characters (>10%) - treat as failure
            self._handle_excessive_chinese_translation(task, ratio, progress_data, retry_lock)

    def _handle_partial_chinese_translation(
            self,
            task: TranslationTask,
            translated_text: str,
            ratio: float,
            progress_data: Dict,
            retry_lock: Lock
    ) -> None:
        """Handle translation with some Chinese characters (≤10%)."""
        logging.warning(f"Text contains Chinese characters ({ratio:.2f}%) but ratio ≤ 10% for {task.filename}")
        self.file_handler.save_content_to_file(translated_text, task.filename, "translation_responses")
        self._mark_translation_failed(
            task.filename, 
            f"contains_chinese_but_stored ({ratio:.2f}%)", 
            progress_data, 
            retry_lock,
            store_failure_marker=False
        )

    def _handle_excessive_chinese_translation(
            self,
            task: TranslationTask,
            ratio: float,
            progress_data: Dict,
            retry_lock: Lock
    ) -> None:
        """Handle translation with excessive Chinese characters (>10%)."""
        error_msg = f"excessive chinese characters ({ratio:.2f}%)"
        logging.error(f"Text contains excessive Chinese characters ({ratio:.2f}%) for {task.filename}")
        self._mark_translation_failed(task.filename, error_msg, progress_data, retry_lock)

    def _handle_translation_error(
            self,
            filename: str,
            error_message: str,
            progress_data: Dict,
            retry_lock: Lock
    ) -> None:
        """Handle translation errors."""
        logging.error("Error processing %s: %s", filename, error_message)
        if "429" in error_message:
            return
        self._mark_translation_failed(filename, error_message.lower(), progress_data, retry_lock)

    def _handle_translation_success(
            self,
            task: TranslationTask,
            translated_text: str,
            progress_data: Dict,
            lock: Lock
    ) -> None:
        """Handle successful translation."""
        logging.info("Successfully translated: %s", task.filename)
        self.file_handler.save_content_to_file(translated_text, task.filename, "translation_responses")
        
        with lock:
            if "failed_translations" in progress_data and task.filename in progress_data["failed_translations"]:
                logging.info(f"Removing {task.filename} from failed translations after successful retry")
                del progress_data["failed_translations"][task.filename]
                self.file_handler.save_progress(progress_data)

    def _translate(
            self,
            model: GenerativeModel,
            raw_text: str,
            additional_info: Optional[str] = None,
            prompt_style: PromptStyle = PromptStyle.Modern
    ) -> Optional[str]:
        """Execute translation with quality checks."""
        prompt = self._build_translation_prompt(raw_text, additional_info, prompt_style)
        print(prompt)
        response = self._get_model_response(model, prompt)
        translated_text = response.text.strip()
        if not translated_text:
            raise ValueError("Empty model response")

        return normalize_translation(translated_text)

    def _build_translation_prompt(
            self,
            text: str,
            additional_info: Optional[str],
            prompt_style: PromptStyle
    ) -> str:
        """Build prompt based on selected style."""
        base_prompt = {
            PromptStyle.Modern: prompts.MODERN_PROMPT,
            PromptStyle.ChinaFantasy: prompts.CHINA_FANTASY_PROMPT,
            PromptStyle.BookInfo: prompts.BOOK_INFO_PROMPT,
            PromptStyle.Words: prompts.WORDS_PROMPT,
            PromptStyle.IncompleteHandle: prompts.INCOMPLETE_HANDLE_PROMPT,
        }[PromptStyle(prompt_style)]
        text = f"[**NỘI DUNG ĐOẠN VĂN**]\n{text.strip()}\n[**NỘI DUNG ĐOẠN VĂN**]"
        if additional_info:
            return f"{base_prompt}\n{text}\n{base_prompt}\n\n{additional_info}".strip()
        return f"{base_prompt}\n{text}\n{base_prompt}".strip()

    def _get_model_response(self, model: GenerativeModel, prompt: str) -> any:
        """Get model response with timeout handling."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(model.generate_content, prompt)
            return future.result(timeout=180)

    def translate_text(self, text: Optional[str], prompt_style: PromptStyle) -> str:
        """Translate a single piece of text."""
        if not text:
            return ""
        return self._translate(self.primary_model, text, prompt_style=prompt_style)

    def stop(self):
        """Stop all translation operations and clean up resources."""
        logging.info("Translator stop() called - cancelling all translation operations")
        self._stop_requested = True
        
        if self.file_handler:
            try:
                progress_data = self.file_handler.load_progress()
                progress_data["clean_cancellation"] = True
                self.file_handler.save_progress(progress_data)
            except Exception as e:
                logging.error(f"Error saving cancellation state: {e}")

    def _mark_translation_failed(
            self,
            filename: str,
            error_message: str,
            progress_data: Dict,
            lock: Lock,
            store_failure_marker: bool = True
    ) -> None:
        """Mark a translation as failed and categorize the failure."""
        with lock:
            if "failed_translations" not in progress_data:
                progress_data["failed_translations"] = {}
            
            failure_type = self._categorize_failure(error_message)
            
            progress_data["failed_translations"][filename] = {
                "error": error_message,
                "failure_type": failure_type,
                "timestamp": time.time(),
                "retried": True if filename in progress_data["failed_translations"] else False,
                "content_stored": not store_failure_marker,
            }
            
            if store_failure_marker:
                self._create_failure_marker(filename, failure_type, error_message)
            
            logging.warning(f"Translation for {filename} marked as failed: {failure_type}")
            self.file_handler.save_progress(progress_data)

    def _categorize_failure(self, error_message: str) -> str:
        """Categorize the type of translation failure."""
        if 'contains_chinese_but_stored' in error_message:
            return "contains_chinese_but_stored"
        elif 'chinese' in error_message:
            return "contains_chinese"
        elif 'prohibited' in error_message:
            return "prohibited_content"
        elif 'copyrighted' in error_message:
            return "copyrighted_content"
        return "generic"

    def _create_failure_marker(self, filename: str, failure_type: str, error_message: str) -> None:
        """Create a failure marker file for failed translations."""
        failure_message = f"[TRANSLATION FAILED]\n\nFailure Type: {failure_type}\n\nError: {error_message}\n\nTimestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\nThis file indicates a failed translation. Please check the error details above or manually translate this content."
        try:
            self.file_handler.save_content_to_file(failure_message, filename, "translation_responses")
            logging.info(f"Created failure marker file for {filename}")
        except Exception as e:
            logging.error(f"Failed to create failure marker file for {filename}: {e}")

    def _prepare_retry_tasks(
        self,
        start_chapter: Optional[int] = None,
        end_chapter: Optional[int] = None
    ) -> List[TranslationTask]:
        """Prepare retry tasks from failed translations that have not been retried."""
        progress_data = self.file_handler.load_progress()
        failed_translations = progress_data.get("failed_translations", {})
        
        if not failed_translations:
            logging.info("No failed translations to retry")
            return []
        
        tasks = []
        for filename, failure_info in failed_translations.items():
            if self._should_skip_retry(failure_info):
                continue
                
            if is_in_chapter_range(filename, start_chapter, end_chapter):
                content = self.file_handler.load_prompt_file_content(filename)
                if content:
                    tasks.append(TranslationTask(filename, content))
        
        logging.info(f"Found {len(tasks)} failed translations to retry")
        return tasks

    def _should_skip_retry(self, failure_info: Dict) -> bool:
        """Determine if a failed translation should be skipped for retry."""
        if failure_info.get("retried", False):
            return True
        if failure_info.get("failure_type") == "contains_chinese_but_stored":
            return True
        return False

    def _prepare_chinese_retry_tasks(
        self,
        start_chapter: Optional[int] = None,
        end_chapter: Optional[int] = None
    ) -> List[TranslationTask]:
        """Prepare retry tasks specifically for translations that contain Chinese characters but were stored."""
        progress_data = self.file_handler.load_progress()
        failed_translations = progress_data.get("failed_translations", {})
        
        if not failed_translations:
            logging.info("No translations with Chinese to retry")
            return []
        
        tasks = []
        for filename, failure_info in failed_translations.items():
            if self._should_skip_chinese_retry(failure_info):
                continue
                
            if is_in_chapter_range(filename, start_chapter, end_chapter):
                content = self.file_handler.load_content_from_file(filename, "translation_responses")
                if content:
                    logging.info(f"Loaded partial translation with Chinese characters for retry: {filename}")
                    tasks.append(TranslationTask(filename, content))
                else:
                    logging.warning(f"Failed to load stored translation for Chinese retry: {filename}")
        
        logging.info(f"Found {len(tasks)} translations with Chinese characters to retry")
        return tasks

    def _should_skip_chinese_retry(self, failure_info: Dict) -> bool:
        """Determine if a Chinese-containing translation should be skipped for retry."""
        if failure_info.get("retried", False):
            return True
        if failure_info.get("final", False):
            return True
        if failure_info.get("failure_type") != "contains_chinese_but_stored":
            return True
        return False

    def _process_chinese_retry_batch(
        self,
        prompt_style: PromptStyle = PromptStyle.Modern,
        start_chapter: Optional[int] = None,
        end_chapter: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> List[concurrent.futures.Future]:
        """Process a batch of translations that contain Chinese characters specifically."""
        executor = ThreadPoolExecutor(max_workers=batch_size)
        futures = []
        
        tasks = self._prepare_chinese_retry_tasks(start_chapter, end_chapter)
        if not tasks:
            logging.info("No Chinese-containing translations to process")
            return futures

        progress_data = self.file_handler.load_progress()
        retry_lock = Lock()

        batch_index = 0
        while tasks and not self._stop_requested:
            self._enforce_rate_limit(progress_data, len(tasks), batch_size)
            batch_index += 1
            batch = tasks[:batch_size]
            tasks = tasks[batch_size:]

            logging.info("Processing Chinese retry batch %d with %d tasks", batch_index, len(batch))
            logging.info(f"Chinese retry tasks in this batch: {[task.filename for task in batch]}")

            batch_futures = [
                executor.submit(
                    self._process_chinese_retry_task,
                    task,
                    progress_data,
                    retry_lock,
                    PromptStyle.IncompleteHandle,
                )
                for task in batch
            ]
            futures.extend(batch_futures)

            progress_data.update({
                "last_batch_time": time.time(),
                "last_batch_size": len(batch)
            })
            self.file_handler.save_progress(progress_data)

        executor.shutdown(wait=False)
        return futures
        
    def _process_chinese_retry_task(
            self,
            task: TranslationTask,
            progress_data: Dict,
            retry_lock: Lock,
            prompt_style: PromptStyle,
    ) -> None:
        """Process a translation task that contains Chinese characters specifically."""
        if self._stop_requested:
            logging.info("Chinese retry task %s cancelled.", task.filename)
            return
            
        model = self.lite_model
        
        with retry_lock:
            if "failed_translations" in progress_data and task.filename in progress_data["failed_translations"]:
                progress_data["failed_translations"][task.filename]["retried"] = True
                self.file_handler.save_progress(progress_data)
                    
        try:
            translated_text = self._translate(
                model=model,
                raw_text=task.content,
                prompt_style=prompt_style,
            )
            
            if not translated_text:
                logging.error("Error processing Chinese retry for %s", task.filename)
                return
            
            has_chinese, ratio = detect_untranslated_chinese(translated_text)
            
            if not has_chinese or ratio <= 0.5:
                self._handle_translation_success(task, translated_text, progress_data, retry_lock)
                logging.info(f"Successfully reduced Chinese characters in {task.filename} to {ratio:.2f}%")
            else:
                self._handle_failed_chinese_retry(task, translated_text, ratio, progress_data, retry_lock)
                
        except Exception as e:
            logging.error("Error processing Chinese retry for %s: %s", task.filename, str(e))
            if "429" in str(e):
                return

    def _handle_failed_chinese_retry(
            self,
            task: TranslationTask,
            translated_text: str,
            ratio: float,
            progress_data: Dict,
            retry_lock: Lock
    ) -> None:
        """Handle a failed attempt to reduce Chinese characters in a translation."""
        logging.warning(f"Retry failed to reduce Chinese characters in {task.filename}, still at {ratio:.2f}%")
        
        self.file_handler.save_content_to_file(translated_text, task.filename, "translation_responses")
        
        with retry_lock:
            if "failed_translations" in progress_data and task.filename in progress_data["failed_translations"]:
                progress_data["failed_translations"][task.filename]["final"] = True
                self.file_handler.save_progress(progress_data)

    def _enforce_rate_limit(self, progress_data: Dict, pending_tasks: int, batch_size: int) -> None:
        """Enforce rate limiting between batches."""
        last_batch_time = progress_data.get("last_batch_time", 0)
        elapsed = time.time() - last_batch_time
        remaining = TRANSLATION_INTERVAL_SECONDS - elapsed

        if remaining > 0 and (progress_data.get("last_batch_size", 0) + pending_tasks) > batch_size:
            logging.info("Rate limiting - sleeping %.2f seconds", remaining)
            time.sleep(remaining)
