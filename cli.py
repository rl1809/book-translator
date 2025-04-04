import argparse
import logging

from config.models import get_model_config
from downloader.factory import *
from translator.file_handler import FileHandler
from translator.manager import TranslationManager
from logger import logging_utils


def main() -> None:
    """Main entry point: parses arguments, sets up logging, runs book processing."""
    parser = argparse.ArgumentParser(description="Download, translate, and create EPUB books")
    parser.add_argument("--book_url",  help="URL of the book's main page")
    parser.add_argument("--start_chapter", help="URL of the book's main page", type=int)
    parser.add_argument("--end_chapter", help="URL of the book's main page", type=int)
    parser.add_argument("--model-name", help="Model to use", type=str)
    parser.add_argument("--prompt_style", help="Prompt style to use", type=int)
    parser.add_argument("--output_directory", help="Directory to save downloaded books to", type=str)
    args = parser.parse_args()

    logging.info("Application started.")

    try:
        downloader = DownloaderFactory.create_downloader( url=args.book_url, output_dir= Path(args.output_directory) )
        book_info = downloader.book_info
        book_dir = downloader.book_dir

        start_chapter, end_chapter = args.start_chapter, args.end_chapter
        model_config = get_model_config(args.model_name)

        logging_utils.configure_logging(Path(book_dir))
        logging.info(f"Processing book: '{book_info.title}' with ID: {book_info.id}")

        file_handler = FileHandler(book_dir=book_dir)
        translator = TranslationManager(model_config=model_config, file_handler=file_handler)

        logging.info(f"Starting book processing: '{book_info.title}' in {book_dir}")

        logging.info("--- Stage 1: Downloading Book Chapters ---")
        downloader.download_book()

        logging.info("--- Stage 2: Creating Prompt Files ---")
        file_handler.create_prompt_files_from_chapters()

        logging.info("--- Stage 3: Translating Prompts ---")
        translator.translate_book(prompt_style=args.prompt_style, start_chapter=start_chapter,
                                            end_chapter=end_chapter)

        logging.info("--- Stage 4: Extracting Chinese Words ---")
        chinese_words_path = file_handler.extract_chinese_sentences_to_file()
        if chinese_words_path:
            logging.info(f"Chinese words extracted to: {chinese_words_path}")
        else:
            logging.warning("No Chinese words were extracted.")

        logging.info("--- Stage 5: Replacing Chinese Words in Chapters ---")
        processed_count = file_handler.replace_chinese_sentences_in_translation_responses()
        if processed_count > 0:
            logging.info(f"Replaced Chinese words in {processed_count} chapter files")
        else:
            logging.warning("No chapters were processed for Chinese word replacement")

        logging.info("--- Stage 6: Generating EPUB ---")
        epub_path = file_handler.generate_epub(book_info.title, book_info.author, cover_image='')
        if epub_path:
            logging.info(f"EPUB file successfully created: {epub_path}")
        else:
            logging.error("EPUB generation failed.")

    except Exception as e:
        logging_utils.log_exception(e, "Application encountered a critical error.")
        logging.critical("Application aborted due to critical error.")
        exit(1)

    finally:
        logging.info("Application finished.")


if __name__ == "__main__":
    main()
