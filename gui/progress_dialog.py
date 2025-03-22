from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QProgressBar,
                             QFrame, QScrollArea, QTabWidget, QWidget, QMessageBox, QTableWidget, 
                             QTableWidgetItem, QHeaderView, QTextEdit)
from PyQt5.QtCore import Qt, QSize
import qtawesome as qta
import re
from pathlib import Path

from gui.ui_styles import ButtonStyles


class ShardDetailsDialog(QDialog):
    def __init__(self, chapter_name, file_handler, parent=None):
        super().__init__(parent)
        self.chapter_name = chapter_name
        self.file_handler = file_handler
        self.setWindowTitle(f"Shard Details - {chapter_name}")
        self.resize(800, 500)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        # Header
        header_layout = QHBoxLayout()
        icon_label = QLabel()
        icon_label.setPixmap(qta.icon("mdi.puzzle-outline", color="#4a86e8").pixmap(24, 24))
        header_label = QLabel(f"<h2>Shards for {self.chapter_name}</h2>")
        header_layout.addWidget(icon_label)
        header_layout.addWidget(header_label)
        header_layout.addStretch(1)
        layout.addLayout(header_layout)
        
        # Table of shards
        self.shards_table = QTableWidget()
        self.shards_table.setColumnCount(4)
        self.shards_table.setHorizontalHeaderLabels(["Shard #", "Status", "View Original", "View Translation"])
        self.shards_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.populate_shards_table()
        layout.addWidget(self.shards_table)
        
        # Close button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.setIcon(qta.icon("mdi.close", color="#424242"))
        close_btn.setIconSize(QSize(16, 16))
        close_btn.setStyleSheet(ButtonStyles.get_neutral_style())
        close_btn.clicked.connect(self.accept)
        button_layout.addWidget(close_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
    def populate_shards_table(self):
        # Get shard files for this chapter
        prompts_dir = self.file_handler.get_path("prompt_files")
        responses_dir = self.file_handler.get_path("translation_responses")
        
        # Pattern for chapter shards (e.g., Chapter_1_1.txt, Chapter_1_2.txt)
        pattern = re.compile(rf"^{re.escape(self.chapter_name)}_(\d+)\.txt$")
        
        prompt_files = []
        for p in prompts_dir.glob("*.txt"):
            match = pattern.match(p.name)
            if match:
                shard_num = int(match.group(1))
                prompt_files.append((shard_num, p))
        
        response_files = {}
        for r in responses_dir.glob("*.txt"):
            match = pattern.match(r.name)
            if match:
                shard_num = int(match.group(1))
                response_files[shard_num] = r
        
        # Get failed translations info from progress.json
        failed_translations = {}
        try:
            progress_data = self.file_handler.load_progress()
            if "failed_translations" in progress_data:
                for filename, failure_info in progress_data["failed_translations"].items():
                    match = pattern.match(filename)
                    if match:
                        shard_num = int(match.group(1))
                        failed_translations[shard_num] = failure_info
        except Exception as e:
            print(f"Error loading failed translations: {e}")
        
        # Sort prompt files by shard number
        prompt_files.sort(key=lambda x: x[0])
        
        # Populate table
        self.shards_table.setRowCount(len(prompt_files))
        for row, (shard_num, prompt_file) in enumerate(prompt_files):
            # Shard number
            shard_item = QTableWidgetItem(str(shard_num))
            shard_item.setTextAlignment(Qt.AlignCenter)
            self.shards_table.setItem(row, 0, shard_item)
            
            # Status
            is_translated = shard_num in response_files
            is_failed = shard_num in failed_translations
            
            if is_failed:
                failure_type = failed_translations[shard_num].get("failure_type", "generic")
                if failure_type == "contains_chinese":
                    status_text = "Failed: Contains Chinese"
                elif failure_type == "prohibited_content":
                    status_text = "Failed: Prohibited Content"
                elif failure_type == "copyrighted_content":
                    status_text = "Failed: Copyrighted Content"
                else:
                    status_text = "Failed: Translation Error"
                    
                status_item = QTableWidgetItem(status_text)
                status_item.setForeground(Qt.red)
            elif is_translated:
                status_item = QTableWidgetItem("Translated")
                status_item.setForeground(Qt.green)
            else:
                status_item = QTableWidgetItem("Not Translated")
                status_item.setForeground(Qt.gray)
                
            status_item.setTextAlignment(Qt.AlignCenter)
            self.shards_table.setItem(row, 1, status_item)
            
            # Original button
            original_btn = QPushButton("View Original")
            original_btn.setIcon(qta.icon("mdi.file-document-outline", color="#555"))
            original_btn.setStyleSheet(ButtonStyles.get_secondary_style())
            original_btn.clicked.connect(lambda checked, f=prompt_file.name: self.view_original_content(f))
            self.shards_table.setCellWidget(row, 2, original_btn)
            
            # Translation button
            translation_btn = QPushButton("View Translation")
            if is_failed:
                translation_btn.setIcon(qta.icon("mdi.alert-circle", color="#D32F2F"))
                translation_btn.setStyleSheet("""
                    QPushButton {
                        background-color: #FFEBEE;
                        color: #D32F2F;
                        border: 1px solid #E57373;
                        padding: 5px 10px;
                        border-radius: 3px;
                    }
                    QPushButton:hover {
                        background-color: #FFCDD2;
                    }
                    QPushButton:pressed {
                        background-color: #EF9A9A;
                    }
                """)
            else:
                translation_btn.setIcon(qta.icon("mdi.translate", color="#555"))
                translation_btn.setStyleSheet(ButtonStyles.get_secondary_style())
                
            translation_file = prompt_file.name if shard_num in response_files else None
            if translation_file:
                translation_btn.clicked.connect(lambda checked, f=translation_file: self.view_translation_content(f))
            else:
                translation_btn.setEnabled(False)
            self.shards_table.setCellWidget(row, 3, translation_btn)
    
    def view_original_content(self, filename):
        content = self.file_handler.load_content_from_file(filename, "prompt_files")
        if content:
            self.show_content_dialog("Original Content", content)
        else:
            QMessageBox.warning(self, "Error", f"Could not load original content: {filename}")
    
    def view_translation_content(self, filename):
        content = self.file_handler.load_content_from_file(filename, "translation_responses")
        if content:
            self.show_content_dialog("Translation Content", content, filename, is_translation=True)
        else:
            QMessageBox.warning(self, "Error", f"Could not load translation content: {filename}")
    
    def delete_translation(self, filename):
        reply = QMessageBox.question(
            self, 
            "Confirm Deletion",
            f"Are you sure you want to delete the translation for {filename}?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            success = self.file_handler.delete_file(filename, "translation_responses")
            if success:
                QMessageBox.information(self, "Success", "Translation deleted successfully")
                self.populate_shards_table()  # Refresh the table
            else:
                QMessageBox.warning(self, "Error", "Failed to delete translation")
    
    def show_content_dialog(self, title, content, filename=None, is_translation=False):
        dialog = QDialog(self)
        dialog.setWindowTitle(title)
        dialog.resize(600, 400)
        
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(15, 15, 15, 15)
        
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        
        # Check if this is a failed translation marker file
        is_failed_translation = False
        if is_translation and "[TRANSLATION FAILED]" in content:
            is_failed_translation = True
            # Set special styling for failed translations
            text_edit.setStyleSheet("""
                QTextEdit {
                    background-color: #FFEBEE;
                    border: 1px solid #E57373;
                    border-radius: 4px;
                    color: #D32F2F;
                }
            """)
            
            # Add warning header
            header_label = QLabel("<h3>⚠️ Failed Translation</h3>")
            header_label.setStyleSheet("color: #D32F2F; background-color: #FFEBEE; padding: 5px; border-radius: 4px;")
            header_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(header_label)
            
            # Add explanation text
            explanation = QLabel("This translation failed. You can manually edit this file or delete it to allow the system to translate it again.")
            explanation.setWordWrap(True)
            explanation.setStyleSheet("color: #D32F2F; margin-bottom: 10px;")
            layout.addWidget(explanation)
        
        text_edit.setPlainText(content)
        layout.addWidget(text_edit)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        if is_translation and filename:
            # Edit button
            edit_btn = QPushButton("Edit")
            edit_btn.setIcon(qta.icon("mdi.pencil", color="#1565C0"))
            edit_btn.setIconSize(QSize(16, 16))
            edit_btn.setStyleSheet(ButtonStyles.get_secondary_style())
            edit_btn.clicked.connect(lambda: self.edit_translation_content(dialog, text_edit, filename))
            button_layout.addWidget(edit_btn)
            
            # Delete button
            delete_btn = QPushButton("Delete")
            delete_btn.setIcon(qta.icon("mdi.delete-outline", color="#D32F2F"))
            delete_btn.setIconSize(QSize(16, 16))
            delete_btn.setStyleSheet(ButtonStyles.get_danger_style())
            delete_btn.clicked.connect(lambda: self.delete_translation_from_dialog(dialog, filename))
            button_layout.addWidget(delete_btn)
            
            # Add retry button for failed translations
            if is_failed_translation:
                retry_btn = QPushButton("Delete & Retry")
                retry_btn.setIcon(qta.icon("mdi.refresh", color="#4CAF50"))
                retry_btn.setIconSize(QSize(16, 16))
                retry_btn.setStyleSheet("""
                    QPushButton {
                        background-color: #4CAF50;
                        color: white;
                        border: none;
                        padding: 5px 10px;
                        border-radius: 3px;
                        font-weight: bold;
                    }
                    QPushButton:hover {
                        background-color: #66BB6A;
                    }
                    QPushButton:pressed {
                        background-color: #43A047;
                    }
                """)
                retry_btn.clicked.connect(lambda: self.retry_failed_translation(dialog, filename))
                button_layout.addWidget(retry_btn)
        
        close_btn = QPushButton("Close")
        close_btn.setStyleSheet(ButtonStyles.get_neutral_style())
        close_btn.clicked.connect(dialog.accept)
        button_layout.addWidget(close_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        dialog.exec_()
    
    def retry_failed_translation(self, parent_dialog, filename):
        """Delete a failed translation and also remove it from the failed_translations list"""
        reply = QMessageBox.question(
            parent_dialog, 
            "Confirm Retry",
            f"Are you sure you want to retry the translation for {filename}?\n\nThis will delete the current failed translation marker.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            # First delete the translation file
            success = self.file_handler.delete_file(filename, "translation_responses")
            
            if success:
                # Now also remove from failed_translations in progress.json
                try:
                    progress_data = self.file_handler.load_progress()
                    if "failed_translations" in progress_data and filename in progress_data["failed_translations"]:
                        del progress_data["failed_translations"][filename]
                        # Removed retry count reset since we're not tracking retries anymore
                        self.file_handler.save_progress(progress_data)
                        QMessageBox.information(parent_dialog, "Success", "Translation reset for retry successfully")
                    else:
                        QMessageBox.information(parent_dialog, "Success", "Translation file deleted, but was not in failed translations list")
                except Exception as e:
                    QMessageBox.warning(
                        parent_dialog, 
                        "Partial Success", 
                        f"Translation file was deleted, but failed to update progress data: {str(e)}"
                    )
                
                parent_dialog.accept()
                self.populate_shards_table()  # Refresh the table
            else:
                QMessageBox.warning(parent_dialog, "Error", "Failed to delete translation")
    
    def restore_view_buttons(self, parent_dialog, text_edit, filename):
        """Restore the original view buttons (Edit, Delete, Close) to the dialog"""
        parent_layout = parent_dialog.layout()
        old_button_layout = parent_layout.itemAt(parent_layout.count() - 1).layout()
        
        # Remove existing buttons
        while old_button_layout.count():
            item = old_button_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        # Create new button layout with original buttons
        new_button_layout = QHBoxLayout()
        new_button_layout.addStretch()
        
        # Edit button
        edit_btn = QPushButton("Edit")
        edit_btn.setIcon(qta.icon("mdi.pencil", color="#1565C0"))
        edit_btn.setIconSize(QSize(16, 16))
        edit_btn.setStyleSheet(ButtonStyles.get_secondary_style())
        edit_btn.clicked.connect(lambda: self.edit_translation_content(parent_dialog, text_edit, filename))
        new_button_layout.addWidget(edit_btn)
        
        # Delete button
        delete_btn = QPushButton("Delete")
        delete_btn.setIcon(qta.icon("mdi.delete-outline", color="#D32F2F"))
        delete_btn.setIconSize(QSize(16, 16))
        delete_btn.setStyleSheet(ButtonStyles.get_danger_style())
        delete_btn.clicked.connect(lambda: self.delete_translation_from_dialog(parent_dialog, filename))
        new_button_layout.addWidget(delete_btn)
        
        # Close button
        close_btn = QPushButton("Close")
        close_btn.setStyleSheet(ButtonStyles.get_neutral_style())
        close_btn.clicked.connect(parent_dialog.accept)
        new_button_layout.addWidget(close_btn)
        
        new_button_layout.addStretch()
        
        # Replace the old button layout with the new one
        parent_layout.removeItem(old_button_layout)
        parent_layout.addLayout(new_button_layout)
    
    def setup_edit_buttons(self, parent_dialog, text_edit, filename):
        """Set up the edit mode buttons (Save, Cancel)"""
        parent_layout = parent_dialog.layout()
        old_button_layout = parent_layout.itemAt(parent_layout.count() - 1).layout()
        
        # Remove existing buttons
        while old_button_layout.count():
            item = old_button_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        # Create new button layout for edit mode
        new_button_layout = QHBoxLayout()
        new_button_layout.addStretch()
        
        # Save button
        save_btn = QPushButton("Save Changes")
        save_btn.setIcon(qta.icon("mdi.content-save", color="#388E3C"))
        save_btn.setIconSize(QSize(16, 16))
        save_btn.setStyleSheet(ButtonStyles.get_primary_style())
        save_btn.clicked.connect(lambda: self.save_edited_translation(parent_dialog, text_edit, filename))
        new_button_layout.addWidget(save_btn)
        
        # Cancel button
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setStyleSheet(ButtonStyles.get_neutral_style())
        cancel_btn.clicked.connect(lambda: self.cancel_edit(parent_dialog, text_edit, filename))
        new_button_layout.addWidget(cancel_btn)
        
        new_button_layout.addStretch()
        
        # Replace the old button layout with the new one
        parent_layout.removeItem(old_button_layout)
        parent_layout.addLayout(new_button_layout)
    
    def edit_translation_content(self, parent_dialog, text_edit, filename):
        # Make the text edit editable
        text_edit.setReadOnly(False)
        text_edit.setStyleSheet("background-color: #FFFDE7;")  # Light yellow background to indicate edit mode
        
        # Replace buttons with Save and Cancel
        self.setup_edit_buttons(parent_dialog, text_edit, filename)
    
    def cancel_edit(self, parent_dialog, text_edit, filename):
        # Restore original content
        original_content = self.file_handler.load_content_from_file(filename, "translation_responses")
        text_edit.setPlainText(original_content)
        text_edit.setReadOnly(True)
        text_edit.setStyleSheet("")
        
        # Restore original buttons
        self.restore_view_buttons(parent_dialog, text_edit, filename)
    
    def save_edited_translation(self, parent_dialog, text_edit, filename):
        # Get the edited content
        content = text_edit.toPlainText()
        
        # Check if the content starts with "[TRANSLATION FAILED]"
        is_failed_translation = content.startswith("[TRANSLATION FAILED]")
        
        # If it no longer starts with the failure marker, consider it fixed
        if content.strip() and not is_failed_translation:
            success = self.file_handler.save_content_to_file(content, filename, "translation_responses")
            if success:
                try:
                    # Remove from failed_translations in progress.json
                    progress_data = self.file_handler.load_progress()
                    if "failed_translations" in progress_data and filename in progress_data["failed_translations"]:
                        del progress_data["failed_translations"][filename]
                        # Removed retry count reset since we're not tracking retries anymore
                        self.file_handler.save_progress(progress_data)
                        QMessageBox.information(parent_dialog, "Success", "Translation updated and removed from failed list")
                    else:
                        QMessageBox.information(parent_dialog, "Success", "Translation updated successfully")
                except Exception as e:
                    QMessageBox.warning(
                        parent_dialog, 
                        "Partial Success", 
                        f"Translation was updated, but failed to update progress data: {str(e)}"
                    )
            else:
                QMessageBox.information(parent_dialog, "Success", "Translation updated successfully")
                
            parent_dialog.accept()
            self.populate_shards_table()  # Refresh the table
        else:
            QMessageBox.warning(parent_dialog, "Error", "Failed to save translation")
    
    def delete_translation_from_dialog(self, parent_dialog, filename):
        reply = QMessageBox.question(
            parent_dialog, 
            "Confirm Deletion",
            f"Are you sure you want to delete the translation for {filename}?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            success = self.file_handler.delete_file(filename, "translation_responses")
            if success:
                QMessageBox.information(parent_dialog, "Success", "Translation deleted successfully")
                parent_dialog.accept()
                self.populate_shards_table()  # Refresh the table
            else:
                QMessageBox.warning(parent_dialog, "Error", "Failed to delete translation")


class EnhancedProgressDialog(QDialog):
    def __init__(self, get_status_func, parent=None, file_handler=None):
        super().__init__(parent)
        self.get_status_func = get_status_func
        self.chapter_status = self.get_status_func() or {}  # Handle None return value
        self.file_handler = file_handler
        self.setWindowTitle("Chapter Translation Progress")
        self.resize(700, 500)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        summary_frame = QFrame()
        summary_frame.setFrameShape(QFrame.StyledPanel)
        summary_frame.setFrameShadow(QFrame.Raised)
        summary_frame.setStyleSheet("""
            QFrame {
                background-color: #f7f7f7;
                border-radius: 8px;
                border: 1px solid #ddd;
            }
        """)
        summary_layout = QHBoxLayout(summary_frame)
        summary_layout.setSpacing(20)

        # Calculate accurate counts based on status
        total_chapters = len(self.chapter_status)
        completed_chapters = sum(1 for _, info in self.chapter_status.items() 
                               if info.get("status") == "Translated")
        
        in_progress_chapters = sum(1 for _, info in self.chapter_status.items() 
                                 if info.get("status") == "Translating")
        
        incomplete_chapters = sum(1 for _, info in self.chapter_status.items() 
                                if info.get("status") == "Incomplete")
        
        not_started_chapters = sum(1 for _, info in self.chapter_status.items() 
                                 if info.get("status") == "Not Started")
        
        # Calculate overall progress based on successful translations only
        total_shards = sum(info.get("total_shards", 0) for _, info in self.chapter_status.items())
        translated_shards = sum(info.get("translated_shards", 0) for _, info in self.chapter_status.items())
        
        avg_progress = 0
        if total_shards > 0:
            avg_progress = (translated_shards / total_shards) * 100

        # Create stats widgets
        total_label = self.create_stat_widget("Total Chapters", str(total_chapters), "mdi.book-open-variant")
        completed_label = self.create_stat_widget("Completed", str(completed_chapters), "mdi.check-circle", "green")
        pending_label = self.create_stat_widget("In Progress", str(in_progress_chapters), "mdi.progress-clock", "blue")
        incomplete_label = self.create_stat_widget("Incomplete", str(incomplete_chapters), "mdi.alert", "orange")

        progress_frame = QFrame()
        progress_layout = QVBoxLayout(progress_frame)
        progress_title = QLabel("<b>Overall Progress</b>")
        progress_title.setAlignment(Qt.AlignCenter)
        overall_progress = QProgressBar()
        overall_progress.setValue(int(avg_progress))
        overall_progress.setFormat(f"{avg_progress:.1f}%")
        overall_progress.setStyleSheet("""
            QProgressBar {
                border: 1px solid #bbb;
                border-radius: 5px;
                text-align: center;
                height: 25px;
                font-weight: bold;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                          stop:0 #4CAF50, stop:1 #8BC34A);
                border-radius: 5px;
            }
        """)
        progress_layout.addWidget(progress_title)
        progress_layout.addWidget(overall_progress)

        summary_layout.addWidget(total_label)
        summary_layout.addWidget(completed_label)
        summary_layout.addWidget(pending_label)
        summary_layout.addWidget(incomplete_label)
        summary_layout.addWidget(progress_frame)
        layout.addWidget(summary_frame)

        tab_widget = QTabWidget()
        tab_widget.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #ddd;
                border-radius: 5px;
                padding: 5px;
            }
            QTabBar::tab {
                padding: 8px 15px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: #f0f0f0;
                border-bottom: 2px solid #4CAF50;
            }
        """)

        chapter_tab = QWidget()
        chapter_layout = QVBoxLayout(chapter_tab)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setMinimumHeight(350)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        scroll_layout.setContentsMargins(5, 5, 5, 5)
        scroll_layout.setSpacing(15)

        sorted_chapters = sorted(self.chapter_status.items(),
                                 key=lambda x: int(x[0].split()[-1].isdigit() and x[0].split()[-1] or 0))

        for chapter, info in sorted_chapters:
            chapter_frame = self.create_chapter_frame(chapter, info)
            scroll_layout.addWidget(chapter_frame)

        scroll_widget.setLayout(scroll_layout)
        scroll_area.setWidget(scroll_widget)
        chapter_layout.addWidget(scroll_area)
        tab_widget.addTab(chapter_tab, "Chapter Details")

        layout.addWidget(tab_widget)

        button_layout = QHBoxLayout()
        button_layout.addStretch()

        refresh_btn = QPushButton("Refresh")
        refresh_btn.setIcon(qta.icon("mdi.refresh", color="#1565C0"))
        refresh_btn.setIconSize(QSize(16, 16))
        refresh_btn.setStyleSheet(ButtonStyles.get_secondary_style())
        refresh_btn.clicked.connect(self.refresh_status)
        button_layout.addWidget(refresh_btn)

        close_btn = QPushButton("Close")
        close_btn.setIcon(qta.icon("mdi.close", color="#424242"))
        close_btn.setIconSize(QSize(16, 16))
        close_btn.setStyleSheet(ButtonStyles.get_neutral_style())
        close_btn.clicked.connect(self.accept)
        button_layout.addWidget(close_btn)

        button_layout.addStretch()
        layout.addLayout(button_layout)

        self.setLayout(layout)

    def create_stat_widget(self, title, value, icon_name, color=None):
        widget = QFrame()
        layout = QVBoxLayout(widget)
        layout.setSpacing(5)
        layout.setAlignment(Qt.AlignCenter)

        icon = qta.icon(icon_name, color=color or "#505050")
        icon_label = QLabel()
        icon_label.setPixmap(icon.pixmap(24, 24))
        icon_label.setAlignment(Qt.AlignCenter)

        title_label = QLabel(f"<b>{title}</b>")
        title_label.setAlignment(Qt.AlignCenter)

        value_label = QLabel(value)
        value_label.setAlignment(Qt.AlignCenter)
        if color:
            value_label.setStyleSheet(f"font-size: 18px; font-weight: bold; color: {color};")
        else:
            value_label.setStyleSheet("font-size: 18px; font-weight: bold;")

        layout.addWidget(icon_label)
        layout.addWidget(title_label)
        layout.addWidget(value_label)

        return widget

    def create_chapter_frame(self, chapter, info):
        chapter_frame = QFrame()
        chapter_frame.setFrameShape(QFrame.StyledPanel)
        chapter_frame.setStyleSheet("""
            QFrame {
                border: 1px solid #ddd;
                border-radius: 6px;
                background-color: #fafafa;
            }
        """)
        chapter_layout_inner = QVBoxLayout(chapter_frame)
        chapter_layout_inner.setSpacing(8)

        header_layout = QHBoxLayout()

        icon_label = QLabel()
        progress_value = int(info.get("progress", 0))
        status = info.get("status", "Not Started")
        
        # Set icon based on status
        if status == "Incomplete":
            icon = qta.icon("mdi.alert", color="orange")
        elif status == "Translated":
            icon = qta.icon("mdi.check-circle", color="green")
        elif status == "Translating":
            icon = qta.icon("mdi.progress-clock", color="blue")
        else:
            icon = qta.icon("mdi.book", color="gray")
        icon_label.setPixmap(icon.pixmap(20, 20))

        chapter_label = QLabel(f"<b>{chapter}</b>")
        chapter_label.setStyleSheet("font-size: 14px;")

        status_text = status
        if status == "Incomplete":
            translated_shards = info.get("translated_shards", 0)
            failed_shards = info.get("failed_shards", 0)
            total_shards = info.get("total_shards", 0)
            status_text = f"Incomplete: {translated_shards} OK, {failed_shards} Failed"
                
        status_label = QLabel(status_text)
        if status == "Incomplete":
            status_label.setStyleSheet("color: orange; font-weight: bold;")
        elif status == "Translated":
            status_label.setStyleSheet("color: green; font-weight: bold;")
        elif status == "Translating":
            status_label.setStyleSheet("color: blue; font-weight: bold;")
        else:
            status_label.setStyleSheet("color: gray;")

        detail_btn = QPushButton("Details")
        detail_btn.setIcon(qta.icon("mdi.information-outline", color="#4a86e8"))
        detail_btn.setIconSize(QSize(16, 16))
        detail_btn.setStyleSheet(ButtonStyles.get_secondary_style())
        detail_btn.setFixedWidth(100)
        detail_btn.clicked.connect(lambda: self.show_shard_details(chapter))

        header_layout.addWidget(icon_label)
        header_layout.addWidget(chapter_label)
        header_layout.addStretch(1)
        header_layout.addWidget(status_label)
        header_layout.addWidget(detail_btn)
        chapter_layout_inner.addLayout(header_layout)

        progress_layout = QHBoxLayout()
        progress_bar = QProgressBar()
        progress_bar.setValue(progress_value)
        translated_shards = info.get("translated_shards", 0)
        failed_shards = info.get("failed_shards", 0)
        total_shards = info.get("total_shards", 0)

        # Set progress bar color based on status
        if status == "Incomplete":
            progress_bar.setStyleSheet("""
                QProgressBar {
                    border: 1px solid #bbb;
                    border-radius: 4px;
                    text-align: center;
                    height: 20px;
                }
                QProgressBar::chunk {
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                             stop:0 #FB8C00, stop:1 #FFA726);
                    border-radius: 4px;
                }
            """)
        else:
            progress_bar.setStyleSheet("""
                QProgressBar {
                    border: 1px solid #bbb;
                    border-radius: 4px;
                    text-align: center;
                    height: 20px;
                }
                QProgressBar::chunk {
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                             stop:0 #76b852, stop:1 #8DC26F);
                    border-radius: 4px;
                }
            """)

        if total_shards > 0:
            # Include info about failed shards in tooltip if any
            if failed_shards > 0:
                progress_bar.setFormat(f"{progress_value}% ({translated_shards}/{total_shards})")
                progress_bar.setToolTip(f"Translated: {translated_shards}, Failed: {failed_shards}, Total: {total_shards}")
            else:
                progress_bar.setFormat(f"{progress_value}%")
                progress_bar.setToolTip(f"{translated_shards}/{total_shards} shards")
        else:
            progress_bar.setFormat(f"{progress_value}%")

        progress_layout.addWidget(progress_bar)

        if "estimated_time" in info:
            time_icon = QLabel()
            time_icon.setPixmap(qta.icon("mdi.clock-outline").pixmap(16, 16))
            time_label = QLabel(f"Est. completion: {info['estimated_time']}")
            time_layout = QHBoxLayout()
            time_layout.addWidget(time_icon)
            time_layout.addWidget(time_label)
            time_layout.addStretch()

            progress_layout.addLayout(time_layout, 1)

        # Add shard count information
        if failed_shards > 0:
            shard_info_layout = QHBoxLayout()
            shard_info_icon = QLabel()
            shard_info_icon.setPixmap(qta.icon("mdi.puzzle-outline", color="#757575").pixmap(16, 16))
            shard_info_label = QLabel(f"Shards: {translated_shards} successful, {failed_shards} failed, {total_shards - translated_shards - failed_shards} pending")
            shard_info_label.setStyleSheet("color: #757575; font-style: italic;")
            shard_info_layout.addWidget(shard_info_icon)
            shard_info_layout.addWidget(shard_info_label)
            shard_info_layout.addStretch()
            chapter_layout_inner.addLayout(shard_info_layout)

        # Show error message for incomplete chapters
        if status == "Incomplete" and "error" in info:
            error_layout = QHBoxLayout()
            error_icon = QLabel()
            error_icon.setPixmap(qta.icon("mdi.alert", color="red").pixmap(16, 16))
            error_text = info["error"]
            if len(error_text) > 100:
                error_text = error_text[:97] + "..."
            error_label = QLabel(error_text)
            error_label.setStyleSheet("color: #d32f2f; font-style: italic;")
            error_layout.addWidget(error_icon)
            error_layout.addWidget(error_label)
            error_layout.addStretch()
            chapter_layout_inner.addLayout(error_layout)

        chapter_layout_inner.addLayout(progress_layout)

        return chapter_frame

    def refresh_status(self):
        self.chapter_status = self.get_status_func()
        self.close()
        new_dialog = EnhancedProgressDialog(self.get_status_func, self.parent(), self.file_handler)
        new_dialog.show()
        self.accept()

    def show_shard_details(self, chapter):
        """Show details dialog for a specific chapter's shards"""
        if self.file_handler:
            details_dialog = ShardDetailsDialog(chapter, self.file_handler, self)
            details_dialog.exec_()
        else:
            QMessageBox.warning(self, "Error", "File handler not available")
