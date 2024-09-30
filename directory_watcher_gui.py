import sys
import os
import time
import pandas as pd
import psycopg2
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog, QListWidget
from PyQt5.QtCore import QThread, pyqtSignal

# Database connection parameters
db_params = {
    'dbname': 'xxxxxxxx',
    'user': 'xxxxxxxx',
    'password': 'xxxxxxxx',
    'host': 'xxxxxxxx',
    'port': 'xxxxx'
}

# SQLite connection for tracking processed files
tracking_db = 'sqlite:///processed_files.db'
engine = create_engine(tracking_db)
Session = sessionmaker(bind=engine)
session = Session()

# Create a table to track processed files
metadata = MetaData()
processed_files_table = Table(
    'processed_files', metadata,
    Column('filename', String, primary_key=True),
    Column('last_modified', DateTime)
)
metadata.create_all(engine)

# Set up logging
logging.basicConfig(filename='watcher.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Watcher(QThread):
    log_signal = pyqtSignal(str)

    def __init__(self, directory_to_watch, output_directory, db_params):
        super().__init__()
        self.DIRECTORY_TO_WATCH = directory_to_watch
        self.OUTPUT_DIRECTORY = output_directory
        self.db_params = db_params
        self.observer = Observer()

    def run(self):
        event_handler = Handler(self.db_params, self.log_signal, self.DIRECTORY_TO_WATCH, self.OUTPUT_DIRECTORY)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()

        # Process existing files in the directory
        event_handler.process_existing_files()

        try:
            while self.observer.is_alive():
                time.sleep(5)
        except KeyboardInterrupt:
            self.stop()
        self.observer.join()

    def stop(self):
        self.observer.stop()
        self.observer.join()


class Handler(FileSystemEventHandler):
    def __init__(self, db_params, log_signal, directory_to_watch, output_directory):
        self.db_params = db_params
        self.log_signal = log_signal
        self.directory_to_watch = directory_to_watch
        self.output_directory = output_directory

    def process(self, event):
        if not event.is_directory:
            self._process_file(event.src_path)

    def process_existing_files(self):
        for filename in os.listdir(self.directory_to_watch):
            file_path = os.path.join(self.directory_to_watch, filename)
            if os.path.isfile(file_path):
                self._process_file(file_path)

    def _process_file(self, file_path):
        filename = os.path.basename(file_path)
        last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))

        record = session.query(processed_files_table).filter_by(filename=filename).first()
        if record is None or record.last_modified < last_modified:
            if record is None:
                new_record = processed_files_table.insert().values(
                    filename=filename,
                    last_modified=last_modified
                )
                session.execute(new_record)
            else:
                update_record = processed_files_table.update().where(
                    processed_files_table.c.filename == filename
                ).values(last_modified=last_modified)
                session.execute(update_record)
            session.commit()

            self.log_signal.emit(f"Processing file: {file_path}")
            logging.info(f"Processing file: {file_path}")

            try:
                if file_path.endswith('.csv'):
                    logging.info(f"Importing CSV file: {file_path}")
                    import_csv_to_postgresql(file_path, self.db_params)
                else:
                    logging.info(f"Converting file to CSV: {file_path}")
                    convert_files_to_csv_utf8(self.directory_to_watch, self.output_directory)
                    output_csv_path = os.path.join(self.output_directory, f"{os.path.splitext(filename)[0]}.csv")
                    if os.path.exists(output_csv_path):
                        logging.info(f"Importing converted CSV file: {output_csv_path}")
                        import_csv_to_postgresql(output_csv_path, self.db_params)
            except Exception as e:
                self.log_signal.emit(f"Error processing file {file_path}: {e}")
                logging.error(f"Error processing file {file_path}: {e}")

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)


def convert_files_to_csv_utf8(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    temp_file = os.path.join(output_folder, 'temp_file.txt')

    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)

        if os.path.isfile(file_path):
            base_filename, file_extension = os.path.splitext(filename)
            output_csv_path = os.path.join(output_folder, f"{base_filename}.csv")

            if file_extension.lower() == '.csv':
                # If the file is already a CSV, simply copy it to the output directory
                if file_path != output_csv_path:
                    os.rename(file_path, output_csv_path)
                    logging.info(f"Copied {file_path} to {output_csv_path}")
            else:
                try:
                    clean_file(file_path, temp_file)

                    if file_extension.lower() in ['.txt', '.csv']:
                        with open(temp_file, 'r', encoding='utf-8') as temp_f:
                            first_line = temp_f.readline()
                            delimiter = ',' if ',' in first_line else ('\t' if '\t' in first_line else ' ')

                        df = pd.read_csv(temp_file, encoding='utf-8', sep=delimiter, engine='python', on_bad_lines='skip')
                    elif file_extension.lower() in ['.xls', '.xlsx']:
                        df = pd.read_excel(file_path)
                    elif file_extension.lower() == '.json':
                        df = pd.read_json(file_path)
                    else:
                        df = pd.read_csv(temp_file, encoding='utf-8', sep=' ', engine='python', on_bad_lines='skip')

                    df.to_csv(output_csv_path, index=False, encoding='utf-8')
                    logging.info(f"Converted {file_path} to {output_csv_path}")
                except UnicodeDecodeError:
                    logging.warning(f"Failed to process {file_path} with 'utf-8' encoding. Trying 'latin1'.")
                    try:
                        clean_file(file_path, temp_file)
                        df = pd.read_csv(temp_file, encoding='latin1', sep=delimiter, engine='python', on_bad_lines='skip')
                        df.to_csv(output_csv_path, index=False, encoding='utf-8')
                        logging.info(f"Converted {file_path} to {output_csv_path} with 'latin1' encoding.")
                    except Exception as e:
                        logging.error(f"Failed to process {file_path} with 'latin1' encoding: {e}")
                except pd.errors.ParserError as e:
                    logging.error(f"Failed to process {file_path}: {e}")
                except Exception as e:
                    logging.error(f"Failed to process {file_path}: {e}")

    if os.path.exists(temp_file):
        os.remove(temp_file)


def clean_file(input_path, temp_path):
    with open(input_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    with open(temp_path, 'w', encoding='utf-8') as file:
        for line in lines:
            cleaned_line = ' '.join(line.split())
            file.write(cleaned_line + '\n')


def table_exists(cursor, table_name):
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
    """)
    return cursor.fetchone()[0]


def import_csv_to_postgresql(file_path, db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        logging.info("Connected to the database successfully")

        if os.path.isfile(file_path) and file_path.endswith('.csv'):
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            sanitized_table_name = sanitize_table_name(table_name)

            # Check if the table already exists
            if table_exists(cursor, sanitized_table_name):
                logging.info(f"Table {sanitized_table_name} already exists. Skipping import.")
                return

            df = pd.read_csv(file_path)
            logging.info(f"Processing file: {file_path} into table: {sanitized_table_name}")

            df.columns = [col.lower() for col in df.columns]

            logging.debug(f"Columns in CSV file: {df.dtypes}")

            try:
                # Quote column names to handle special characters and spaces
                cols_with_types = ", ".join([f'"{col}" {infer_sql_type(df[col])}' for col in df.columns])
                create_table_query = f'CREATE TABLE "{sanitized_table_name}" ({cols_with_types});'
                logging.debug(f"Creating table with query: {create_table_query}")
                cursor.execute(create_table_query)

                temp_csv = f"{file_path}.tmp"
                df.to_csv(temp_csv, index=False, header=False)

                with open(temp_csv, 'r') as f:
                    cursor.copy_expert(f'COPY "{sanitized_table_name}" FROM STDIN WITH CSV', f)

                os.remove(temp_csv)
                conn.commit()
                logging.info(f"Imported {file_path} to table {sanitized_table_name}")

            except Exception as e:
                logging.error(f'Failed to import {file_path} to table "{sanitized_table_name}": {e}')
                conn.rollback()

        cursor.close()
        conn.close()
        logging.info("Database connection closed")
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")


def sanitize_table_name(name):
    sanitized_name = name.strip().replace(' ', '_').replace('-', '_').replace('.', '').lower()
    if sanitized_name[0].isdigit():
        sanitized_name = 't_' + sanitized_name
    return sanitized_name


def infer_sql_type(pd_series):
    if pd.api.types.is_integer_dtype(pd_series):
        if pd_series.max() > 2147483647 or pd_series.min() < -2147483648:
            return "BIGINT"
        else:
            return "INTEGER"
    elif pd.api.types.is_float_dtype(pd_series):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_datetime64_any_dtype(pd_series):
        return "TIMESTAMP"
    else:
        return "TEXT"


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Directory Watcher")
        self.setGeometry(300, 300, 400, 400)

        self.layout = QVBoxLayout()

        self.dir_label = QLabel("Directory to Watch:")
        self.dir_input = QLineEdit()
        self.browse_button = QPushButton("Browse")
        self.browse_button.clicked.connect(self.browse_directory)

        self.output_label = QLabel("Output Directory:")
        self.output_input = QLineEdit()
        self.output_browse_button = QPushButton("Browse Output")
        self.output_browse_button.clicked.connect(self.browse_output_directory)

        self.db_label = QLabel("Database Parameters (Optional):")
        self.db_input = QTextEdit()
        self.db_input.setPlainText(str(db_params))

        self.start_button = QPushButton("Start Watching")
        self.start_button.clicked.connect(self.start_watching)
        self.stop_button = QPushButton("Stop Watching")
        self.stop_button.clicked.connect(self.stop_watching)
        self.stop_button.setEnabled(False)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)

        self.file_list_widget = QListWidget()  # Widget to display the files

        self.layout.addWidget(self.dir_label)
        self.layout.addWidget(self.dir_input)
        self.layout.addWidget(self.browse_button)
        self.layout.addWidget(self.output_label)
        self.layout.addWidget(self.output_input)
        self.layout.addWidget(self.output_browse_button)
        self.layout.addWidget(self.db_label)
        self.layout.addWidget(self.db_input)
        self.layout.addWidget(self.start_button)
        self.layout.addWidget(self.stop_button)
        self.layout.addWidget(self.log_output)
        self.layout.addWidget(self.file_list_widget)  # Add the file list widget

        self.setLayout(self.layout)

        self.watcher = None

    def browse_directory(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.dir_input.setText(directory)
            self.update_file_list(directory)

    def browse_output_directory(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Output Directory")
        if directory:
            self.output_input.setText(directory)

    def update_file_list(self, directory):
        self.file_list_widget.clear()
        if os.path.isdir(directory):
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                if os.path.isfile(file_path):
                    self.file_list_widget.addItem(filename)
        else:
            logging.error(f"{directory} is not a valid directory.")
            self.log_output.append(f"{directory} is not a valid directory.")

    def start_watching(self):
        directory = self.dir_input.text()
        output_directory = self.output_input.text()
        logging.debug(f"Selected directory: {directory}")
        logging.debug(f"Output directory: {output_directory}")
        if not directory or not output_directory:
            self.log_output.append("Please select both input and output directories.")
            return

        # Check if the directory has files
        try:
            files = os.listdir(directory)
            logging.debug(f"Files in directory: {files}")
            if not files:
                self.log_output.append("The selected directory is empty.")
                return
        except Exception as e:
            self.log_output.append(f"Error reading directory: {e}")
            logging.error(f"Error reading directory: {e}")
            return

        # Parse database parameters from the input (optional)
        try:
            db_params = eval(self.db_input.toPlainText())
        except Exception as e:
            self.log_output.append(f"Error parsing database parameters: {e}")
            logging.error(f"Error parsing database parameters: {e}")
            return

        self.watcher = Watcher(directory, output_directory, db_params)
        self.watcher.log_signal.connect(self.log_output.append)
        self.watcher.start()
        self.log_output.append(f"Started watching directory: {directory}")
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.update_file_list(directory)

    def stop_watching(self):
        if self.watcher:
            self.watcher.stop()
            self.watcher.wait()
            self.log_output.append("Stopped watching directory.")
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec())
