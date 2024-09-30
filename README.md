# GUI
 Developing Graphic User Inferface for cleaning data into PostgreSQL# Directory Watcher GUI

## Introduction
This Python application is a Directory Watcher and File Processor with a user-friendly PyQt5 GUI. It monitors a specified folder for new or modified files, converts various file formats (e.g., .csv, .xls, .json) into CSV if necessary, and automatically imports the data into a PostgreSQL database. The app tracks processed files using an SQLite database to avoid redundant operations, ensuring files are only reprocessed when modified. With real-time monitoring, automatic file processing, and robust error logging, the tool simplifies and automates data handling and storage tasks.

## Prerequisites
- **Windows OS**
- **No need to install Python** or any other dependencies.

## Installation
No installation required. Just download and run the executable.

## How to Run
1. **Download and Extract the Files:**
   - Download the ZIP file containing `directory_watcher_gui.exe`.
   - Extract the contents of the ZIP file to a directory on your computer.

2. **Navigate to the Directory:**
   - Open the directory containing the extracted files.
   - Double-click `directory_watcher_gui.exe` to run the application.

## Usage
1. **Select the Directory to Watch:**
   - Click the "Browse" button next to "Directory to Watch".
   - Select the folder you want to monitor for changes.

2. **Select the Output Directory:**
   - Click the "Browse Output" button next to "Output Directory".
   - Select the folder where the processed CSV files will be saved.

3. **Set Database Parameters (Optional):**
   - By default, the database parameters are set to:
     ```json
     {
         "dbname": "xxxxxxxxx",
         "user": "xxxxxxxxxxx",
         "password": "xxxxxxxxxx",
         "host": "xxxxxxxxxx",
         "port": "xxxxxxxxxx"
     }
     ```
   - Modify these parameters in the "Database Parameters (Optional)" text box if needed.

4. **Start Watching:**
   - Click "Start Watching" to begin monitoring the selected directory.
   - **Ensure the Executable is Running Before Adding Files:**
     - Ensure `directory_watcher_gui.exe` is running before adding or modifying files in the monitored directory for them to be processed.
     - If you get the message "The selected directory is empty", add the files to the designated input folder and click "Start" again.

5. **Stop Watching:**
   - Click "Stop Watching" to stop monitoring the directory.

## Error Handling
- **File Conversion Errors:**
  - The application will log any errors encountered during file conversion to the log output window.
- **Database Import Errors:**
  - Any errors during the import process will also be logged in the log output window.

## Logging
- All operations are logged in a file named `import_log.log` located in the same directory as the executable.

## Known Issues
- The application may not display hidden files or system files within the directory.
- Ensure that the directories selected have read and write permissions.


## Distribution
To run the application on a different computer, follow these steps:

1. **Copy the Executable and Necessary Files:**
   - Transfer the `directory_watcher_gui.exe` file and any associated files or directories to the target computer.

2. **Ensure Python and Dependencies are Installed:**
   - If Python is not installed on the target computer, download and install it from Python's official website.
   - Open a command prompt and run the following command to install the necessary Python packages:
     ```sh
     pip install watchdog sqlalchemy pandas psycopg2 PyQt5
     ```

3. **Run the Executable:**
   - Navigate to the directory containing `directory_watcher_gui.exe`.
   - Double-click `directory_watcher_gui.exe` to launch the application.

## Notes
- Make sure to update any paths or configuration settings to match the environment of the target computer.
- Verify that the target computer has the necessary permissions to access the directories and database.
