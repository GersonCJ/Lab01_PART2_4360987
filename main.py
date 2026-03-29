from constants import path_strings
from pathlib import Path
import src.extraction as ext
# ------------------ Extraction using commit a499dd34c1372468f2335a370c5dd13cc3a72d90

url = path_strings.url_main
url_metadata = path_strings.metadata_url
bronze_path = Path(path_strings.bronze_path)

if not any(bronze_path.iterdir()):
    print("Starting extraction ...")
    ext.extract(url, url_metadata)
else:
    print("Data already available. Skipping extraction ...")


def main():
    print("Hello from lab01-part02-4360987!")


if __name__ == "__main__":
    main()
