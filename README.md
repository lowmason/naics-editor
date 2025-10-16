# NAICS Editor

A tool for editing and managing NAICS text variables:
 - Title
 - Descriptions
 - Examples
 - Exclusions

## Features

1. Browse by NAICS level and/or code

2. Search NAICS text using Regular Expresions

3. Edit NAICS text variables:
    1. Title
    2. Descriptions
    3. Examples
    4. Exclusions

4. Save to database

## Getting Started

### Installation

```bash
git clone https://github.com/lowmason/naics-editor.git
cd naics-editor
```

### Running the App

```bash
uv run uvicorn main:app --reload
```
Open your browser to http://127.0.0.1:8000.

## Contributing

Pull requests are welcome! For major changes, please open an issue first.

## License

[MIT](LICENSE)