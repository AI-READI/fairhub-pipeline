# Azure Function Pipelines

## Getting started

### Prerequisites/Dependencies

You will need the following installed on your system:

- [mise](https://mise.jdx.dev/) — manages Python and uv versions

### Setup

1. Install project tooling (Python 3.11 + uv) via mise:

   ```bash
   mise trust # only needed the first time
   mise install
   ```

   mise will automatically create a virtual environment via uv (`uv_venv_auto = true`).

2. Install the dependencies:

   ```bash
   uv pip install -r requirements.txt
   uv run xxx.py  # Always use 'uv run' — never bare 'python xxx.py'
   ```

3. Add your environment variables. An example is provided at `.env.example`

   ```bash
   cp .env.example .env
   ```

   Make sure to update the values in `.env` to match your local setup.

4. Format the code:

   ```bash
   poe format_with_isort
   poe format_with_black
   ```

   You can also run `poe format` to run both commands at once.

5. Check the code quality:

   ```bash
   poe typecheck
   poe pylint
   poe flake8
   ```

   You can also run `poe lint` to run all three commands at once.

6. To start the local server, run:

   ```bash
   poe init # pick python
   poe dev
   ```

   This runs `func start` with the `--python` flag.

7. To run code with Python GIL disabled, run:

   ```bash
   uv run python -X gil=0 .\garmin_pipeline_local.py
   ```

## License

This work is licensed under
[MIT](https://opensource.org/licenses/mit). See [LICENSE](https://github.com/AI-READI/pipeline/blob/main/LICENSE) for more information.

<a href="https://aireadi.org" >
  <img src="https://www.channelfutures.com/files/2017/04/3_0.png" height="30" />
</a>
