# Azure Function Pipelines

## Getting started

### Prerequisites/Dependencies

You will need the following installed on your system:

- Python 3.8+
- [Pip](https://pip.pypa.io/en/stable/)

### Setup

If you would like to update the api, please follow the instructions below.

1. Create a local virtual environment and activate it:

   ```bash
   python -m venv .venv
   source .venv/bin/activate # linux
   .venv\Scripts\activate # windows
   ```

   If you are using Anaconda, you can create a virtual environment with:

   ```bash
   conda create -n fairhub-pipeline-dev-env python=3.11
   conda activate fairhub-pipeline-dev-env
   ```

2. Install the dependencies:

   ```bash
   pip install -r requirements.txt
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

## License

This work is licensed under
[MIT](https://opensource.org/licenses/mit). See [LICENSE](https://github.com/AI-READI/pipeline/blob/main/LICENSE) for more information.

<a href="https://aireadi.org" >
  <img src="https://www.channelfutures.com/files/2017/04/3_0.png" height="30" />
</a>
