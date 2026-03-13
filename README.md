# databricks-million-songs-tutorial

Code for the [Databricks SDP million songs tutorial](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started).

Requires:
- Python
- Databricks Workspace

*NOTE:* If you fork this repo you will need to install Databricks App against Github to allow Databricks git integration in the workspace, it will prompt you in Databricks the first time you attempt to push.

## Development environment

I'm using this repo as a test of developing Databricks pipelines with a combination of Databrick Workspace GUI, Visual Studio Code and Copilot integration. Below are instructions for how I setup my environment.

### Visual Studio Code

*Extensions*
- Python (Microsoft)
- Flake 8
- Black Formatter
- Databricks
- GitHub Copilot Chat

### Getting started

* Install the requirements by typing
```
pip install -r requirements.txt
```

### Run tests

* To run the tests, you need to run the command:
```python
pip install -e .   # install package using setup.py in editable mode
```
* Then run the tests using 
```
py.test
``` 