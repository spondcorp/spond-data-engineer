# Spond Data Engineer Take-Home Assignment

## Repo Structure
All the original files are unchanged, except for the README.md file which has been renamed to assignment.md

Answers to the conceptual question can be found under the [conceptual_questions](conceptual_questions) directory.
Each question is addressed in a separate markdown file.

Answers to the coding questions can be found in their separate directories under the
[coding_questions](coding_questions) directory.

## Running the code
You can run this code locally using a CLI or IDE, or you can run it in a docker container.
You can find instructions below on how to do either.

### Run Locally
To run this code locally, make sure you have [pip](https://pip.pypa.io/en/stable/installation/#) installed and run

```shell
pip install -r requirements.txt
```
If you're running in an IDE, then you're good to go! For CLI runs, keep reading...

To make sure the utils can be accessed, run these commands to add it to the PYTHONPATH


<details><summary><b>MacOS & Linux</b></summary>

```shell
PYTHONPATH="<your_path>/spond-data-engineer/coding_questions:$PYTHONPATH"
export PYTHONPATH
```

</details>

<details><summary><b>Windows</b></summary>

```shell
set PYTHONPATH=%PYTHONPATH%;<your_path>\spond-data-engineer\coding_questions
```

</details>

Finally, run the scripts from the repo root using
```shell
python3 coding_questions/2-1/usa_admins_emails.py
python3 coding_questions/2-2/scd2.py
```


### Run in Docker
To run the scripts in a docker environment, run the below command from the repo root
 after following the [Docker installation guide](https://docs.docker.com/engine/install/)

```shell
docker build -t spond-data-engineer .
```

For an interactive environment run
```shell
docker container run -it spond-data-engineer /bin/bash
```

Once you are inside the container, you can run the below  commands to execute the code
```shell
python3 coding_questions/2-1/usa_admins_emails.py
python3 coding_questions/2-2/scd2.py
```
[//]: # (TODO: Add paths)
The output will be available under ...

Or simply run the image and the two scripts will execute
```shell
docker run spond-data-engineer
```

### pre-commit
This repo uses [pre-commit](https://pre-commit.com/) hooks to manage code style and formatting, among other things.
It is strongly recommended, although not required, to set this up locally before committing to this repo to make
sure the CI checks pass successfully.

You can follow the [installation guide](https://pre-commit.com/#installation) to set it up.
