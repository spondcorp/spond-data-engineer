# Spond Data Engineer Take-Home Assignment

### pre-commit
This repo uses [pre-commit](https://pre-commit.com/) hooks to manage code formatting among other things.

It is strongly recommended, although not required, to set this up locally before committing to this repo to make sure the CI checks pass successfully.

You can follow the [installation guide](https://pre-commit.com/#installation) here to set it up.

### Docker
To run this in a docker environnment, after following the [installation guide](https://docs.docker.com/engine/install/), run the below command from the repo root

```commandline
docker build -t spond-data-engineer .
```

For an interactive environment run
```commandline
docker container run -it spond-data-engineer /bin/bash
```

Once you are inside the container, you can run the below two commands to execute the code
```commandline
python3 coding-questions/2-1/usa_admins_emails.py
python3 coding-questions/2-2/scd2.py
```
[//]: # (TODO: Add paths)
The output will be available under ...
