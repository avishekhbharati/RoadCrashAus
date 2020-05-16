# Flask S3 Browser

Creating a virtual environment is recommended.

Creating virtual environment using Python 3 installed with Homebrew:
```shell
virtualenv -p python3 venv --always-copy
source venv/bin/activate

For Windows users
PS C:\DEV\aProject\env\Scripts>.\activate

or using git-bash
source venv/Scripts/activate
```

Install Dependencies

```shell
pip3 install -r requirements.txt
```

## Configuration

Create a new file `.env` using the contents of `.env-sample`. If you are not using the AWS CLI, modify the placeholders to add your AWS credentials and bucket name.
