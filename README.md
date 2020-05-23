# Flask S3 Browser

Install pip3
```
sudo apt update
sudo apt install python3-pip
pip3 --version

#update setup tools
pip3 install --upgrade setuptools
sudo apt-get install python3.6-dev libmysqlclient-dev
```


Creating a virtual environment is recommended.

Creating virtual environment using Python 3 installed with Homebrew:
```shell
sudo apt install python3-venv
pip3 install virtualenv
cd flask_api

python3 -m venv venv
source venv/bin/activate
```

Install Dependencies

```shell
pip3 install -r requirements.txt
```

## Configuration

Create a new file `.env` using the contents of `.env-sample`. If you are not using the AWS CLI, modify the placeholders to add your AWS credentials and bucket name.
