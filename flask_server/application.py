from flask import Flask, render_template, request, redirect, url_for, flash, Response, session
from flask_bootstrap import Bootstrap
from filters import datetimeformat, file_type
from resources import get_bucket, get_buckets_list
from flask_cors import CORS

#application varibale is for aws. As it looks for this key
application = app = Flask(__name__)
Bootstrap(app)

#allows CORS for all domains on all routes
CORS(app)

app.secret_key = 'secret'
app.jinja_env.filters['datetimeformat'] = datetimeformat
app.jinja_env.filters['file_type'] = file_type


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        bucket = request.form['bucket']
        session['bucket'] = bucket
        return redirect(url_for('files'))
    else:
        buckets = get_buckets_list()
        return render_template("index.html", buckets=buckets)


@app.route('/files')
def files():
    my_bucket = get_bucket()
    summaries = my_bucket.objects.all()
    print(summaries)

    return render_template('files.html', my_bucket=my_bucket, files=summaries)


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']

    my_bucket = get_bucket()
    fileList = my_bucket.objects.all()

    #check if the file is duplicate
    for data in fileList:
        if data.key == file.filename:
            flash('Upload Failed!! File with same name already exists.')
            return redirect(url_for('files'))

    my_bucket.Object(file.filename).put(Body=file)

    flash('File uploaded successfully')
    return redirect(url_for('files'))


@app.route('/delete', methods=['POST'])
def delete():
    key = request.form['key']

    my_bucket = get_bucket()
    my_bucket.Object(key).delete()

    flash('File deleted successfully')
    return redirect(url_for('files'))


@app.route('/download', methods=['POST'])
def download():
    key = request.form['key']

    my_bucket = get_bucket()
    file_obj = my_bucket.Object(key).get()

    return Response(
        file_obj['Body'].read(),
        mimetype='text/plain',
        headers={"Content-Disposition": "attachment;filename={}".format(key)}
    )

@app.route('/a', methods=['GET', 'POST'])
def test():
    return "Hello"

    

if __name__ == "__main__":
    app.run(host="localhost", debug=True)
