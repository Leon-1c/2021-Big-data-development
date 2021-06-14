from flask import Flask, render_template, request, make_response
import jdbc
app = Flask(__name__)


@app.route('/')
def login():
    return render_template('login.html')


@app.route('/init', methods=['POST', 'GET'])
def init():
    if request.method == 'POST':
        result = request.form
        net = result['Net']
        port = result['Port']
        db = result['Db']
        user = result['User']
        password = result['Password']
        url = jdbc.get_url(net, port, db)

        resp = make_response(render_template('search.html', result=result))
        resp.set_cookie('User', user)
        resp.set_cookie('Password', password)
        resp.set_cookie('Url', url)

        return resp
        # return render_template("result.html", result=result)


@app.route('/search', methods=['POST', 'GET'])
def show():
    if request.method == 'POST':
        sql = request.form['Sql']
        user = request.cookies.get('User')
        password = request.cookies.get('Password')
        url = request.cookies.get('Url')
        sql_head, sql_body = jdbc.search(user, password, url, sql)
        # print(result.get('Name'))
        return render_template("table.html", table_head=sql_head, table_body=sql_body)


if __name__ == '__main__':
    app.run(debug=True)
