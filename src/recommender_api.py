# 啟動Flask
from flask import Flask
app = Flask(__name__)

@app.route('/hello_world')
def hello_world():
    return "Hello World!"

@app.route("/recommendProducts/<name>")
def recommendProducts(name):
    recommendP= model.recommendProducts(int(name),5)
    return_str = ""
    for p in recommendP:
        print  "對使用者"+ str(p[0]) + "推薦電影"+ str(movieTitle[p[1]]) + "推薦評分"+ str(p[2])
        return_str+="對使用者"+ str(p[0]) + "推薦電影"+ str(movieTitle[p[1]]) + "推薦評分"+ str(p[2]) +"<br/>"
    return return_str

@app.route('/recommendUsers/<item>')
def recommendUsers(item):
    recommendU= model.recommendUsers(int(item),5)
    return_str = ""
    for u in recommendU:
        print "對電影"+ str(movieTitle[u[1]]) + "推薦使用者"+ str(u[0]) + "推薦評分"+ str(u[2])
        return_str+="對電影"+ str(movieTitle[u[1]]) + "推薦使用者"+ str(u[0]) + "推薦評分"+ str(u[2]) + "<br/>"
    return return_str

app.run(host="0.0.0.0", port=5000)
