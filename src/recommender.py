
Path="/user/cloudera/"
rawUserData = sc.textFile(Path+"ratings_1k_noheader.csv")
rawUserData.count()

# 顯示第一筆評分資料
rawUserData.first()

# 顯示前五筆評分資料
rawUserData.take(5)

#轉換評價資料格式
rawRatings = rawUserData.map(lambda line: line.split(",")[:3] )
#rawRatings.take(5)
ratingsRDD = rawRatings.map(lambda x: (x[0],x[1],x[2]))
ratingsRDD.take(5)

# 計算評價資料筆數
numRatings = ratingsRDD.count()
numRatings

# 計算評價資料筆數
numRatings = ratingsRDD.count()
numRatings

# 計算評價資料中不重複之評價人數量
numUsers = ratingsRDD.map(lambda x: x[0] ).distinct().count()
numUsers

# 計算評價資料中不重複之評價電影數量
numMovies = ratingsRDD.map(lambda x: x[1]).distinct().count()
numMovies
#ratingsRDD.persist()

# 建立顯示評價模型 ALS.train(ratings,rank,iterations,lambda)
from pyspark.mllib.recommendation import ALS
model = ALS.train(ratingsRDD, 10, 10, 0.01)
print model

# 儲存model到HDFS
model.save(sc,"/user/root/model")
#model.recommendProducts(100,5)

# 讀取儲存的model再使用
sameModel = pyspark.mllib.recommendation.MatrixFactorizationModel.load(sc,"/user/root/model")
sameModel.recommendUsers(product=100,num=5)
#model.recommendUsers(product=100,num=5)

# 針對編號100的電影推薦使用者,取最高分的5位
model.recommendUsers(product=100,num=5)

# 預測使用者對電影的喜好分數
model.predict(7, 100)

# 讀取電影名稱資料
Path="/user/cloudera/"
itemRDD = sc.textFile(Path+"movies_noheader.csv")
itemRDD.count()
itemRDD.first()

# 轉換電影名稱為RDD collectAsMap()去重複K,V
movieTitle = itemRDD.map( lambda line : line.split(",")).map(lambda a: (float(a[0]),a[1])).collectAsMap()
movieTitle[1003]
#len(movieTitle)

# 列出前5部電影
movieTitle.items()[:5]

# Popular Movie
ratingsRDD1 = rawRatings.map(lambda x: (x[0],x[1]))
groupMovie = ratingsRDD1.groupBy(lambda x: x[1]).collect()
groupMovie
orderMovie = sorted([(len(y),x) for (x, y) in groupMovie])
orderMovie.reverse()
for x in orderMovie:
    print str(x[0]),movieTitle[int(x[1])]

# 針對使用者7推薦電影
recommendP= model.recommendProducts(7,5)
for p in recommendP:
    print  "對使用者"+ str(p[0]) + "推薦電影: "+ str(movieTitle[p[1]]) + "\t推薦評分"+ str(p[2])



