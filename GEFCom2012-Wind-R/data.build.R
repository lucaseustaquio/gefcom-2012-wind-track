source("fn.base.R")
 

#############################################################
# training/testing data
#############################################################
library(data.table)
data.hist.dt <- read.csv(fn.dat.raw.dir("train.csv.gz"),
                         colClasses = c("character", "numeric",
                                        "numeric","numeric","numeric",
                                        "numeric","numeric","numeric"))
data.hist.dt$date <- as.POSIXct(
  data.hist.dt$date,
  format = "%Y%m%d%H", tz = "GMT")

data.test.keys <- read.csv(fn.dat.raw.dir("benchmark.csv.gz"),
                           colClasses = c("integer","character", "numeric",
                                          "numeric","numeric","numeric",
                                          "numeric","numeric","numeric"))
data.test.keys$date <- as.POSIXct(
  data.test.keys$date,
  format = "%Y%m%d%H", tz = "GMT")
rownames(data.test.keys) <- data.test.keys$id
data.test.keys$id <- NULL
for (i in 1:7) {
  data.test.keys[[paste("wp",i,sep="")]] <- NA
}
data.hist.dt <- rbind(data.hist.dt, data.test.keys)
data.test.keys <- data.test.keys[, "date", drop = F]

test.diff <- 546
data.tr.keys <- data.test.keys
data.tr.keys$date <- data.tr.keys$date - as.difftime(test.diff, unit="days")

data.test.keys$start <- 
  data.test.keys$date - 
  as.difftime(1:48, unit="hours")
data.test.keys$date <- as.character(data.test.keys$date)
data.test.keys$start <- as.character(data.test.keys$start)
data.test.keys <- data.table(data.test.keys, key="date")

data.tr.keys$start <- 
  data.tr.keys$date - 
    as.difftime(1:48, unit="hours")
data.tr.keys$date <- as.character(data.tr.keys$date)
data.tr.keys$start <- as.character(data.tr.keys$start)
data.tr.keys <- data.table(data.tr.keys, key="date")

data.hist.dt$date <- as.character(data.hist.dt$date)
data.hist.dt <- data.table(data.hist.dt)

data.hist.dt <- data.hist.dt[
  ,list(
    farm = rep(1:7, each = length(wp1)),
    wp = c(wp1,wp2,wp3,wp4,wp5,wp6,wp7)
        ),
  by="date"]


data.hist.dt$date <- as.POSIXct(data.hist.dt$date, tz = "GMT")
data.hist.dt$hour <- as.factor(format(
  data.hist.dt$date, "%H"))
data.hist.dt$month <- as.factor(format(
  data.hist.dt$date, "%m"))
data.hist.dt$year <- as.factor(format(
  data.hist.dt$date, "%Y"))
data.hist.dt$date <- as.character(data.hist.dt$date)
setkeyv(data.hist.dt, c("date", "farm"))

rm(i, test.diff)

save.image()

#############################################################
# wind forecast
#############################################################
library(data.table)
data.forecast.keys.dt <- NULL
data.forecast.others.dt <- NULL
min.forecast <- as.POSIXct(min(data.tr.keys$start), tz = "GMT")
min.forecast <- min.forecast - 
  as.difftime(35, unit="hours")
for (i in 1:7) {
  data.forecast.cur <- read.csv(fn.dat.raw.dir(paste("windforecasts_wf",
                                                i, ".csv.gz", sep="")))
  data.forecast.cur$date <- as.POSIXct(
    as.character(data.forecast.cur$date),
    format = "%Y%m%d%H", tz = "GMT")
  data.forecast.cur$pdate <- data.forecast.cur$date + 
    as.difftime(data.forecast.cur$hors, unit="hours")
  data.forecast.cur <- data.forecast.cur[
    data.forecast.cur$pdate >=min.forecast, ]
  data.forecast.cur$date <- as.character(data.forecast.cur$date)
  data.forecast.cur$pdate <- as.character(data.forecast.cur$pdate)
  data.forecast.cur$wd_cut <- cut(data.forecast.cur$wd, seq(0,360,30),
                                  include.lowest = T)
  data.forecast.cur <- data.forecast.cur[!is.na(data.forecast.cur$ws),]
  data.forecast.cur$farm <- i
  keys.forescast <- data.forecast.cur$date %in% c(data.tr.keys$start,
                                                  data.test.keys$start)
  keys.blacklist <- data.forecast.cur$pdate %in% c(data.tr.keys$date,
                                  data.test.keys$date)
  keys.others <- !keys.forescast & !keys.blacklist 
  keys.tr <- keys.blacklist & data.forecast.cur$pdate %in% c(data.tr.keys$date)
  
  data.forecast.cur$start <- data.forecast.cur$date
  data.forecast.cur$date <- data.forecast.cur$pdate
  data.forecast.cur$dist <- data.forecast.cur$hors
  data.forecast.cur$dist <- as.factor(sprintf("%02d", 
    data.forecast.cur$hors))
  
  data.forecast.cur$start <- as.POSIXct(data.forecast.cur$start, tz = "GMT")
  data.forecast.cur$turn <- as.factor(format(
    data.forecast.cur$start, "%H"))
  data.forecast.cur$start <- as.character(data.forecast.cur$start)
  data.forecast.cur$set <- NA
  data.forecast.cur$set[keys.forescast] <- rep(1:(sum(keys.forescast)/48), 
                                               each=48)
  data.forecast.cur$set[keys.others] <- rep(1:(sum(keys.others)/(36*4)), 
                                               each=(36*4))
  data.forecast.cur$set[keys.tr] <- rep(1:(sum(keys.tr)/(48*4)), 
                                            each=(48*4))
  data.forecast.keys.dt <- rbind(data.forecast.keys.dt, 
                            data.forecast.cur[keys.forescast | keys.tr,])
  data.forecast.others.dt <- rbind(data.forecast.others.dt, 
                                 data.forecast.cur[keys.others,])
}

cols.forecast <- c("date","farm","start", "dist","turn", "set",
                   "ws","wd","wd_cut")
keys.forescast <- c("date", "farm")
data.forecast.keys.dt <- data.table(data.forecast.keys.dt[, cols.forecast],
                                    key = c(keys.forescast))
data.forecast.others.dt <- data.table(data.forecast.others.dt[, cols.forecast],
                                    key = keys.forescast)

data.forecast.all.dt <- rbind(data.forecast.keys.dt,
                              data.forecast.others.dt)
setkeyv(data.forecast.all.dt, keys.forescast)

rm(min.forecast,i,data.forecast.cur,keys.others,
   cols.forecast,keys.forescast,keys.blacklist,keys.tr)

save.image()

#############################################################
# history features
#############################################################
library(data.table)

data.hist2.dt <- data.table(data.hist.dt)
data.hist2.dt <- data.hist2.dt[!is.na(data.hist2.dt$wp),]
setkeyv(data.hist2.dt, c("date", "farm"))

cols.feat <- c("start", "farm", "dist")
data.feat.dt <- data.frame(unique(rbind(
  data.forecast.keys.dt[, 1, by=cols.feat],
  data.forecast.others.dt[, 1, by=cols.feat])))
data.feat.dt$V1 <- NULL

data.feat.dt$start <- as.POSIXct(
  data.feat.dt$start, tz = "GMT")

hist.length <- 6
for (i in 1:hist.length) {
  data.col <- paste("wp_hm",sprintf("%02d", i),sep="")
  data.col.key <- data.table(as.character(data.feat.dt$start - 
    as.difftime(i-1, unit="hours")), 
                             data.feat.dt$farm)
  data.feat.dt[[data.col]] <- data.hist2.dt[data.col.key]$wp
}

for (i in 1:hist.length) {
  data.col <- paste("wp_hp",sprintf("%02d", i),sep="")
  data.colm <- paste("wp_hm",sprintf("%02d", i),sep="")
  data.col.key <- data.table(as.character(data.feat.dt$start + 
    as.difftime(48+i, unit="hours")), 
                             data.feat.dt$farm)
  data.feat.dt[[data.col]] <- data.hist2.dt[data.col.key]$wp
  data.feat.dt[[data.col]][is.na(data.feat.dt[[data.col]])] <- 
    data.feat.dt[[data.colm]][is.na(data.feat.dt[[data.col]])]
}

for (i in 1:hist.length) {
  data.colm <- paste("wp_hm",sprintf("%02d", i),sep="")
  data.colp <- paste("wp_hp",sprintf("%02d", i),sep="")
  data.coln <- paste("wp_hn",sprintf("%02d", i),sep="")
  data.feat.dt[[data.coln]] <- data.feat.dt[[data.colm]]
  data.feat.dt[[data.coln]][as.integer(data.feat.dt$dist) > 24] <- 
    data.feat.dt[[data.colp]][as.integer(data.feat.dt$dist) > 24]
  data.feat.dt[[data.colp]] <- NULL
  data.feat.dt[[data.colm]] <- NULL
}

data.feat.dt$start <- as.character(data.feat.dt$start)
data.feat.dt$dist_prev <- NULL
data.feat.dt$V1 <- NULL
data.feat.dt <- data.table(data.feat.dt, key=c("start", "farm", "dist"))

rm(data.hist2.dt,i,data.col, 
   data.col.key,cols.feat,data.colm,
   data.colp,data.coln)

save.image()

#############################################################
# ws angle feature
#############################################################
tic()
fn.register.wk()
data.farm.ws.dt <- foreach (farm = 1:7, .combine=rbind) %dopar% {
  
  sink(paste(wd.path,"/log/feat_ws_f",farm,".log", sep=""))
  
  library(cvTools)
  library(Metrics)
  library(data.table)
  library(gbm)
  
  data.feat.all <- data.frame(rbind(data.forecast.keys.dt, 
                                    data.forecast.others.dt))
  data.feat.all <- data.feat.all[data.feat.all$farm %in% farm,]
  data.feat.all <- fn.append.dt(data.feat.all,  data.hist.dt)
  data.feat.all$wd_cut <- cut(data.feat.all$wd, seq(0,360,8),
                              include.lowest = T)
  data.feat.all$ws2 <- data.feat.all$ws^2
  data.feat.all$ws3 <- data.feat.all$ws^3
  
  data.tr.farm <- data.feat.all[!is.na(data.feat.all$wp),]
  data.test.farm <- data.feat.all[is.na(data.feat.all$wp),]
  
  library(cvTools)
  data.cv.folds <- 5
  data.test.k <- data.cv.folds+1
  set.seed(13213)
  data.cv.idx <- cvFolds(length(unique(data.feat.all$set)), 
                         K=data.cv.folds)
  
  
  data.pred <- NULL
  for (k in 1:data.test.k) {  
    is.test <- k == data.test.k
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.farm)
      data.test.cv.idx <- c()
      data.cv.pred <- data.test.farm
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.farm, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.farm, data.cv.idx, k)
      data.cv.pred <- data.tr.farm[data.test.cv.idx,]
    }
    models.tr.ws <-
      gbm(
        formula =  wp ~ 
          wd_cut*(ws + ws2 + ws3),
        data = rbind(data.tr.farm[data.tr.cv.idx,],
                     data.tr.farm[data.test.cv.idx,]),
        n.trees = 1000,
        interaction.depth = 3,
        n.minobsinnode = 5,
        shrinkage =  0.01,
        distribution = "gaussian",
        train.fraction = length(data.tr.cv.idx)/
          (length(data.tr.cv.idx)+length(data.test.cv.idx))
      )
    data.cv.pred$ws.angle <- predict(models.tr.ws, data.cv.pred, 
                                     models.tr.ws$n.trees)
    data.pred <- rbind(data.pred, data.cv.pred)
  }
  fn.pred.eval(data.pred)
  
  sink()
  
  data.pred[, c("date", "farm", "dist",
                "wp", "ws.angle")]
}
fn.kill.wk()
toc()
fn.pred.eval(data.farm.ws.dt[
  data.farm.ws.dt$date > max(data.tr.keys$date),], "ws.angle")
# farm      rmse
# 1    1 0.1842614
# 2    2 0.1703720
# 3    3 0.1925604
# 4    4 0.1782320
# 5    5 0.1864862
# 6    6 0.1686556
# 7    7 0.1625098
# 8   NA 0.1778668

data.farm.ws.dt <- fn.ws.time(data.farm.ws.dt)

save.image()

#############################################################
# ws unsupervided features
#############################################################
library(data.table)
data.farm.ws.feat <- data.farm.ws.dt
data.forecast.all.dt <- rbind(data.forecast.keys.dt, 
      data.forecast.others.dt)
setkeyv(data.forecast.all.dt, c("date", "farm", "dist"))
data.farm.ws.feat <- fn.append.dt(data.farm.ws.feat, data.forecast.all.dt)

data.farm.ws.feat$dist <- as.integer(data.farm.ws.feat$dist)
data.farm.ws.feat$dist.quarter <- floor((data.farm.ws.feat$dist-1)/12)+1
data.farm.ws.feat$dist.quarter.pos <- ((data.farm.ws.feat$dist-1)%%12)+1

data.farm.ws.feat <- 
  data.farm.ws.feat[
    ,list(begin = date[dist.quarter.pos==1],
          ws1  = ws.angle[dist.quarter.pos==1],
          ws2  = ws.angle[dist.quarter.pos==2],
          ws3  = ws.angle[dist.quarter.pos==3],
          ws4  = ws.angle[dist.quarter.pos==4],
          ws5  = ws.angle[dist.quarter.pos==5],
          ws6  = ws.angle[dist.quarter.pos==6],
          ws7  = ws.angle[dist.quarter.pos==7],
          ws8  = ws.angle[dist.quarter.pos==8],
          ws9  = ws.angle[dist.quarter.pos==9],
          ws10 = ws.angle[dist.quarter.pos==10],
          ws11 = ws.angle[dist.quarter.pos==11],
          ws12 = ws.angle[dist.quarter.pos==12],
          date1  = date[dist.quarter.pos==1],
          date2  = date[dist.quarter.pos==2],
          date3  = date[dist.quarter.pos==3],
          date4  = date[dist.quarter.pos==4],
          date5  = date[dist.quarter.pos==5],
          date6  = date[dist.quarter.pos==6],
          date7  = date[dist.quarter.pos==7],
          date8  = date[dist.quarter.pos==8],
          date9  = date[dist.quarter.pos==9],
          date10 = date[dist.quarter.pos==10],
          date11 = date[dist.quarter.pos==11],
          date12 = date[dist.quarter.pos==12],
          dist1  = dist[dist.quarter.pos==1],
          dist2  = dist[dist.quarter.pos==2],
          dist3  = dist[dist.quarter.pos==3],
          dist4  = dist[dist.quarter.pos==4],
          dist5  = dist[dist.quarter.pos==5],
          dist6  = dist[dist.quarter.pos==6],
          dist7  = dist[dist.quarter.pos==7],
          dist8  = dist[dist.quarter.pos==8],
          dist9  = dist[dist.quarter.pos==9],
          dist10 = dist[dist.quarter.pos==10],
          dist11 = dist[dist.quarter.pos==11],
          dist12 = dist[dist.quarter.pos==12]
          ),
    by=c("start", "farm", "dist.quarter")]
data.farm.ws.feat$begin <- as.POSIXct(data.farm.ws.feat$begin, 
                                      tz = "GMT")
data.farm.ws.feat$begin <- as.factor(format(
  data.farm.ws.feat$begin, "%H"))

tic()
fn.register.wk()
data.farm.ws.clust.dt <- foreach (farm=1:8, .combine=rbind) %dopar% {
  
  sink(paste(wd.path,"/log/feat_clust_f",farm,".log", sep=""))
  
  library(cvTools)
  library(Metrics)
  library(data.table)
  
  data.clust.farm <- data.frame(data.farm.ws.feat)
  is.farm <- farm < 8
  if (is.farm) {
    clust.centers <- 6
    data.clust.farm <- data.clust.farm[
      data.clust.farm$farm %in% farm,]
  } else {
    clust.centers = 24
  }
  
  rownames(data.clust.farm) <- 1:nrow(data.clust.farm)
  
  x.col <- paste("ws",1:12,sep="")
  model.kmeans <- kmeans(x=data.clust.farm[,x.col],
                        center = clust.centers,
                        iter.max = 30, nstart = 30)
  data.clust.farm$cluster <- model.kmeans$cluster
  cat("Cluster sizes:", model.kmeans$size, "")
  
  sink()
  data.clust.farm$is.farm <- is.farm
  data.frame(data.table(data.clust.farm)[
    ,list(
      date = c(date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,date11,date12),
      farm = rep(farm,12),
      dist = c(dist1,dist2,dist3,dist4,dist5,dist6,dist7,dist8,dist9,dist10,dist11,dist12),
      clust.pos = (1:12),
      clust = rep(cluster, 12),
      begin = rep(begin,12),
      is.farm = rep(is.farm,12)
      ),])
}
fn.kill.wk()
toc()

data.farm.ws.clust.farm.dt <- 
  data.farm.ws.clust.dt[data.farm.ws.clust.dt$is.farm,]
data.farm.ws.clust.farm.dt$is.farm <- NULL
data.farm.ws.clust.farm.dt$clust.farm <- as.factor(
  paste(data.farm.ws.clust.farm.dt$farm, data.farm.ws.clust.farm.dt$clust, sep = "_"))
data.farm.ws.clust.farm.dt$clust <- NULL
data.farm.ws.clust.farm.dt$clust.farm2 <- 
  as.factor(paste(data.farm.ws.clust.farm.dt$clust.farm, 
        (data.farm.ws.clust.farm.dt$dist - 1)%%12, sep="_"))
data.farm.ws.clust.farm.dt$dist <- as.factor(sprintf("%02d", data.farm.ws.clust.farm.dt$dist))
data.farm.ws.clust.farm.dt <- data.table(data.farm.ws.clust.farm.dt, 
                                         key = c("date", "farm", "dist"))

data.farm.ws.clust.dt <- 
  data.farm.ws.clust.dt[!data.farm.ws.clust.dt$is.farm,]
data.farm.ws.clust.dt$is.farm <- NULL
data.farm.ws.clust.dt$clust <- as.factor(data.farm.ws.clust.dt$clust)
data.farm.ws.clust.dt$clust2 <- 
  as.factor(paste(data.farm.ws.clust.dt$clust, 
                  (data.farm.ws.clust.dt$dist - 1)%%12, sep="_"))
data.farm.ws.clust.dt$dist <- as.factor(sprintf("%02d", data.farm.ws.clust.dt$dist))
data.farm.ws.clust.dt <- data.table(data.farm.ws.clust.dt, 
                                         key = c("date", "farm", "dist"))

rm(data.farm.ws.feat)

save.image()

#############################################################
# ws2 angle feature
#############################################################
tic()
fn.register.wk()
data.farm.ws2.dt <- foreach (farm = 1:7, .combine=rbind) %dopar% {
  
  sink(paste(wd.path,"/log/feat_ws_f",farm,".log", sep=""))
  
  library(cvTools)
  library(Metrics)
  library(data.table)
  library(gbm)
  
  data.feat.all <- data.frame(rbind(data.forecast.keys.dt, 
                                    data.forecast.others.dt))
  data.feat.all <- data.feat.all[data.feat.all$farm %in% farm,]
  data.feat.all <- fn.append.dt(data.feat.all,  data.hist.dt)
  data.feat.all <- fn.append.dt(data.feat.all,  data.farm.ws.clust.dt)
  data.feat.all <- fn.append.dt(data.feat.all,  data.farm.ws.clust.farm.dt)
  data.feat.all$wd_cut <- cut(data.feat.all$wd, seq(0,360,8),
                              include.lowest = T)
  data.feat.all$ws2 <- data.feat.all$ws^2
  data.feat.all$ws3 <- data.feat.all$ws^3
  
  data.tr.farm <- data.feat.all[!is.na(data.feat.all$wp),]
  data.test.farm <- data.feat.all[is.na(data.feat.all$wp),]
  
  library(cvTools)
  data.cv.folds <- 5
  data.test.k <- data.cv.folds+1
  set.seed(13213)
  data.cv.idx <- cvFolds(length(unique(data.feat.all$set)), 
                         K=data.cv.folds)
  
  
  data.pred <- NULL
  for (k in 1:data.test.k) {  
    is.test <- k == data.test.k
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.farm)
      data.test.cv.idx <- c()
      data.cv.pred <- data.test.farm
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.farm, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.farm, data.cv.idx, k)
      data.cv.pred <- data.tr.farm[data.test.cv.idx,]
    }
    models.tr.ws <-
      gbm(
        formula =  wp ~ 
          wd_cut*(ws + ws2 + ws3) + 
          clust.farm + clust.pos + clust + begin,
        data = rbind(data.tr.farm[data.tr.cv.idx,],
                     data.tr.farm[data.test.cv.idx,]),
        n.trees = 1000,
        interaction.depth = 3,
        n.minobsinnode = 5,
        shrinkage =  0.008,
        distribution = "gaussian",
        train.fraction = length(data.tr.cv.idx)/
          (length(data.tr.cv.idx)+length(data.test.cv.idx))
      )
    data.cv.pred$ws.angle <- predict(models.tr.ws, data.cv.pred, 
                                     models.tr.ws$n.trees)
    data.pred <- rbind(data.pred, data.cv.pred)
  }
  fn.pred.eval(data.pred)
  
  sink()
  
  data.pred <- data.pred[, c("date", "farm", "dist",
                "wp", "ws.angle")]
  colnames(data.pred) <- gsub("ws.angle", "ws2.angle", colnames(data.pred))
  
  data.pred
}
fn.kill.wk()
toc()
data.farm.ws2.dt <- data.table(data.farm.ws2.dt, key = key(data.farm.ws.dt))
data.farm.ws2.dt <- fn.ws.time(data.farm.ws2.dt)
setnames(data.farm.ws2.dt,gsub("ws.angle", "ws2.angle", colnames(data.farm.ws2.dt)))

fn.pred.eval(data.farm.ws2.dt[
  data.farm.ws2.dt$date > max(data.tr.keys$date),], "ws2.angle")
# farm      rmse
# 1    1 0.1759325
# 2    2 0.1612061
# 3    3 0.1841361
# 4    4 0.1669643
# 5    5 0.1779388
# 6    6 0.1630376
# 7    7 0.1616986
# 8   NA 0.1703411

save.image()


#############################################################
# building dataset
#############################################################
library(data.table)

data.feat.all <- rbind(data.forecast.keys.dt, 
                       data.forecast.others.dt)
data.feat.all <- fn.append.dt(data.feat.all,  data.hist.dt)
data.feat.all <- fn.append.dt(data.feat.all,  data.farm.ws.dt)
data.feat.all <- fn.append.dt(data.feat.all,  data.farm.ws2.dt)
data.feat.all <- fn.append.dt(data.feat.all,  data.farm.ws.clust.farm.dt)
data.feat.all <- fn.append.dt(data.feat.all,  data.farm.ws.clust.dt)

col.first <- c("date", "farm", "wp")
col.feat <- colnames(data.feat.all)
col.feat <- col.feat[!(col.feat %in% col.first)]
data.feat.all <- data.feat.all[,
                               c(col.first, col.feat),
                               with = F]
data.feat.all$dist_cut <- cut(as.integer(data.feat.all$dist),
                              seq(0,48,3))

data.feat.all <- fn.append.dt(data.feat.all,  data.feat.dt)
data.feat.all$is_key <- data.feat.all$date %in% 
  data.forecast.keys.dt$date
data.feat.all$is_training <- data.feat.all$date <=
  max(data.tr.keys$date)

data.feat.all <- data.frame(data.feat.all)
cols.scale <- colnames(data.feat.all)[grepl("wp.+",colnames(data.feat.all))]

library(caret)
model.scale <- preProcess(data.feat.all[,cols.scale,drop=F])

data.feat.all[,cols.scale] <- predict(model.scale, data.feat.all[,cols.scale,drop=F])

data.feat.all$dist_half <- 
  cut(as.integer(data.feat.all$dist), c(-Inf,24,Inf))
      
data.feat.all$dist_cut <- 
  cut(as.integer(data.feat.all$dist), seq(0,48,3))

set.size <- max(data.feat.all$set)/4
data.feat.all$set_seq_cut <- as.factor(floor((data.feat.all$set-1)/set.size))

library(cvTools)
data.cv.folds <- 5
data.test.k <- data.cv.folds+1
set.seed(13213)
data.cv.idx <- cvFolds(length(unique(data.feat.all$set)), 
                       K=data.cv.folds)

data.tr.key <- data.feat.all[
  data.feat.all$is_key & data.feat.all$is_training,]
data.test.key <- data.feat.all[
  data.feat.all$is_key & !data.feat.all$is_training,]

data.tr.other <- data.feat.all[
  !data.feat.all$is_key & data.feat.all$is_training,]
data.test.other <- data.feat.all[
  !data.feat.all$is_key & !data.feat.all$is_training,]

save(data.tr.key,data.test.key,
     data.tr.other,data.test.other,
     data.cv.idx, data.test.k,
     data.test.keys,
     file = "data/training.RData")

data.farm.ws2.dt <- data.table(data.farm.ws2.dt, key = key(data.farm.ws.dt))
