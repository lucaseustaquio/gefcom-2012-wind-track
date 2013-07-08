source("fn.base.R") 

#############################################################
# ensemble
#############################################################
library(data.table)
k <- 1
load("data/training.RData")
cols.feat <- c("date", "farm", "dist", "dist_cut", "dist_half", "turn", 
               "month", "year", "hour", "set", "set_seq_cut",
               "is_key", "is_training")
data.feat <- data.table(
  rbind(data.tr.other[,cols.feat], data.tr.key[,cols.feat],
    data.test.other[,cols.feat], data.test.key[,cols.feat]),
  key = c("date", "farm", "dist"))

load("data/data.farm.forecast.gbm.RData")
data.farm.forecast.gbm$farm <- as.factor(data.farm.forecast.gbm$farm)
load("data/data.farm.hist.gbm.RData")
data.farm.hist.gbm$farm <- as.factor(data.farm.hist.gbm$farm)
load("data/data.farm.mix.gbm.RData")
data.farm.mix.gbm$farm <- as.factor(data.farm.mix.gbm$farm)

load("data/data.dist.forecast.gbm.RData")
data.dist.forecast.gbm$farm <- as.factor(data.dist.forecast.gbm$farm)
load("data/data.dist.hist.gbm.RData")
data.dist.hist.gbm$farm <- as.factor(data.dist.hist.gbm$farm)
load("data/data.dist.mix.gbm.RData")
data.dist.mix.gbm$farm <- as.factor(data.dist.mix.gbm$farm)

load("data/data.misc.forecast.gbm.RData")
data.misc.forecast.gbm$farm <- as.factor(data.misc.forecast.gbm$farm)
load("data/data.misc.hist.gbm.RData")
data.misc.hist.gbm$farm <- as.factor(data.misc.hist.gbm$farm)
load("data/data.misc.mix.gbm.RData")
data.misc.mix.gbm$farm <- as.factor(data.misc.mix.gbm$farm)

data.pred.ens <- fn.merge(c("date","farm", "dist"),
                          data.farm.forecast.gbm,
                          data.farm.hist.gbm,
                          data.farm.mix.gbm,
                          
                          data.dist.forecast.gbm,
                          data.dist.hist.gbm,
                          data.dist.mix.gbm,
                          
                          data.misc.forecast.gbm,
                          data.misc.hist.gbm,
                          data.misc.mix.gbm)

data.pred.ens$farm <- as.factor(data.pred.ens$farm)
data.pred.ens <- fn.append.dt(data.pred.ens,data.feat)
                              

library(Metrics)
test.val <- !is.na(data.pred.ens$wp)  & !data.pred.ens$is_training
cols.pred <- colnames(data.pred.ens)[grepl("pred\\.",colnames(data.pred.ens))]
for (col.pred in cols.pred) {
  if (is.numeric(data.pred.ens[[col.pred]])) {
    cat("RMSE:", rmse(data.pred.ens$wp[test.val], 
                      data.pred.ens[[col.pred]][test.val]), "-", col.pred,"\n")
  }
}
# RMSE: 0.1518441 - pred.farm.forecast.gbm 
# RMSE: 0.163189 - pred.farm.hist.gbm 
# RMSE: 0.1512711 - pred.farm.mix.gbm 
# RMSE: 0.1498218 - pred.dist.forecast.gbm 
# RMSE: 0.1575373 - pred.dist.hist.gbm 
# RMSE: 0.1488073 - pred.dist.mix.gbm 
# RMSE: 0.1504932 - pred.misc.forecast.gbm 
# RMSE: 0.1568915 - pred.misc.hist.gbm 
# RMSE: 0.1476583 - pred.misc.mix.gbm 
#############################################################
# all models ensembling
#############################################################
tic()
fn.register.wk()
data.pred.ens.all <- foreach (k = 1:data.test.k, .combine=rbind) %dopar% {

  library(cvTools)
  library(Metrics)
  
  is.test <- k == data.test.k
  
  sink(paste(wd.path,"/log/ens",
             ifelse(is.test, "_test", paste("_cv",k,sep="")),
             ".log", sep=""))
  
  tr.rows <- which(!is.na(data.pred.ens$wp))
  if (is.test) {
    data.tr.cv.idx <- tr.rows
    data.test.cv.idx <- which(is.na(data.pred.ens$wp))
  } else {
    data.tr.cv.idx <- fn.cv.tr.subset(data.pred.ens, data.cv.idx, k)
    data.tr.cv.idx <- data.tr.cv.idx[data.tr.cv.idx %in% tr.rows]
    data.test.cv.idx <- fn.cv.test.subset(data.pred.ens, data.cv.idx, k)
    data.test.cv.idx <- data.test.cv.idx[data.test.cv.idx %in% tr.rows]
  }
  
  model.ens.formula = wp ~ farm + 
      pred.farm.forecast.gbm + 
      pred.farm.mix.gbm +
      pred.farm.hist.gbm + 
      
      pred.dist.forecast.gbm + 
      pred.dist.hist.gbm +
      pred.dist.mix.gbm + 
      
      pred.misc.mix.gbm + 
      pred.misc.hist.gbm
      
      dist + dist_cut:(
        pred.farm.forecast.gbm + 
          pred.farm.mix.gbm +
          pred.farm.hist.gbm + 
          
          pred.dist.forecast.gbm + 
          pred.dist.hist.gbm +
          pred.dist.mix.gbm +  
          
          pred.misc.mix.gbm + 
          pred.misc.hist.gbm)
  
 model.ens <- glm(formula = model.ens.formula,
                  
      data = data.pred.ens[data.tr.cv.idx,])

  data.pred <- data.pred.ens[data.test.cv.idx,]
  data.pred$pred.ens <- 
    suppressWarnings(predict(model.ens, data.pred))
  sink()
  data.pred
}
fn.kill.wk()
toc()
all.test.val <- !is.na(data.pred.ens.all$wp) & !data.pred.ens.all$is_training
cat("RMSE:", rmse(data.pred.ens.all$wp[all.test.val], 
                  data.pred.ens.all$pred.ens[all.test.val]), "- ensemble\n")
# RMSE: 0.1472225 - ensemble

# RMSE: 0.1467883 - ensemble

#############################################################
# smoothing features
#############################################################

library(data.table)
data.feat.smooth <- data.pred.ens.all
data.feat.smooth <- data.feat.smooth[data.feat.smooth$is_key | data.feat.smooth$is_training, ]
data.feat.smooth <- data.table(data.feat.smooth)
data.feat.smooth$dist <- as.integer(data.feat.smooth$dist)
data.feat.smooth$date.pos <- as.POSIXct(data.feat.smooth$date, tz = "GMT")
setkeyv(data.feat.smooth, c("date", "farm", "dist"))

data.feat.smooth.border <- data.table(
  unique(data.pred.ens.all[, c("date", "farm", "wp")]), key = c("date", "farm"))

for (i in c(-3:-1,1:3)) {
  
  ens.col <- paste("pred.ens.", ifelse(i < 0, "p", "n"), 
                   abs(i), sep = "")
  smooth.key <- data.table(
    date = as.character(data.feat.smooth$date.pos + as.difftime(i, unit="hours")),
    farm = data.feat.smooth$farm,
    dist = data.feat.smooth$dist + i)
  
  data.feat.smooth[[ens.col]] <- 
    data.feat.smooth[smooth.key]$pred.ens
  
  border.key.idx <- smooth.key$dist < 1 | smooth.key$dist > 48
  border.key <- smooth.key[border.key.idx,]
  border.key$dist <- NULL
  
  data.feat.smooth[[ens.col]][border.key.idx] <- 
    data.feat.smooth.border[border.key]$wp
  
  smooth.na <- is.na(data.feat.smooth[[ens.col]])
  data.feat.smooth[[ens.col]][smooth.na] <-
    data.feat.smooth$pred.ens[smooth.na]
}

data.feat.smooth <- data.frame(data.feat.smooth)
data.feat.smooth$dist <- as.factor(data.feat.smooth$dist)

#############################################################
# smooth training
#############################################################
tic()
fn.register.wk()
data.pred.smooth <- foreach (k = 1:data.test.k, .combine=rbind) %dopar% {
  
  library(cvTools)
  library(Metrics)
  
  is.test <- k == data.test.k
  
  tr.rows <- which(!is.na(data.feat.smooth$wp))
  if (is.test) {
    data.tr.cv.idx <- tr.rows
    data.test.cv.idx <- which(is.na(data.feat.smooth$wp))
  } else {
    data.tr.cv.idx <- fn.cv.tr.subset(data.feat.smooth, data.cv.idx, k)
    data.tr.cv.idx <- data.tr.cv.idx[data.tr.cv.idx %in% tr.rows]
    data.test.cv.idx <- fn.cv.test.subset(data.feat.smooth, data.cv.idx, k)
    data.test.cv.idx <- data.test.cv.idx[data.test.cv.idx %in% tr.rows]
  }
  
  model.smooth <- glm(formula = 
    wp ~  
    pred.ens.p1 + pred.ens.p2 + pred.ens.p3 +
    pred.ens.n1 + pred.ens.n2 + pred.ens.n3 +
    pred.ens,
    data = data.feat.smooth[data.tr.cv.idx,])
  
  data.pred <- data.feat.smooth[data.test.cv.idx,]
  data.pred$pred.smooth <- 
    suppressWarnings(predict(model.smooth, data.pred))

  data.pred
}
fn.kill.wk()
toc()

smooth.test.val <- !is.na(data.pred.smooth$wp)
cat("RMSE old:", rmse(data.pred.smooth$wp[smooth.test.val], 
                      data.pred.smooth$pred.ens[smooth.test.val]), "- smooth,",
    "RMSE new:", rmse(data.pred.smooth$wp[smooth.test.val], 
                      data.pred.smooth$pred.smooth[smooth.test.val]), "- smooth\n")
# RMSE old: 0.1414407 - smooth, RMSE new: 0.1411419 - smooth
# RMSE old: 0.1409356 - smooth, RMSE new: 0.1406673 - smooth

#############################################################
# submission
#############################################################
data.pred.ens.dt <- data.table(data.pred.smooth)
data.pred.ens.dt$pred.ens <- pmax(0,pmin(max(data.pred.ens.dt$wp, na.rm = T), 
                                  data.pred.ens.dt$pred.smooth))
data.pred.ens.dt$date <- as.factor(data.pred.ens.dt$date)
data.pred.ens.dt$farm <- as.factor(data.pred.ens.dt$farm)
setkeyv(data.pred.ens.dt, c("date", "farm"))
data.ens.submit <- data.frame(
  id = as.integer(rownames(data.test.keys)),
  date = format(as.POSIXct(data.test.keys$date, tz = "GMT"), 
                "%Y%m%d%H"))
for (i in 1:7) {
  col <- paste("wp",i,sep="")
  data.ens.keys <- data.table(date = as.factor(data.test.keys$date),
                             farm = as.factor(i))
  data.ens.submit[[col]] <- data.pred.ens.dt[data.ens.keys]$pred.ens
}
write.csv(data.ens.submit, 
          file = gzfile("./data/data.ens.submit.csv.gz"),
          row.names = F, quote = F)