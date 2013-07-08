source("fn.base.R") 

load("data/training.RData")

cols.common.pred <- c("date", "farm", "dist", "wp", "is_key", "is_training")
data.tr <- rbind(data.tr.other, data.test.other, data.tr.key)
data.tr$farm <- as.factor(data.tr$farm)
data.test <- rbind(data.test.key)
data.test$farm <- as.factor(data.test$farm)
rm(data.tr.other, data.test.other, data.tr.key, data.test.key)

tr.dist.params <- expand.grid(dist = levels(data.tr$dist_cut))
tr.dist.params <- tr.dist.params[order(tr.dist.params$dist),,drop=F]
rownames(tr.dist.params) <- 1:nrow(tr.dist.params)
r <- 1 # for test purposes
dist <- tr.dist.params[1, "dist"] # for test purposes

packages <- c("gbm","cvTools","Metrics","data.table", "foreach", 
              "parallel")
fn.libraries(packages)

#############################################################
# gbm - Wind forescast model
#############################################################
tic()
fn.register.wk()
data.dist.forecast.gbm <- foreach (r = 1:nrow(tr.dist.params), .combine=rbind) %dopar% {
  
  fn.libraries(packages)
  
  dist <- tr.dist.params[r, "dist"]
  k <- 1
  
  sink(paste(wd.path,"/log/gbm_forecast_dist",dist,
             ".log", sep=""))
  
  data.tr.dist <- data.tr[data.tr$dist_cut %in% dist,]
  data.test.dist <- data.test[data.test$dist_cut %in% dist,]
  
  models.tr.gbm <- list()
  
  for (k in 1:data.test.k) {  
    models.tr.gbm[[k]] <- list()
    is.test <- k == data.test.k
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.dist)
      data.test.cv.idx <- c()
      models.tr.gbm[[k]]$data.pred <- data.test.dist
      train.fraction <- 1
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.dist, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.dist, data.cv.idx, k)
      models.tr.gbm[[k]]$data.pred <- data.tr.dist[data.test.cv.idx,]
      train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.dist)
    }
    
    data.gbm <- rbind(data.tr.dist[data.tr.cv.idx,], 
                      data.tr.dist[data.test.cv.idx,])
    data.weights <- rep(1, nrow(data.gbm))
    data.weights[!data.gbm$is_training] <- 1
    
    models.tr.gbm[[k]]$model <-
      gbm(
        formula =  wp ~ 
          ws + wd_cut +   
          ws.angle + 
          ws.angle.p1 + ws.angle.p2 + ws.angle.p3 + 
          ws.angle.n1 + ws.angle.n2 + ws.angle.n3 +
          hour + month + 
          farm + 
          set_seq_cut,
        data = data.gbm,
        n.trees = 3000,
        interaction.depth = 8,
        n.minobsinnode = 30,
        shrinkage =  0.01,
        distribution = "gaussian",
        train.fraction = train.fraction,
        weights = data.weights
      )
  }
  model.trees <- 0
  for (k in 1:data.cv.idx$K) {
    model.trees <- model.trees + gbm.perf(models.tr.gbm[[k]]$model, 
                                          method = "test", plot.it = F)
  }
  model.trees <- round(model.trees/data.cv.idx$K)
  cat("Best test trees:", model.trees, "\n")
  
  print(summary( models.tr.gbm[[data.test.k]]$model, 
                 n.trees = data.test.k, 
                 plotit = F))
  
  data.pred <- NULL
  cols.pred <- c(cols.common.pred, 
                 "pred.dist.forecast.gbm.clust",
                 "pred.dist.forecast.gbm")
  for (k in 1:data.test.k) {
    models.tr.gbm[[k]]$data.pred$pred.dist.forecast.gbm <- 
      predict(models.tr.gbm[[k]]$model,
              models.tr.gbm[[k]]$data.pred,
              model.trees)
    models.tr.gbm[[k]]$data.pred$pred.dist.forecast.gbm.clust <-
      paste(dist)
    data.pred <- rbind(data.pred,
                       models.tr.gbm[[k]]$data.pred[,cols.pred])
  }
  data.pred$pred.dist.forecast.gbm.clust <- 
    as.factor(data.pred$pred.dist.forecast.gbm.clust)
  
  has.wp <- !is.na(data.pred$wp)
  cat("RMSE:", rmse(data.pred$wp[has.wp], 
                    data.pred$pred.dist.forecast.gbm[has.wp]), "\n")
  sink()
  
  data.pred
}
fn.kill.wk()
toc()
fn.pred.eval(data.dist.forecast.gbm[!data.dist.forecast.gbm$is_training,])
# farm      rmse
# 1    1 0.1531824
# 2    2 0.1509242
# 3    3 0.1665140
# 4    4 0.1481117
# 5    5 0.1620348
# 6    6 0.1459440
# 7    7 0.1511933
# 8 <NA> 0.1541434
save(data.dist.forecast.gbm, file="data/data.dist.forecast.gbm.RData")

#############################################################
# gbm - History model
#############################################################
tic()
fn.register.wk()
data.dist.hist.gbm <- foreach (r = 1:nrow(tr.dist.params), .combine=rbind) %dopar% {
  
  fn.libraries(packages)
  
  dist <- tr.dist.params[r, "dist"]
  k <- 1
  
  sink(paste(wd.path,"/log/gbm_hist_dist",dist,
          ".log", sep=""))
  
  data.tr.dist <- data.tr[data.tr$dist_cut %in% dist,]
  data.test.dist <- data.test[data.test$dist_cut %in% dist,]
  
  models.tr.gbm <- list()
  
  for (k in 1:data.test.k) {  
      models.tr.gbm[[k]] <- list()
      is.test <- k == data.test.k
      
      if (is.test) {
        data.tr.cv.idx <- 1:nrow(data.tr.dist)
        data.test.cv.idx <- c()
        models.tr.gbm[[k]]$data.pred <- data.test.dist
        train.fraction <- 1
      } else {
        data.tr.cv.idx <- fn.cv.tr.subset(data.tr.dist, data.cv.idx, k)
        data.test.cv.idx <- fn.cv.test.subset(data.tr.dist, data.cv.idx, k)
        models.tr.gbm[[k]]$data.pred <- data.tr.dist[data.test.cv.idx,]
        train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.dist)
      }
      
      data.gbm <- rbind(data.tr.dist[data.tr.cv.idx,], 
                        data.tr.dist[data.test.cv.idx,])
      data.weights <- rep(1, nrow(data.gbm))
      data.weights[!data.gbm$is_training] <- 1
      
      models.tr.gbm[[k]]$model <-
        gbm(
          formula =  wp ~ 
            wp_hn01 + wp_hn02 + wp_hn03 + wp_hn04 + 
            set_seq_cut + hour + month + year + dist + 
            clust.farm + clust + clust.pos + begin,
          data = data.gbm,
          n.trees = 2500,
          interaction.depth = 8,
          n.minobsinnode = 30,
          shrinkage =  0.03,
          distribution = "gaussian",
          train.fraction = train.fraction,
          weights = data.weights
        )
  }
  model.trees <- 0
  for (k in 1:data.cv.idx$K) {
    model.trees <- model.trees + gbm.perf(models.tr.gbm[[k]]$model, 
                                         method = "test", plot.it = F)
  }
  model.trees <- round(model.trees/data.cv.idx$K)
  cat("Best test trees:", model.trees, "\n")
  
  print(summary( models.tr.gbm[[data.test.k]]$model, 
              n.trees = data.test.k, 
              plotit = F))
  
  data.pred <- NULL
  cols.pred <- c(cols.common.pred, 
                 "pred.dist.hist.gbm.clust",
                 "pred.dist.hist.gbm")
  for (k in 1:data.test.k) {
    models.tr.gbm[[k]]$data.pred$pred.dist.hist.gbm <- 
     predict(models.tr.gbm[[k]]$model,
             models.tr.gbm[[k]]$data.pred,
             model.trees)
    
    data.pred.tmp <- models.tr.gbm[[k]]$data.pred
    data.pred.tmp <- data.pred.tmp[!is.na(data.pred.tmp$wp_hn01),]
    data.pred.tmp <- data.pred.tmp[
      data.pred.tmp$is_training 
      | !duplicated(data.pred.tmp[, c("set", "farm", "dist")]),]
    
    data.pred.tmp$pred.dist.hist.gbm.clust <-
      paste(dist)
    data.pred <- rbind(data.pred,
                       data.pred.tmp[,cols.pred])
  }
  data.pred$pred.dist.hist.gbm.clust <- 
    as.factor(data.pred$pred.dist.hist.gbm.clust)
  
  fn.pred.eval(data.pred)
  sink()
  
  data.pred
}
fn.kill.wk()
toc()
fn.pred.eval(data.dist.hist.gbm[!data.dist.hist.gbm$is_training,])
# farm      rmse
# 1    1 0.1533026
# 2    2 0.1607960
# 3    3 0.1687806
# 4    4 0.1500818
# 5    5 0.1664092
# 6    6 0.1444300
# 7    7 0.1568856
# 8 <NA> 0.1574500

save(data.dist.hist.gbm, file="data/data.dist.hist.gbm.RData")

#############################################################
# gbm - Mix model
#############################################################
tic()
fn.register.wk()
data.dist.mix.gbm <- foreach (r = 1:nrow(tr.dist.params), .combine=rbind) %dopar% {
  
  fn.libraries(packages)
  
  dist <- tr.dist.params[r, "dist"]
  k <- 1
  
  sink(paste(wd.path,"/log/gbm_mix_dist",dist,
            ".log", sep=""))

  
  data.tr.dist <- data.tr[data.tr$dist_cut %in% dist,]
  data.test.dist <- data.test[data.test$dist_cut %in% dist,]
  
  models.tr.gbm <- list()
  
  for (k in 1:data.test.k) {  
   models.tr.gbm[[k]] <- list()
   is.test <- k == data.test.k
   
   if (is.test) {
     data.tr.cv.idx <- 1:nrow(data.tr.dist)
     data.test.cv.idx <- c()
     models.tr.gbm[[k]]$data.pred <- data.test.dist
     train.fraction <- 1
   } else {
     data.tr.cv.idx <- fn.cv.tr.subset(data.tr.dist, data.cv.idx, k)
     data.test.cv.idx <- fn.cv.test.subset(data.tr.dist, data.cv.idx, k)
     models.tr.gbm[[k]]$data.pred <- data.tr.dist[data.test.cv.idx,]
     train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.dist)
   }
   
   data.gbm <- rbind(data.tr.dist[data.tr.cv.idx,],
                     data.tr.dist[data.test.cv.idx,])
   data.weights <- rep(1, nrow(data.gbm))
   data.weights[!data.gbm$is_training] <- 1
   
   models.tr.gbm[[k]]$model <-
     gbm(
       formula =  wp ~ 
         ws + wd_cut +
         ws.angle +        
         ws.angle.p1 + ws.angle.p2 + ws.angle.p3 + 
         ws.angle.n1 + ws.angle.n2 + ws.angle.n3 +
         hour + month + 
         farm + 
         set_seq_cut +
         wp_hn01,
       data = data.gbm,
       n.trees = 2500,
       interaction.depth = 5,
       n.minobsinnode = 30,
       shrinkage =  0.02,
       distribution = "gaussian",
       train.fraction = train.fraction,
       weights = data.weights
     )
  }
  model.trees <- 0
  for (k in 1:data.cv.idx$K) {
   model.trees <- model.trees + gbm.perf(models.tr.gbm[[k]]$model, 
                                         method = "test", plot.it = F)
  }
  model.trees <- round(model.trees/data.cv.idx$K)
  cat("Best test trees:", model.trees, "\n")
  
  print(summary( models.tr.gbm[[data.test.k]]$model, 
                n.trees = data.test.k, 
                plotit = F))
  
  data.pred <- NULL
  cols.pred <- c(cols.common.pred, 
                 "pred.dist.mix.gbm.clust",
                 "pred.dist.mix.gbm")
  for (k in 1:data.test.k) {
   models.tr.gbm[[k]]$data.pred$pred.dist.mix.gbm <- 
     predict(models.tr.gbm[[k]]$model,
             models.tr.gbm[[k]]$data.pred,
             model.trees)
   
   data.pred.tmp <- models.tr.gbm[[k]]$data.pred
   data.pred.tmp <- data.pred.tmp[!is.na(data.pred.tmp$wp_hn01),]
   data.pred.tmp <- data.pred.tmp[
     data.pred.tmp$is_training 
     | !duplicated(data.pred.tmp[, c("set", "farm", "dist")]),]
   
   data.pred.tmp$pred.dist.mix.gbm.clust <-
     paste(dist)
   data.pred <- rbind(data.pred,
                      data.pred.tmp[,cols.pred])
  }
  data.pred$pred.dist.mix.gbm.clust <- 
    as.factor(data.pred$pred.dist.mix.gbm.clust)
  

  
  fn.pred.eval(data.pred)
  
  sink()
  
  data.pred
}
fn.kill.wk()
toc()
fn.pred.eval(data.dist.mix.gbm[!data.dist.mix.gbm$is_training,])
# farm      rmse
# 1    1 0.1467280
# 2    2 0.1488733
# 3    3 0.1617312
# 4    4 0.1406530
# 5    5 0.1537902
# 6    6 0.1383493
# 7    7 0.1485310
# 8 <NA> 0.1485578

save(data.dist.mix.gbm, file="data/data.dist.mix.gbm.RData")
