source("fn.base.R") 

load("data/training.RData")

cols.common.pred <- c("date", "farm", "dist", "wp", "is_key", "is_training")
data.tr <- rbind(data.tr.other, data.test.other, data.tr.key)
data.test <- rbind(data.test.key)
rm(data.tr.other, data.test.other, data.tr.key, data.test.key)

tr.farm.params <- expand.grid(farm = 1:7, dist = levels(data.tr$dist_half))
tr.farm.params <- tr.farm.params[order(tr.farm.params$farm),]
rownames(tr.farm.params) <- 1:nrow(tr.farm.params)
r <- 1 # for test purposes
farm <- 1 # for test purposes
k <- 1 # for test purposes

packages <- c("gbm","cvTools","Metrics","data.table", "foreach")
fn.libraries(packages)

#############################################################
# gbm - Forescast prediction
#############################################################
tic()
fn.register.wk()
data.farm.forecast.gbm <- foreach (farm = 1:7, .combine=rbind) %dopar% {

  fn.libraries(packages)
  
  sink(paste(wd.path,"/log/gbm_forecast_f",farm,
             ".log", sep=""))
  
  
  data.tr.farm <- data.tr[data.tr$farm %in% farm,]
  data.test.farm <- data.test[data.test$farm %in% farm,]
  
  models.tr.gbm <- foreach (k = 1:data.test.k)  %do% {  
    
    fn.libraries(packages)
    
    model.tr.gbm <- list()
    is.test <- k == data.test.k
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.farm)
      data.test.cv.idx <- c()
      model.tr.gbm$data.pred <- data.test.farm
      train.fraction <- 1
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.farm, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.farm, data.cv.idx, k)
      model.tr.gbm$data.pred <- data.tr.farm[data.test.cv.idx,]
      train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.farm)
    }
    
    data.gbm <- rbind(data.tr.farm[data.tr.cv.idx,], 
                      data.tr.farm[data.test.cv.idx,])
    data.weights <- rep(1, nrow(data.gbm))
    data.weights[!data.gbm$is_training] <- 1

    model.tr.gbm$model <-
      gbm(
        formula =  wp ~ 
          ws + 
          ws.angle + 
          ws.angle.p1 + ws.angle.p2 + ws.angle.p3 + 
          ws.angle.n1 + ws.angle.n2 + ws.angle.n3 +
          hour + month + year +
          dist + 
          set_seq_cut,
        data = data.gbm,
        n.trees = 2000,
        interaction.depth = 12,
        n.minobsinnode = 30,
        shrinkage =  0.01,
        distribution = "gaussian",
        train.fraction = train.fraction,
        weights = data.weights
      )
    model.tr.gbm
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
                 "pred.farm.forecast.gbm.clust", 
                 "pred.farm.forecast.gbm")
  for (k in 1:data.test.k) {
    models.tr.gbm[[k]]$data.pred$pred.farm.forecast.gbm <- 
      predict(models.tr.gbm[[k]]$model,
              models.tr.gbm[[k]]$data.pred,
              model.trees)
    models.tr.gbm[[k]]$data.pred$pred.farm.forecast.gbm.clust <-
      as.character(farm)
    data.pred <- rbind(data.pred,
                       models.tr.gbm[[k]]$data.pred[,cols.pred])
  }
  
  data.pred$pred.farm.forecast.gbm.clust <- 
    as.factor(data.pred$pred.farm.forecast.gbm.clust)
  
  fn.pred.eval(data.pred[data.pred$is_training,])
  fn.pred.eval(data.pred[!data.pred$is_training,])
  
  sink()

  data.pred  
}
fn.kill.wk()
toc()
fn.pred.eval(data.farm.forecast.gbm[!data.farm.forecast.gbm$is_training,])
# farm      rmse
# 1    1 0.1496007
# 2    2 0.1529860
# 3    3 0.1686836
# 4    4 0.1470595
# 5    5 0.1650272
# 6    6 0.1450074
# 7    7 0.1502064
# 8   NA 0.1543136
save(data.farm.forecast.gbm, file="data/data.farm.forecast.gbm.RData")

#############################################################
# gbm - History values
#############################################################
tic()
fn.register.wk()
data.farm.hist.gbm <- foreach (r = 1:nrow(tr.farm.params), .combine=rbind) %dopar% {
  
  fn.libraries(packages)
  
  farm <- tr.farm.params[r, "farm"]
  dist <- tr.farm.params[r, "dist"]
  k <- 1
  
  sink(paste(wd.path,"/log/gbm_hist_f",farm,"_", dist,
             ".log", sep=""))

  data.tr.farm <- data.tr[data.tr$farm %in% farm
                          & data.tr$dist_half %in% dist,]
  data.test.farm <- data.test[data.test$farm %in% farm
                              & data.test$dist_half %in% dist,]
  
  models.tr.gbm <- list()
  
  for (k in 1:data.test.k) {  
   models.tr.gbm[[k]] <- list()
   is.test <- k == data.test.k
   
   if (is.test) {
     data.tr.cv.idx <- 1:nrow(data.tr.farm)
     data.test.cv.idx <- c()
     models.tr.gbm[[k]]$data.pred <- data.test.farm
     train.fraction <- 1
   } else {
     data.tr.cv.idx <- fn.cv.tr.subset(data.tr.farm, data.cv.idx, k)
     data.test.cv.idx <- fn.cv.test.subset(data.tr.farm, data.cv.idx, k)
     models.tr.gbm[[k]]$data.pred <- data.tr.farm[data.test.cv.idx,]
     train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.farm)
   }
   
   data.gbm <- rbind(data.tr.farm[data.tr.cv.idx,], 
                     data.tr.farm[data.test.cv.idx,])
   data.weights <- rep(1, nrow(data.gbm))
   
   models.tr.gbm[[k]]$model <-
     gbm(
       formula =  wp ~ 
         wp_hn01 + wp_hn02 + wp_hn03 + wp_hn04 + 
         dist + set_seq_cut + hour + month + year + 
         clust.farm + clust + begin + clust.pos,
       data = data.gbm,
       n.trees = 1500,
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
                 "pred.farm.hist.gbm.clust", 
                 "pred.farm.hist.gbm")
  for (k in 1:data.test.k) {
   models.tr.gbm[[k]]$data.pred$pred.farm.hist.gbm <- 
     predict(models.tr.gbm[[k]]$model,
             models.tr.gbm[[k]]$data.pred,
             model.trees)
   
   data.pred.tmp <- models.tr.gbm[[k]]$data.pred
   data.pred.tmp <- data.pred.tmp[!is.na(data.pred.tmp$wp_hn01),]
   data.pred.tmp <- data.pred.tmp[
     data.pred.tmp$is_training 
     | !duplicated(data.pred.tmp[, c("set", "farm", "dist")]),]
   
   data.pred.tmp$pred.farm.hist.gbm.clust <- 
     paste(farm, dist, sep = "_")
   data.pred <- rbind(data.pred,
                      data.pred.tmp[,cols.pred])
  }
  data.pred$pred.farm.hist.gbm.clust <- 
    as.factor(data.pred$pred.farm.hist.gbm.clust)
  
  fn.pred.eval(data.pred[data.pred$is_training,])
  fn.pred.eval(data.pred[!data.pred$is_training,])
  
  sink()
  
  data.pred  
  
}
fn.kill.wk()
toc()
fn.pred.eval(data.farm.hist.gbm[!data.farm.hist.gbm$is_training,])
# farm      rmse
# 1    1 0.1603988
# 2    2 0.1691851
# 3    3 0.1743400
# 4    4 0.1544081
# 5    5 0.1714164
# 6    6 0.1482569
# 7    7 0.1594681
# 8   NA 0.1627362
save(data.farm.hist.gbm, file="data/data.farm.hist.gbm.RData")

#############################################################
# gbm - mixed values
#############################################################
tic()
fn.register.wk()
data.farm.mix.gbm <- foreach (r = 1:nrow(tr.farm.params), .combine=rbind) %dopar% {
  
  fn.libraries(packages)
  
  farm <- tr.farm.params[r, "farm"]
  dist <- tr.farm.params[r, "dist"]
  k <- 1
  
  sink(paste(wd.path,"/log/gbm_mix_f",farm,"_", dist,
                        ".log", sep=""))

   data.tr.farm <- data.tr[data.tr$farm %in% farm
                           & data.tr$dist_half %in% dist,]
   data.test.farm <- data.test[data.test$farm %in% farm
                             & data.test$dist_half %in% dist,]
   
  models.tr.gbm <- foreach (k = 1:data.test.k)  %do% {  
    
    fn.libraries(packages)
    
    model.tr.gbm <- list()
    is.test <- k == data.test.k
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.farm)
      data.test.cv.idx <- c()
      model.tr.gbm$data.pred <- data.test.farm
      train.fraction <- 1
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.farm, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.farm, data.cv.idx, k)
      model.tr.gbm$data.pred <- data.tr.farm[data.test.cv.idx,]
      train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.farm)
    }
    
    data.gbm <- rbind(data.tr.farm[data.tr.cv.idx,], 
                      data.tr.farm[data.test.cv.idx,])
    data.weights <- rep(1, nrow(data.gbm))
    data.weights[!data.gbm$is_training] <- 1
    
    model.tr.gbm$model <-
      gbm(
        formula =  wp ~ 
          ws + 
          ws.angle + 
          ws.angle.p1 + ws.angle.p2 + ws.angle.p3 + 
          ws.angle.n1 + ws.angle.n2 + ws.angle.n3 +
          hour + month + year + 
          dist + 
          set_seq_cut +
          wp_hn01,
        data = data.gbm,
        n.trees = 2000,
        interaction.depth = 12,
        n.minobsinnode = 30,
        shrinkage =  0.01,
        distribution = "gaussian",
        train.fraction = train.fraction,
        weights = data.weights
      )
    model.tr.gbm
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
                  "pred.farm.mix.gbm.clust",
                  "pred.farm.mix.gbm")
   for (k in 1:data.test.k) {
     models.tr.gbm[[k]]$data.pred$pred.farm.mix.gbm <- 
       predict(models.tr.gbm[[k]]$model,
               models.tr.gbm[[k]]$data.pred,
               model.trees)
     
     data.pred.tmp <- models.tr.gbm[[k]]$data.pred
     data.pred.tmp <- data.pred.tmp[!is.na(data.pred.tmp$wp_hn01),]
     data.pred.tmp <- data.pred.tmp[
       data.pred.tmp$is_training 
        | !duplicated(data.pred.tmp[, c("set", "farm", "dist")]),]
     
     data.pred.tmp$pred.farm.mix.gbm.clust <- 
       paste(farm, dist, sep = "_")
     
     data.pred <- rbind(data.pred,
                        data.pred.tmp[,cols.pred])
  }
  
  data.pred$pred.farm.mix.gbm.clust <-
      as.factor(data.pred$pred.farm.mix.gbm.clust)
    
  fn.pred.eval(data.pred)
  
  sink()
   
  data.pred  
   
}
fn.kill.wk()
toc()
fn.pred.eval(data.farm.mix.gbm[!data.farm.mix.gbm$is_training,])
# farm      rmse
# 1    1 0.1488398
# 2    2 0.1538101
# 3    3 0.1659575
# 4    4 0.1425018
# 5    5 0.1584712
# 6    6 0.1395162
# 7    7 0.1478593
# 8   NA 0.1512329
save(data.farm.mix.gbm, file="data/data.farm.mix.gbm.RData")