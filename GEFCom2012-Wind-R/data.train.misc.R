source("fn.base.R") 

load("data/training.RData")

cols.common.pred <- c("date", "farm", "dist", "wp", "is_key", "is_training")
data.tr <- rbind(data.tr.other, data.test.other, data.tr.key)
data.tr$farm <- as.factor(data.tr$farm)
data.test <- rbind(data.test.key)
data.test$farm <- as.factor(data.test$farm)
rm(data.tr.other, data.test.other, data.tr.key, data.test.key)

packages <- c("gbm","cvTools","Metrics","data.table", "foreach")
fn.libraries(packages)

#############################################################
# gbm - history values
#############################################################
tic()
fn.register.wk()
data.misc.hist.gbm <- foreach (r = 1, .combine=rbind) %do% {
  
  fn.libraries(packages)
  
  k <- 1   

  
  data.tr.misc <- data.tr
  data.test.misc <- data.test
  
  models.tr.gbm <- foreach (k = 1:data.test.k) %dopar% {
    
    fn.libraries(packages)
    
    is.test <- k == data.test.k
    
    sink(paste(wd.path,"/log/gbm_misc_hist", 
               ifelse(is.test, "_test", k),".log", sep=""))
    
    model.tr.gbm <- list()
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.misc)
      data.test.cv.idx <- c()
      model.tr.gbm$data.pred <- data.test.misc
      train.fraction <- 1
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.misc, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.misc, data.cv.idx, k)
      model.tr.gbm$data.pred <- data.tr.misc[data.test.cv.idx,]
      train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.misc)
    }
    
    data.gbm <- rbind(data.tr.misc[data.tr.cv.idx,], 
                      data.tr.misc[data.test.cv.idx,])
    data.weights <- rep(1, nrow(data.gbm))
    data.weights[!data.gbm$is_training] <- 1
    
    model.tr.gbm$model <-
      gbm(
        formula =  wp ~ 
          wp_hn01 + wp_hn02 + wp_hn03 + wp_hn04 + 
          set_seq_cut + hour + month + year + dist + 
          clust.farm + clust + clust.pos + begin,
        data = data.gbm,
        n.trees = 3500,
        interaction.depth = 8,
        n.minobsinnode = 30,
        shrinkage =  0.05,
        distribution = "gaussian",
        train.fraction = train.fraction,
        weights = data.weights
      )
    
    sink()
    
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
  cols.pred <- c(cols.common.pred, "pred.misc.hist.gbm")
  for (k in 1:data.test.k) {
    models.tr.gbm[[k]]$data.pred$pred.misc.hist.gbm <- 
      predict(models.tr.gbm[[k]]$model,
              models.tr.gbm[[k]]$data.pred,
              model.trees)
    
    data.pred.tmp <- models.tr.gbm[[k]]$data.pred
    data.pred.tmp <- data.pred.tmp[!is.na(data.pred.tmp$wp_hn01),]
    data.pred.tmp <- data.pred.tmp[
      data.pred.tmp$is_training 
      | !duplicated(data.pred.tmp[, c("set", "farm", "dist")]),]
    
    data.pred <- rbind(data.pred,
                       data.pred.tmp[,cols.pred])
  }
  
  fn.pred.eval(data.pred[data.pred$is_training,])
  fn.pred.eval(data.pred[!data.pred$is_training,])
  
  data.pred  
  
}
fn.kill.wk()
toc()
fn.pred.eval(data.misc.hist.gbm[data.misc.hist.gbm$is_training,])
# farm      rmse
# 1    1 0.1458868
# 2    2 0.1586102
# 3    3 0.1630799
# 4    4 0.1484378
# 5    5 0.1527065
# 6    6 0.1411164
# 7    7 0.1467217
# 8 <NA> 0.1511051
save(data.misc.hist.gbm, file="data/data.misc.hist.gbm.RData")

#############################################################
# gbm - mixed values
#############################################################
tic()
fn.register.wk()
data.misc.mix.gbm <- foreach (r = 1, .combine=rbind) %do% {
  
  fn.libraries(packages)
  
  k <- 1   
  
  data.tr.misc <- data.tr
  data.test.misc <- data.test
  
  models.tr.gbm <- foreach (k = 1:data.test.k) %dopar% {
    
    fn.libraries(packages)
    
    is.test <- k == data.test.k
    model.name <- paste("gbm_misc_mix", 
               ifelse(is.test, "test", k))
    sink(paste(wd.path,"/log/", 
               model.name,".log", sep=""))
    
    model.tr.gbm <- list()
    
    if (is.test) {
      data.tr.cv.idx <- 1:nrow(data.tr.misc)
      data.test.cv.idx <- c()
      model.tr.gbm$data.pred <- data.test.misc
      train.fraction <- 1
    } else {
      data.tr.cv.idx <- fn.cv.tr.subset(data.tr.misc, data.cv.idx, k)
      data.test.cv.idx <- fn.cv.test.subset(data.tr.misc, data.cv.idx, k)
      model.tr.gbm$data.pred <- data.tr.misc[data.test.cv.idx,]
      train.fraction <- length(data.tr.cv.idx)/nrow(data.tr.misc)
    }
    
    model.tr.gbm$model <-
      gbm(
        formula =  wp ~ 
          ws + wd_cut +
          ws.angle +        
          ws.angle.p1 + ws.angle.p2 + ws.angle.p3 + 
          ws.angle.n1 + ws.angle.n2 + ws.angle.n3 +
          hour + month + year + 
          wp_hn01 + 
          set_seq_cut +
          farm + dist,
        data = rbind(data.tr.misc[data.tr.cv.idx,], 
                     data.tr.misc[data.test.cv.idx,]),
        n.trees = 3500,
        interaction.depth = 8,
        n.minobsinnode = 30,
        shrinkage =  0.05,
        distribution = "gaussian",
        train.fraction = train.fraction,
        keep.data = F
      )
    
    sink()
    save(model.tr.gbm, file = paste(wd.path,"/model/", 
                                    model.name,".RData", sep=""))
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
  cols.pred <- c(cols.common.pred, "pred.misc.mix.gbm")
  for (k in 1:data.test.k) {
    models.tr.gbm[[k]]$data.pred$pred.misc.mix.gbm <- 
      predict(models.tr.gbm[[k]]$model,
              models.tr.gbm[[k]]$data.pred,
              model.trees)
    
    data.pred.tmp <- models.tr.gbm[[k]]$data.pred
    data.pred.tmp <- data.pred.tmp[!is.na(data.pred.tmp$wp_hn01),]
    data.pred.tmp <- data.pred.tmp[
      data.pred.tmp$is_training 
      | !duplicated(data.pred.tmp[, c("set", "farm", "dist")]),]
    
    data.pred <- rbind(data.pred,
                       data.pred.tmp[,cols.pred])
  }
  
  fn.pred.eval(data.pred[data.pred$is_training,])
  fn.pred.eval(data.pred[!data.pred$is_training,])
  
  data.pred  
  
}
toc()
fn.kill.wk()
fn.pred.eval(data.misc.mix.gbm[!data.misc.mix.gbm$is_training,])
# farm      rmse
# 1    1 0.1453459
# 2    2 0.1479235
# 3    3 0.1606840
# 4    4 0.1401418
# 5    5 0.1536353
# 6    6 0.1375856
# 7    7 0.1470418
# 8 <NA> 0.1476583
save(data.misc.mix.gbm, file="data/data.misc.mix.gbm.RData")