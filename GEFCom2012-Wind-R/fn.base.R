require("compiler")
enableJIT(3) 
setCompilerOptions(suppressUndefined = T)
options(stringsAsFactors = FALSE)

wd.path <- normalizePath(getwd())

#############################################################
# data folder
#############################################################
fn.dat.raw.dir <- function(fname) {
  return (paste("../data/raw/", fname, sep = ""))
}

#############################################################
# index of
#############################################################
fn.index.of <- function(val, strs, fixed = T) {
  return (which((grepl(val, strs, fixed = fixed))))
}

#############################################################
# data plot
#############################################################
fn.hist.plot <- function(col.plot, filter.month = c("01")) {
  library(data.table)
  data.plot.dt <- fn.append.dt(data.feat.dt,  date.dt)
  data.plot.dt <- data.plot.dt[!is.na(data.plot.dt$wp),]
  data.plot.dt <- data.plot.dt[data.plot.dt$month %in% filter.month,]
  data.plot.dt <- data.plot.dt[, mean(wp), by=c(col.plot, "farm")]
  setkeyv(data.plot.dt, col.plot)
  
  for (i in 1:7) {
    data.plot.cur <- data.frame(data.plot.dt[data.plot.dt$farm == i,])
    feature <- as.factor(gsub(" ", "_", do.call(paste, data.plot.cur[, col.plot])))
    power <- data.plot.cur$V1
    if (i == 1) {
      plot(feature, power, type = "p", col = i, 
           ylim = c(0,max(data.plot.dt$V1)))
    } else {
      lines(feature, power, col = i, type="p")
    }
    
  }
  invisible(NULL)
}

#############################################################
# find na
#############################################################
fn.find.na <- function(dfrm) {
  lapply(dfrm, function(x) which(is.na(x) ) )
}

#############################################################
# row max e min
#############################################################
fn.row.max <- function(dfrm) {
  apply(dfrm, 1, function(x) max(x) ) 
}

fn.row.min <- function(dfrm) {
  apply(dfrm, 1, function(x) min(x) ) 
}

##############################################################
## Registers parallel workers
##############################################################
fn.register.wk <- function(n.proc = NULL) {
  if (is.null(n.proc)) {
    n.proc = as.integer(Sys.getenv("NUMBER_OF_PROCESSORS"))
    if (is.na(n.proc)) {
      library(parallel)
      n.proc <-detectCores()
    }
  }
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (!exists(".pworkers", envir=baseenv()) || length(workers$.pworkers) == 0) {

      library(doSNOW)
      library(foreach)
      suppressWarnings(workers<-makePSOCKcluster(n.proc));
      registerDoSNOW(workers)
      clusterEvalQ(workers, Sys.getpid())
      clusterSetupRNG(workers)
      assign(".pworkers", workers, envir=baseenv());
  }
  invisible(workers);
}

##############################################################
## Kill parallel workers
##############################################################
fn.kill.wk <- function() {
  library(doSNOW)
  library(foreach)
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (exists(".pworkers", envir=baseenv()) && length(workers$.pworkers) != 0) {
    stopCluster(workers$.pworkers);
    assign(".pworkers", NULL, envir=baseenv());
  }
  invisible(workers);
}

##############################################################
## clean parallel workers
##############################################################
fn.clean.wk <- function() {
  library(doSNOW)
  library(foreach)
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (exists(".pworkers", envir=baseenv()) && length(workers$.pworkers) != 0) {
    clusterCall(workers$.pworkers, 
                 function() { rm(list = ls(envir=.GlobalEnv), envir=.GlobalEnv); gc(); })
  }
  invisible(workers);
}

##################################################
# write libfm data
##################################################
fn.write.libfm <- function(
  data.tr, 
  data.test, 
  name = "data",
  dir = "../data/libfm",
  col.y = "score",
  col.x = colnames(data.tr)[!(colnames(data.tr) %in% col.y)])
{
  library(data.table)
  
  model.dts <- list()
  val.xi <- 0
  col.x.groups <- NULL
  feat.size <- 0
  for (i in (1:length(col.x))) {
    col <- col.x[i]
    if (is.factor(data.test[[col]])) {
      col.ids.test <- as.factor(levels(data.test[[col]]))
      col.ids.tr   <- as.factor(levels(data.test[[col]]))
      
      model.dt1 <- data.table(ID = col.ids.test, key = "ID")
      model.dt2 <- data.table(ID = col.ids.tr, key = "ID")
      model.dts[[col]] <- merge(model.dt1, model.dt2)
      feat.size <- feat.size + nrow(model.dts[[col]])
      
      model.dts[[col]]$X.Idx <- val.xi:(val.xi+nrow(model.dts[[col]])-1)
#       col.x.groups[model.dts[[col]]$X.Idx+1] <- (i-1)
      
      val.xi <- val.xi+nrow(model.dts[[col]])
      rm(model.dt1, model.dt2)
      
    } else {
      model.dts[[col]] <- val.xi
#       col.x.groups[val.xi+1] <- (i-1)
      val.xi <- val.xi + 1
    }
  }
  
  write.file <- cmpfun(function (data, file) { 
    col.chunk <- col.x
    if (!is.null(data[[col.y]])) {
      col.chunk <- c(col.y, col.x)
    }
    unlink(file)
    cat("Saving ", file, "\n")
    fileConn <- file(file, open="at")
   
    data.chunk <- data[, col.chunk]
    if (is.null(data.chunk[[col.y]])) {
      data.chunk[[col.y]] <- 0
    }
    for (col in col.x) {
      if (is.factor(data.chunk[[col]])) {
        data.chunk[[col]] <-  paste(
          model.dts[[col]][J(data.chunk[[col]])]$X.Idx,
          c(1), sep = ":")
      } else {
        data.chunk[[col]] <- paste(
          rep(model.dts[[col]], nrow(data.chunk)),
          data.chunk[[col]], sep = ":")
      }
    }
    data.chunk <- do.call(paste, data.chunk[, c(col.y, col.x)])
    data.chunk <- gsub(" [0-9]+\\:?NA", "", data.chunk)
    data.chunk <- gsub(" NA\\:[0-9]+", "", data.chunk)
    data.chunk <- gsub(" [0-9]+\\:0 ", " ", data.chunk)
    data.chunk <- gsub(" [0-9]+\\:0$", "", data.chunk)
    data.chunk <- gsub(" [0-9]+\\:0(\\.0+) ", " ", data.chunk)
    data.chunk <- gsub(" [0-9]+\\:0(\\.0+)$", "", data.chunk)
    writeLines(c(data.chunk), fileConn)

    close(fileConn)
    cat("\n")
  })
  write.file(data.tr, paste(dir, "/", name, ".tr.libfm", sep=""))
  write.file(data.test, paste(dir, "/", name, ".test.libfm", sep=""))
  
}
# debug(fn.write.libfm)

##################################################
# write vw data
##################################################
fn.write.vw <- function(
  data.tr, 
  data.test, 
  name = "data",
  dir = "../data/vw",
  col.y = "wp",
  col.x = colnames(data.tr)[!(colnames(data.tr) %in% col.y)])
{
  library(data.table)
  
  model.dts <- list()
  val.xi <- 0
  col.x.groups <- NULL
  feat.size <- 0
  for (i in (1:length(col.x))) {
    col <- col.x[i]
    if (is.factor(data.test[[col]])) {
      col.ids.test <- as.factor(levels(data.test[[col]]))
      col.ids.tr   <- as.factor(levels(data.test[[col]]))
      
      model.dt1 <- data.table(ID = col.ids.test, key = "ID")
      model.dt2 <- data.table(ID = col.ids.tr, key = "ID")
      model.dts[[col]] <- merge(model.dt1, model.dt2)
      feat.size <- feat.size + nrow(model.dts[[col]])
      
      model.dts[[col]]$X.Idx <- val.xi:(val.xi+nrow(model.dts[[col]])-1)
      
      val.xi <- val.xi+nrow(model.dts[[col]])
      rm(model.dt1, model.dt2)
      
    } else {
      model.dts[[col]] <- val.xi
      val.xi <- val.xi + 1
    }
  }
  
  write.file <- function (data, file) { 
    col.chunk <- col.x
    if (!is.null(data[[col.y]])) {
      col.chunk <- c(col.y, col.x)
    }
    unlink(file)
    cat("Saving ", file, "\n")
    fileConn <- file(file, open="at")
    
    data.chunk <- data[, col.chunk]
    if (is.null(data.chunk[[col.y]])) {
      data.chunk[[col.y]] <- 0
    }
    for (col in col.x) {
      if (is.factor(data.chunk[[col]])) {
        data.chunk[[col]] <-  paste(
          model.dts[[col]][J(data.chunk[[col]])]$X.Idx,
          c(1), sep = ":")
      } else {
        data.chunk[[col]] <- paste(
          rep(model.dts[[col]], nrow(data.chunk)),
          data.chunk[[col]], sep = ":")
      }
    }
    data.chunk <- do.call(paste, data.chunk[, c(col.y, col.x)])
    chunk.size <- as.numeric(object.size(data.chunk))
    chunk.size.ch <- T
    while (chunk.size.ch) {
      data.chunk <- gsub(" [0-9]+\\:?NA", "", data.chunk)
      data.chunk <- gsub(" NA\\:-?[0-9]+", "", data.chunk)
      data.chunk <- gsub(" [0-9]+\\:0\\s+", " ", data.chunk)
      data.chunk <- gsub(" [0-9]+\\:0$", "", data.chunk)
      data.chunk <- gsub(" [0-9]+\\:0(\\.0+)\\s+", " ", data.chunk)
      data.chunk <- gsub(" [0-9]+\\:0(\\.0+)$", "", data.chunk)
      chunk.size.ch <- chunk.size != as.numeric(object.size(data.chunk))
      chunk.size <- as.numeric(object.size(data.chunk))
    }
    data.chunk <- gsub("^([0-9]+(\\.[0-9]+)?)\\s+", "\\1 | ", data.chunk)
    writeLines(c(data.chunk), fileConn)
    
    close(fileConn)
    cat("\n")
  }
#   debug(write.file)
  write.file(data.tr, paste(dir, "/", name, ".tr.vw", sep=""))
  write.file(data.test, paste(dir, "/", name, ".test.vw", sep=""))
}

##################################################
# degree 2 radian
##################################################
fn.deg2rad <- function (x) { x*pi/180 }

##################################################
# sin degree
##################################################
fn.sin.deg <- function (x) { sin(fn.deg2rad(x)) }

##################################################
# cos degree
##################################################
fn.cos.deg <- function (x) { cos(fn.deg2rad(x)) }

##################################################
# append variables by key
##################################################
fn.append.dt <- function (df.val, df.append, 
                          col.append = NULL,
                          na.repl = T) {
  library(data.table)
  col.key <- key(df.append)
  if (is.null(col.append)) {
    col.append <- colnames(df.append)[!(colnames(df.append) %in% col.key)]
  }
  if (is.data.table(df.val)) {
    data.key <- df.val[,col.key,with=F]
  } else {
    data.key <- df.val[,col.key]
  }
  df.append <- df.append[data.key]
  for (col in col.append) {
    df.val.col.old <- df.val[[col]]
    df.val[[col]] <- df.append[[col]]
    if (!na.repl && !is.null(df.val.col.old)) {
      df.val[[col]][is.na(df.val[[col]])] <- 
        df.val.col.old[is.na(df.val[[col]])]
    }
  }
  df.val 
}
##################################################
# append vector components
##################################################
fn.append.vec.dt <- function (df.val, col.vec) {
  library(data.table)
  col.vec.pos <- paste(col.vec, "p", sep="")
  df.val[[col.vec.pos]] <- 0
  df.val[[col.vec.pos]][df.val[[col.vec]] > 0] <- 
    abs(df.val[[col.vec]][df.val[[col.vec]] > 0])
  
  col.vec.neg <- paste(col.vec, "n", sep="")
  df.val[[col.vec.neg]] <- 0
  df.val[[col.vec.neg]][df.val[[col.vec]] < 0] <- 
    abs(df.val[[col.vec]][df.val[[col.vec]] < 0])
  
  df.val 
}
#debug(fn.append.vec.dt)
##################################################
# append variables by key
##################################################
fn.append.hist.dt <- function (df.val, df.hist) {
  library(data.table)
  col.key <- key(df.hist)
  data.key <- df.val[,col.key,with=F]
  df.append <- df.hist[data.table(data.key)]
#   data.has.wp <- !is.na(df.val$wp)
#   df.append$mean[data.has.wp] <- 
#     (df.append$sum[data.has.wp] - df.val$wp[data.has.wp])/
#     (df.append$sz[data.has.wp]-1)
  data.col <- col.key[!(col.key %in% "farm")]
  data.col <- paste(c("wp",data.col), collapse="_")
  df.val[[data.col]] <- df.append$mean
  df.val
}

##################################################
# apply formula to dataset
##################################################
fn.apply.formula <- function(formula, data, 
                             has.out = length(grep("\\~", 
                                                   as.character(formula))) > 0) { 
  formula <-as.character(formula)
  if (length(formula) == 3) {
    formula <- paste(formula[2], formula[1], formula[3])
  }
  if (!has.out) {
    col.dummy = "col.dummy";
    formula <- paste(col.dummy, "~", formula);
    data[[col.dummy]] <- rep(0, NROW(data));
  }
  if (length(grep("1", formula)) == 0) {
    formula <- paste(formula, " - 1 ");
  }
  
  mf <- model.frame(as.formula(formula), data);
  Y <- model.response(mf, "any")
  if (length(dim(Y)) == 1L) {
    nm <- rownames(Y)
    dim(Y) <- NULL
    if (!is.null(nm)) 
      names(Y) <- nm
  }
  
  mt <- attr(mf, "terms")
  X <- data.frame(model.matrix(as.formula(formula), mf, contrasts))
  result <- (list(X=X, Y=Y));
  if (!has.out) {
    result <- result$X;
  }
  return (result);
}

#############################################################
# cv subset
#############################################################
fn.cv.tr.subset <- function(data.tr, cv.idx, k) {
  sets <- cv.idx$subsets[cv.idx$which != k,1]
  which(data.tr$set %in% sets)
}

fn.cv.test.subset <- function(data.tr, cv.idx, k) {
  sets <-cv.idx$subsets[cv.idx$which == k,1]
  which(data.tr$set %in% sets)
}

#############################################################
# prediction evaluation
#############################################################
fn.pred.eval <- function(data.pred, 
                         pred.col = colnames(data.pred)[length(colnames(data.pred))]) {
  library(Metrics)
  library(data.table)
  data.pred <- data.pred[!is.na(data.pred$wp),]
  
  rmse.overall <- rmse(data.pred$wp, data.pred[[pred.col]])
  data.pred$pred.val <- data.pred[[pred.col]]
  data.pred <- data.table(data.pred)[
    ,list(rmse = rmse(wp, pred.val)),
    by="farm"]
  data.pred <- rbind(data.frame(data.pred),
                     data.frame(farm = NA, rmse = rmse.overall))
  print(data.pred)
                         
  invisible(data.pred)                       
}

#############################################################
# tic toc
#############################################################
tic <- function(gcFirst = TRUE, type=c("elapsed", "user.self", "sys.self"))
{
  type <- match.arg(type)
  assign(".type", type, envir=baseenv())
  if(gcFirst) gc(FALSE)
  tic <- proc.time()[type]         
  assign(".tic", tic, envir=baseenv())
  invisible(tic)
}

toc <- function()
{
  type <- get(".type", envir=baseenv())
  toc <- proc.time()[type]
  tic <- get(".tic", envir=baseenv())
  print(toc - tic)
  invisible(toc)
}

#############################################################
# is.between
#############################################################
fn.is.between <- function(x, a, b) {
  x >= a & x <= b
}

#############################################################
# is.between
#############################################################
fn.ma.rep.na <- function(x) {
  idx.nas <- which(is.na(x))
  if (max(idx.nas) < length(x)) {
    x[idx.nas] <- x[max(idx.nas)+1]
  } else {
    x[idx.nas] <- x[min(idx.nas)-1] 
  }
  x
}

#############################################################
# combine.factors
#############################################################
fn.combine.factors <- function(df, cols) {
  col.name <- paste(cols, collapse = "_")
  df[[col.name]] <- "";
  for (col in cols){
    df[[col.name]] <- paste(df[[col.name]], df[[col]], sep = "_") 
  }
  df[[col.name]] <- as.factor(df[[col.name]])
  df
}

#############################################################
# combine.factors
#############################################################
fn.merge <- function(keys, ...) {
  library(data.table)
  dts <- list(...)
  dt.merge <- data.table(dts[[1]], key=keys)
  col.blacklist <- colnames(dt.merge)[!(colnames(dt.merge) %in% keys)]
  for (i in 2:length(dts)) {
    dt.merge2 <- data.table(dts[[i]], key=keys)
    dt.merge2 <- dt.merge2[, 
                           colnames(dt.merge2)[
                             !(colnames(dt.merge2) %in% col.blacklist)],
                                               with = F]
    dt.merge <- merge(dt.merge, dt.merge2)
  }
  if (!is.data.table(dts[[1]])) {
    dt.merge <- data.frame(dt.merge)
  }
  dt.merge
}

#############################################################
# combine.factors
#############################################################
fn.libraries <- function(packages) {
  for (package in packages) {
    library(package, character.only = T)
  }
} 

#############################################################
# fill ws model with previous/next values
#############################################################
fn.ws.time <- function(data.farm.ws.dt) {
  library(data.table)
  data.farm.ws.dt <- data.table(data.farm.ws.dt)
  data.farm.ws.dt$dist <- as.integer(data.farm.ws.dt$dist)
  data.farm.ws.dt$date.time <- 
    as.POSIXct(data.farm.ws.dt$date, tz = "GMT")
  setkeyv(data.farm.ws.dt, c("date","farm","dist"))
  
  data.farm.ws.dt.border <- data.table(
    unique(data.farm.ws.dt[, c("date", "farm", "wp"), with = F]), 
    key = c("date", "farm"))
  
  ws.rng <- c(-3:-1,1:3)
  i <- ws.rng[1]
  for (i in ws.rng) {
    
    ws.col <- paste("ws.angle.", ifelse(i < 0, "p", "n"), 
                    abs(i), sep = "")
    
    i.inc <- rep(i, nrow(data.farm.ws.dt))
    i.inc <- pmin(i.inc, 48 - data.farm.ws.dt$dist)
    ws.key <- data.table(
      date = as.character(data.farm.ws.dt$date.time + as.difftime(i.inc, unit="hours")),
      farm = data.farm.ws.dt$farm,
      dist = data.farm.ws.dt$dist + i.inc)
    
    data.farm.ws.dt[[ws.col]] <- 
      data.farm.ws.dt[ws.key]$ws.angle
    
    border.key.idx <- ws.key$dist < 1
    border.key <- ws.key[border.key.idx,]
    border.key$dist <- NULL
    
    data.farm.ws.dt[[ws.col]][border.key.idx] <- 
      data.farm.ws.dt.border[border.key]$wp
    
    keys.na <- is.na(data.farm.ws.dt[[ws.col]]) &
      data.farm.ws.dt$date %in% data.test.keys$date
    
    data.farm.ws.dt[[ws.col]][keys.na] <-
      data.farm.ws.dt$ws.angle[keys.na]
  }
  data.farm.ws.dt$date.time <- NULL
  data.farm.ws.dt$dist <- as.factor(sprintf("%02d", data.farm.ws.dt$dist))
  setkeyv(data.farm.ws.dt,c("date","farm","dist"))
  
  data.farm.ws.dt
}