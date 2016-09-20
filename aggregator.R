library(zoo)
library(lubridate)
library(data.table)

##########################################################
# EDIT THIS TO YOUR DESIRED DIRECTORY! (keep slash at end)
stockdir <- "/home/alex/oss/data/stocks/"
refinedir <- "/home/alex/oss/data/refined/"
##########################################################

aggregate.Trades <- function(date, exch, stock, beginTime, endTime){
trades <- fread(paste0(stockdir, date, "/", exch, "/", stock, ".csv"))
                  
trades[, V1 := as.POSIXct(V1, format = "%Y-%m-%d %H:%M:%OS")]

setnames(trades, "V1", "ddate")
setnames(trades, "V2", "price")
setnames(trades, "V3", "vol")
setnames(trades, "V4", "exch")
setnames(trades, "V5", "cond")

trades.filtered <- trades[!grepl("[A-DG-Z1-6]", cond)]

beginTime <- as.POSIXct(paste(date, "14:30:00"))
#beginTime <- force_tz(beginTime, "EST")
#beginTime <- as.POSIXct(with_tz(beginTime, "GMT"))

endTime <- as.POSIXct(paste(date, "21:00:30"))
#endTime <- force_tz(endTime, "EST")
#beginTime <- as.POSIXct(with_tz(endTime, "GMT"))

trades.filtered.05 <- trades.filtered[
  ,list(vwap = weighted.mean(price, vol), vol = sum(vol))
  ,keyby = list(btime=cut(ddate, 
                          seq(beginTime, endTime, by="5 sec"), 
                          include.lowest=T))
  ]
trades.filtered.10 <- trades.filtered[
  ,list(vwap = weighted.mean(price, vol), vol = sum(vol))
  ,keyby = list(btime=cut(ddate, 
                          seq(beginTime, endTime, by="10 sec"), 
                          include.lowest=T))
  ]
trades.filtered.15 <- trades.filtered[
  ,list(vwap = weighted.mean(price, vol), vol = sum(vol))
  ,keyby = list(btime=cut(ddate, 
                          seq(beginTime, endTime, by="15 sec"), 
                          include.lowest=T))
  ]
trades.filtered.30 <- trades.filtered[
  ,list(vwap = weighted.mean(price, vol), vol = sum(vol))
  ,keyby = list(btime=cut(ddate, 
                          seq(beginTime, endTime, by="30 sec"), 
                          include.lowest=T))
  ]

trades.filtered.05[, btime := as.POSIXct(btime, format = "%Y-%m-%d %H:%M:%S"
                                         ,tz = "GMT")]
trades.filtered.10[, btime := as.POSIXct(btime, format = "%Y-%m-%d %H:%M:%S"
                                         ,tz = "GMT")]
trades.filtered.15[, btime := as.POSIXct(btime, format = "%Y-%m-%d %H:%M:%S"
                                         ,tz = "GMT")]
trades.filtered.30[, btime := as.POSIXct(btime, format = "%Y-%m-%d %H:%M:%S"
                                         ,tz = "GMT")]

trades.filtered.05[, btime := format(btime, tz = "", usetz = T)]
trades.filtered.10[, btime := format(btime, tz = "", usetz = T)]
trades.filtered.15[, btime := format(btime, tz = "", usetz = T)]
trades.filtered.30[, btime := format(btime, tz = "", usetz = T)]

trades.filtered.05[, btime := strtrim(btime, 19)]
trades.filtered.10[, btime := strtrim(btime, 19)]
trades.filtered.15[, btime := strtrim(btime, 19)]
trades.filtered.30[, btime := strtrim(btime, 19)]

if(!file.exists(paste0(refinedir, date))){
  dir.create(file.path(paste0(refinedir, date)))}
if(!file.exists(paste0(refinedir, date, "/", exch))){
  dir.create(file.path(paste0(refinedir, date, "/", exch)))}
if(!file.exists(paste0(refinedir, date, "/", exch, "/", stock))){
  dir.create(file.path(paste0(refinedir, date, "/", exch, "/"
                              , stock)))}

write.table(trades.filtered.05, paste0(refinedir, date, "/", exch
                                       , "/", stock, "/", stock, "05.csv")
            , quote = F, sep = ",", row.names = F
            , col.names = F)
write.table(trades.filtered.10, paste0(refinedir, date, "/", exch
                                       , "/", stock, "/", stock, "10.csv")
            , quote = F, sep = ",", row.names = F
            , col.names = F)
write.table(trades.filtered.15, paste0(refinedir, date, "/", exch
                                       , "/", stock, "/", stock, "15.csv")
            , quote = F, sep = ",", row.names = F
            , col.names = F)
write.table(trades.filtered.30, paste0(refinedir, date, "/", exch
                                       , "/", stock, "/", stock, "30.csv")
            , quote = F, sep = ",", row.names = F
            , col.names = F)

rm(trades, trades.filtered, trades.filtered.05, trades.filtered.10
   , trades.filtered.15, trades.filtered.30, beginTime, endTime)
}

date = Sys.Date()
# date = "2014-01-17"

beginTime <- ymd_hms(paste(date, "14:30:00"))
beginTime <- force_tz(beginTime, "EST")
beginTime <- as.POSIXct(with_tz(beginTime, "GMT"))

endTime <- ymd_hms(paste(date, "21:00:30"))
endTime <- force_tz(endTime, "EST")
endTime <- as.POSIXct(with_tz(endTime, "GMT"))

files = list.files(paste0(stockdir, date))
for(i in 1:length(files)){
  exch = files[i]
  in_exch = list.files(paste0(stockdir, date, "/", exch))
  for(j in 1:length(in_exch)){
    stock = sub(".csv", "", in_exch[j]) 
    aggregate.Trades(date, exch, stock, beginTime, endTime)  
  }
  rm(in_exch)
}
