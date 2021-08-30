
Sys.setenv(TZ = "Africa/Addis_Ababa")

convJSON <- function(obj, ...){
    args <- list(...)
    if(!'pretty' %in% names(args)) args$pretty <- TRUE
    if(!'auto_unbox' %in% names(args)) args$auto_unbox <- TRUE
    if(!'na' %in% names(args)) args$na <- "null"
    args <- c(list(x = obj), args)
    json <- do.call(jsonlite::toJSON, args)
    return(json)
}

convCSV <- function(obj, col.names = TRUE){
    filename <- tempfile()
    write.table(obj, filename, sep = ",", na = "", col.names = col.names,
                row.names = FALSE, quote = FALSE)
    don <- readLines(filename)
    unlink(filename)
    don <- paste0(don, collapse = "\n")

    return(don)
}

connect.database <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- do.call(DBI::dbConnect, args)
    con
}

## replaced by DBI::dbGetQuery
getQuery <- function(con, query){
    res <- DBI::dbSendQuery(con, query)
    out <- DBI::dbFetch(res)
    DBI::dbClearResult(res)
    return(out)
}


char_utc2local_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = "UTC")
    x <- as.POSIXct(x)
    x <- format(x, format, tz = tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

time_utc2local_char <- function(dates, format, tz){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz)
    x
}

char_local2utc_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = tz)
    x <- as.POSIXct(x)
    x <- format(x, format, tz = "UTC")
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_local2utc_char <- function(dates, format){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = "UTC")
    x
}

time_local2utc_time <- function(dates){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_local2utc_char(dates, format)
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_utc2time_local <- function(dates, tz){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_utc2local_char(dates, format, tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

day_of_month <- function(year, mon){
    daty <- paste(year, mon, 28:31, sep = '-')
    daty <- as.Date(daty)
    rev((28:31)[!is.na(daty)])[1]
}

nb_day_of_month <- function(daty){
    nbm <- mapply(day_of_month,
                  substr(daty, 1, 4),
                  substr(daty, 5, 6),
                  USE.NAMES = FALSE)
    as.numeric(nbm)
}

nb_day_of_pentad <- function(daty){
    day <- as.numeric(substr(daty, 7, 7))
    nbp <- rep(5, length(daty))
    nbp[day >= 6] <- nb_day_of_month(daty[day == 6]) - 25
    return(nbp)
}

nb_day_of_dekad <- function(daty){
    day <- as.numeric(substr(daty, 7, 7))
    nbd <- rep(10, length(daty))
    nbd[day == 3] <- nb_day_of_month(daty[day == 3]) - 20
    return(nbd)
}

#########

getAggrDateRange <- function(tstep, start, end, tz){
    datyRg <- switch(tstep,
                    'hourly' = local({
                        xstart <- strptime(start, "%Y-%m-%d-%H", tz = tz)
                        xend <- strptime(end, "%Y-%m-%d-%H", tz = tz)
                        c(xstart, xend)
                    }),
                    'daily' = local({
                        xstart <- as.Date(start, "%Y-%m-%d")
                        xend <- as.Date(end, "%Y-%m-%d")
                        c(xstart, xend)
                    }),
                    'pentad' = local({
                        xstart <- as.Date(start, "%Y-%m-%d")
                        tmp_s <- as.numeric(format(xstart, '%d'))
                        tmp_s <- seq(1, 26, 5)[tmp_s]
                        xstart <- paste0(format(xstart, "%Y-%m-"), tmp_s)
                        xstart <- as.Date(xstart)
                        xend <- as.Date(end, "%Y-%m-%d")
                        tmp_e <- as.numeric(format(xend, '%d'))
                        if(tmp_e < 6){
                            tmp_e1 <- seq(5, 25, 5)[tmp_e]
                        }else{
                            tmp_e1 <- day_of_month(format(xend, '%Y'),
                                                   format(xend, '%m'))
                        }
                        xend <- paste0(format(xend, "%Y-%m-"), tmp_e1)
                        xend <- as.Date(xend)
                        c(xstart, xend)
                     }),
                    'dekadal' = local({
                        xstart <- as.Date(start, "%Y-%m-%d")
                        tmp_s <- as.numeric(format(xstart, '%d'))
                        tmp_s <- c(1, 11, 21)[tmp_s]
                        xstart <- paste0(format(xstart, "%Y-%m-"), tmp_s)
                        xstart <- as.Date(xstart)
                        xend <- as.Date(end, "%Y-%m-%d")
                        tmp_e <- as.numeric(format(xend, '%d'))
                        if(tmp_e < 3){
                            tmp_e1 <- c(10, 20)[tmp_e]
                        }else{
                            tmp_e1 <- day_of_month(format(xend, '%Y'),
                                                   format(xend, '%m'))
                        }
                        xend <- paste0(format(xend, "%Y-%m-"), tmp_e1)
                        xend <- as.Date(xend)
                        c(xstart, xend)
                    }),
                    'monthly' = local({
                        xstart <- as.Date(paste0(start, '-1'), "%Y-%m-%d")
                        xend <- as.Date(paste0(end, '-15'), "%Y-%m-%d")
                        tmp_e <- day_of_month(format(xend, '%Y'),
                                              format(xend, '%m'))
                        xend <- paste0(format(xend, "%Y-%m-"), tmp_e)
                        xend <- as.Date(xend)
                        c(xstart, xend)
                    })
                )

    return(datyRg)
}


