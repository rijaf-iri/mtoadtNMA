
getAggrAWSData_allVars <- function(tstep, net_aws, start, end, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    net_aws <- strsplit(net_aws, "_")[[1]]

    out <- data.frame(Date = NA, status = "no.data")

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        out$status <- 'unable to connect to database'
        return(out)
    }

    ######

    datyRg <- getAggrDateRange(tstep, start, end, tz)
    start <- as.numeric(datyRg[1])
    end <- as.numeric(datyRg[2])

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        pars_file <- 'AWS_dataHourVarObj.json'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        pars_file <- 'AWS_dataDayVarObj.json'
        qc_name <- 'qc_output'
    }

    ######

    query <- paste0("SELECT * FROM ", data_table, " WHERE (", 
                    "network=", net_aws[1], " AND id='", net_aws[2], "') AND (", 
                    "obs_time >= ", start, " AND obs_time <= ", end, ")")

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0) return(out)

    qres[!is.na(qres[, qc_name]), 'value'] <- NA
    qres_var_hgt <- paste0(qres$var_code, "_", qres$height)

    ######

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", pars_file)
    pars_info <- jsonlite::read_json(parsFile)
    pars_info <- pars_info$variables
    info_var_hgt <- sapply(pars_info, function(v) paste0(v$var_code, '_', v$height))

    qres <- qres[qres_var_hgt %in% info_var_hgt, , drop = FALSE]
    if(nrow(qres) == 0) return(out)

    ######

    qres$all_vars <- paste0(qres$var_code, "_", qres$height, "_", qres$stat_code)
    don <- reshape2::acast(qres, obs_time~all_vars, mean, value.var = 'value')
    don[is.nan(don)] <- NA
    d_row <- as.integer(dimnames(don)[[1]])
    d_col <- strsplit(dimnames(don)[[2]], "_")
    stat_name <- c('Ave', 'Min', 'Max', 'Tot')

    col_name <- sapply(d_col, function(x){
        var_hgt <- paste0(x[1], '_', x[2])
        ix <- which(info_var_hgt == var_hgt)
        vvr <- pars_info[[ix]]
        vvr <- paste0(vvr$var_name, "_", vvr$height, "m")
        vvr <- gsub(" ", "-", vvr)
        vst <- stat_name[as.integer(x[3])]
        paste0(vvr, "_", vst)
    })

    ######

    if(tstep == "hourly"){
        daty <- as.POSIXct(d_row, origin = origin, tz = tz)
        odaty <- format(daty, "%Y%m%d%H")
    }else{
        daty <- as.Date(d_row, origin = origin)
        odaty <- format(daty, "%Y%m%d")
    }

    ###########

    if(tstep %in% c("pentad", "dekadal", "monthly")){
        mfracFile <- paste0("Min_Frac_", tools::toTitleCase(tstep), ".json")
        mfracFile <- file.path(aws_dir, "AWS_DATA", "JSON", mfracFile)
        minFrac <- jsonlite::read_json(mfracFile)

        yymm <- format(daty, "%Y%m")
        if(tstep ==  "pentad"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 5, 10, 15, 20, 25, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_pentad
        }

        if(tstep ==  "dekadal"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 10, 20, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_dekad
        }

        if(tstep ==  "monthly"){
            index <- split(seq_along(daty), yymm)
            nbday_fun <- nb_day_of_month
        }

        odaty <- names(index)

        pmon <- lapply(index, function(x) as.numeric(unique(format(daty[x], "%m"))))
        nbd0 <- sapply(seq_along(pmon), function(j) nbday_fun(names(pmon[j])))
        nobs <- sapply(index, length)
        avail_frac <- nobs/nbd0

        tmp <- lapply(seq_along(d_col), function(j){
            ina <- avail_frac >= minFrac[[d_col[[j]][1]]]
            fun_agg <- switch(d_col[[j]][3],
                            "4" = sum, "1" = mean,
                            "2" = min, "3" = max)
            xout <- rep(NA, length(index))
            xout[ina] <- sapply(index[ina], function(ix){
                x <- don[ix, j]
                if(all(is.na(x))) return(NA)
                fun_agg(x, na.rm = TRUE)
             })

            xout
        })

        don <- do.call(cbind, tmp)
    }

    ina <- rowSums(!is.na(don)) > 0
    don <- don[ina, , drop = FALSE]
    odaty <- odaty[ina]
    out <- data.frame(odaty, don)
    names(out) <- c('Date', col_name)

    return(out)
}

##########

getAggrAWSData_oneVar <- function(tstep, net_aws, var_hgt, start, end, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    out <- list(date = NULL, data = NULL, status = "no-data")

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        out$status <- 'failed-connection'
        return(out)
    }

    ######

    datyRg <- getAggrDateRange(tstep, start, end, tz)
    start <- as.numeric(datyRg[1])
    end <- as.numeric(datyRg[2])

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        qc_name <- 'qc_output'
    }

    ######

    net_aws <- strsplit(net_aws, "_")[[1]]
    var_hgt <- strsplit(var_hgt, "_")[[1]]

    ######

    if(var_hgt[1] == "5"){
          query <- paste0("SELECT obs_time, stat_code, value, ", qc_name, " FROM ", data_table,
                          " WHERE (", "network=", net_aws[1], " AND id='", net_aws[2], 
                          "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                          ") AND (obs_time >= ", start, " AND obs_time <= ", end, ")")
    }else{
        query <- paste0("SELECT obs_time, stat_code, value, ", qc_name, " FROM ", data_table,
                        " WHERE (", "network=", net_aws[1], " AND id='", net_aws[2], 
                        "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                        " AND stat_code IN (1, 2, 3)) AND (",
                        "obs_time >= ", start, " AND obs_time <= ", end, ")")
    }

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0) return(out)

    qres[!is.na(qres[, qc_name]), 'value'] <- NA

    don <- reshape2::acast(qres, obs_time~stat_code, mean, value.var = 'value')
    d_row <- as.integer(dimnames(don)[[1]])
    d_col <- dimnames(don)[[2]]

    if(tstep == "hourly"){
        daty <- as.POSIXct(d_row, origin = origin, tz = tz)
        odaty <- format(daty, "%Y%m%d%H")
    }else{
        daty <- as.Date(d_row, origin = origin)
        odaty <- format(daty, "%Y%m%d")
    }

    ###########

    if(tstep %in% c("pentad", "dekadal", "monthly")){
        mfracFile <- paste0("Min_Frac_", tools::toTitleCase(tstep), ".json")
        mfracFile <- file.path(aws_dir, "AWS_DATA", "JSON", mfracFile)
        minFrac <- jsonlite::read_json(mfracFile)

        yymm <- format(daty, "%Y%m")
        if(tstep ==  "pentad"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 5, 10, 15, 20, 25, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_pentad
        }

        if(tstep ==  "dekadal"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 10, 20, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_dekad
        }

        if(tstep ==  "monthly"){
            index <- split(seq_along(daty), yymm)
            nbday_fun <- nb_day_of_month
        }

        odaty <- names(index)

        pmon <- lapply(index, function(x) as.numeric(unique(format(daty[x], "%m"))))
        nbd0 <- sapply(seq_along(pmon), function(j) nbday_fun(names(pmon[j])))
        nobs <- sapply(index, length)
        avail_frac <- nobs/nbd0
        ina <- avail_frac >= minFrac[[var_hgt[1]]]

        xout <- don[1, , drop = FALSE]
        xout[] <- NA
        xout <- xout[rep(1, length(index)), , drop = FALSE]

        tmp <- lapply(index[ina], function(ix){
            x <- don[ix, , drop = FALSE]
            agg <- lapply(d_col, function(n){
                fun <- switch(n, "4" = sum, "1" = mean,
                                 "2" = min, "3" = max)
                if(all(is.na(x[, n]))) return(NA)
                fun(x[, n], na.rm = TRUE)
            })
            agg <- do.call(cbind, agg)

            return(agg)
        })
        xout[ina, ] <- do.call(rbind, tmp)
        don <- xout
    }

    don <- data.frame(don)
    names(don) <- d_col
    rownames(don) <- NULL

    out <- list(date = odaty, data = don, status = "ok")

    return(out)
}

##########

getAggrAWSData_awsSel <- function(tstep, net_aws, var_hgt, pars,
                                  start, end, aws_dir)
{
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    net_aws <- strsplit(net_aws, "_")
    var_hgt <- strsplit(var_hgt, "_")[[1]]

    net_code <- sapply(awsPars, "[[", "network_code")
    aws_id <- sapply(awsPars, "[[", "id")

    istn <- sapply(net_aws, function(a) which(net_code == a[1] & aws_id == a[2]))
    awsPars <- awsPars[istn]

    sel_net <- sapply(awsPars, '[[', 'network_code')
    sel_id <- sapply(awsPars, '[[', 'id')
    sel_name <- sapply(awsPars, function(a) paste0(a$name, " [ID = " , a$id, " ; ", a$network, "]"))
    sel_aws <- paste0(sel_net, '_', sel_id)
    var_name <- awsPars[[1]]$PARS_Info[[var_hgt[1]]][[1]]$name

    stat_code <- (1:4)[c('Ave', 'Min', 'Max', 'Tot') %in% pars]
    par_name <- switch(pars, "Ave" = "Average", "Tot" = "Total",
                            "Min" = "Minimum", "Max" = "Maximum")

    out <- list(var_name = var_name, stat_name = par_name,
                var_code = var_hgt[1], height = var_hgt[2],
                stat_code = stat_code, net_aws = NULL,
                date = NULL, data = NULL, status = "no-data")

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        out$status <- 'failed-connection'
        return(out)
    }

    ######

    datyRg <- getAggrDateRange(tstep, start, end, tz)
    start <- as.numeric(datyRg[1])
    end <- as.numeric(datyRg[2])

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        qc_name <- 'qc_output'
    }

    ######

    all_aws <- paste0("(", sel_net, ", ", "'", sel_id, "'", ")")
    all_aws <- paste0(all_aws, collapse = ", ")

    query <- paste0("SELECT * FROM ", data_table, " WHERE (",
        "(network, id) IN (", all_aws, ") AND height=", var_hgt[2],
        " AND var_code=", var_hgt[1], " AND stat_code=", stat_code,
        ") AND (", "obs_time >= ", start, " AND obs_time <= ", end, ")")

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0){
        out$status <- "no-data"
        return(out)
    }

    qres[!is.na(qres[, qc_name]), 'value'] <- NA

    qres$aws <- paste0(qres$network, "_", qres$id)
    don <- reshape2::acast(qres, obs_time~aws, mean, value.var = 'value')
    don[is.nan(don)] <- NA
    isel <- match(sel_aws, dimnames(don)[[2]])
    don <- don[, isel, drop = FALSE]
    dimnames(don)[[2]] <- sel_aws
    d_row <- as.integer(dimnames(don)[[1]])

    if(tstep == "hourly"){
        daty <- as.POSIXct(d_row, origin = origin, tz = tz)
        odaty <- format(daty, "%Y%m%d%H")
    }else{
        daty <- as.Date(d_row, origin = origin)
        odaty <- format(daty, "%Y%m%d")
    }

    ###########

    if(tstep %in% c("pentad", "dekadal", "monthly")){
        mfracFile <- paste0("Min_Frac_", tools::toTitleCase(tstep), ".json")
        mfracFile <- file.path(aws_dir, "AWS_DATA", "JSON", mfracFile)
        minFrac <- jsonlite::read_json(mfracFile)

        yymm <- format(daty, "%Y%m")
        if(tstep ==  "pentad"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 5, 10, 15, 20, 25, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_pentad
        }

        if(tstep ==  "dekadal"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 10, 20, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_dekad
        }

        if(tstep ==  "monthly"){
            index <- split(seq_along(daty), yymm)
            nbday_fun <- nb_day_of_month
        }

        odaty <- names(index)

        pmon <- lapply(index, function(x) as.numeric(unique(format(daty[x], "%m"))))
        nbd0 <- sapply(seq_along(pmon), function(j) nbday_fun(names(pmon[j])))
        nobs <- sapply(index, length)
        avail_frac <- nobs/nbd0
        ina <- avail_frac >= minFrac[[var_hgt[1]]]

        fun_agg <- switch(pars, 
                          "Tot" = colSums,
                          "Ave" = colMeans,
                          "Min" = matrixStats::colMins,
                          "Max" = matrixStats::colMaxs)

        xout <- don[1, , drop = FALSE]
        xout[] <- NA
        xout <- xout[rep(1, length(index)), , drop = FALSE]
        tmp <- lapply(seq_along(index[ina]), function(j){
            ix <- index[ina][[j]]
            x <- don[ix, , drop = FALSE]
            nna <- colSums(!is.na(x))/nbd0[j] >= minFrac[[var_hgt[1]]]
            x <- fun_agg(x, na.rm = TRUE)
            x[!nna] <- NA
            x
        })

        xout[ina, ] <- do.call(rbind, tmp)
        don <- xout
    }

    ina <- colSums(!is.na(don)) == 0
    don <- don[, !ina, drop = FALSE]

    out$net_aws <- dimnames(don)[[2]]
    dimnames(don) <- NULL

    out$date <- odaty
    out$data <- don
    out$status <- "ok"

    return(out)
}
