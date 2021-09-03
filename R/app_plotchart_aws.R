
#' Get 15 minutes data.
#'
#' Get 15 minutes data to display on chart.
#' 
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param stat statistic code.
#' @param start start time.
#' @param end end time.
#' @param plotrange get range.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

chartMinAWSData <- function(net_aws, var_hgt, stat, start, end,
                            plotrange, aws_dir)
{
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    plotrange <- as.logical(as.integer(plotrange))

    net_aws <- strsplit(net_aws, "_")[[1]]
    var_hgt <- strsplit(var_hgt, "_")[[1]]

    net_code <- sapply(awsPars, "[[", "network_code")
    aws_id <- sapply(awsPars, "[[", "id")

    istn <- which(net_code == net_aws[1] & aws_id == net_aws[2])
    awsPars <- awsPars[[istn]]

    aws_name <- paste0(awsPars$name, " [ID = " , awsPars$id, " ; ", awsPars$network, "]")

    var_name <- awsPars$PARS_Info[[var_hgt[1]]][[1]]$name
    var_stat <- awsPars$STATS[[var_hgt[1]]][[var_hgt[2]]]
    # var_stat <- awsPars$STATS[['2']][['2']]
    stat_name <- sapply(var_stat, '[[', 'name')
    stat_code <- sapply(var_stat, '[[', 'code')
    pars <- stat_name[stat_code == as.integer(stat)]

    npars <- if(plotrange) paste0(", min-avg-max, ") else paste0(", ", pars, ", ")
    titre <- paste0(paste0(var_name, ' at ', var_hgt[2], 'm'), npars, aws_name)
    nplt <- paste0(var_name, " [", pars, "]")
    nstat <- if(plotrange) "_min-avg-max_" else paste0("_", pars)
    filename <- paste0(paste0(var_name, '_at_', var_hgt[2], 'm'),
                       nstat, '_', awsPars$id, '_', awsPars$network)
    filename <- gsub(" ", "-", filename)

    OUT <- list(opts = list(title = titre, arearange = FALSE, 
                status = 'no-data', name = 'none',
                filename = filename), data = NULL, var = var_hgt[1])

    ######
    start <- strptime(start, "%Y-%m-%d-%H-%M", tz = tz)
    start <- as.numeric(start)
    end <- strptime(end, "%Y-%m-%d-%H-%M", tz = tz)
    end <- as.numeric(end)

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        OUT$opts$status <- 'unable to connect to database'
        return(convJSON(OUT))
    }

    if(plotrange){
        query <- paste0("SELECT obs_time, stat_code, value FROM aws_data WHERE (",
                       "network=", net_aws[1], " AND id='", net_aws[2], 
                       "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                       " AND stat_code IN (1, 2, 3)) AND (",
                       "obs_time >= ", start, " AND obs_time <= ", end, ")")
    }else{
          query <- paste0("SELECT obs_time, value FROM aws_data WHERE (",
                       "network=", net_aws[1], " AND id='", net_aws[2], 
                       "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                       " AND stat_code=", stat, ") AND (",
                       "obs_time >= ", start, " AND obs_time <= ", end, ")")
    }

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0) return(convJSON(OUT))

    ######
    plotR <- FALSE

    if(plotrange){
        db_vorder <- c('Ave', 'Min', 'Max')
        qres <- reshape2::acast(qres, obs_time~stat_code, mean, value.var = 'value')
        qres[is.nan(qres)] <- NA
        c_qres <- as.integer(dimnames(qres)[[2]])
        c_qres <- db_vorder[c_qres]
        r_qres <- as.integer(dimnames(qres)[[1]])

        qres <- data.frame(r_qres, qres)
        names(qres) <- c("obs_time", c_qres)

        rvars <- c("Min", "Max", "Ave")
        if(all(rvars %in% names(qres))){
            plotR <- TRUE
            qres <- qres[, c("obs_time", rvars), drop = FALSE]
        }else{
            ist <- db_vorder[as.integer(stat)]
            if(ist %in% names(qres)){
                qres <- qres[, c("obs_time", ist), drop = FALSE]
                OUT$opts$title <- gsub("min-avg-max", ist, OUT$opts$title)
                OUT$opts$filename <- gsub("min-avg-max", ist, OUT$opts$filename)
            }else{
                return(convJSON(OUT))
            }
        }
    }

    ######
    qres <- qres[order(qres$obs_time), , drop = FALSE]
    don <- qres[, -1, drop = FALSE]
    daty <- as.POSIXct(qres$obs_time, origin = origin, tz = tz)

    ddif <- diff(daty)
    ## missing diff >  15 minutes
    idt <- which(ddif > 15)
    if(length(idt) > 0){
        miss.daty <- daty[idt] + 15 * 60
        miss.daty <- format(miss.daty, "%Y%m%d%H%M%S", tz = tz)
        daty1 <- rep(NA, length(daty) + length(miss.daty))
        don1 <- data.frame(stat = rep(NA, length(daty1)))

        if(plotR){
            don1 <- cbind(don1, don1, don1)
            names(don1) <- c('Min', 'Max', 'Ave')
        }

        daty1[idt + seq(length(miss.daty))] <- miss.daty
        ix <- is.na(daty1)
        daty1[ix] <- format(daty, "%Y%m%d%H%M%S", tz = tz)
        don1[ix, ] <- don

        daty <- strptime(daty1, "%Y%m%d%H%M%S", tz = tz)
        don <- don1
    }

    ######
    ## convert to millisecond
    time <- 1000 * as.numeric(as.POSIXct(daty))

    if(plotR){
        don <- as.matrix(cbind(time, don[, c('Min', 'Max', 'Ave')]))
        dimnames(don) <- NULL

        OUT$data <- don
        OUT$opts$name <- c("Range", "Average")
    }else{
        don <- as.matrix(cbind(time, don))
        dimnames(don) <- NULL

        OUT$data <- don
        OUT$opts$name <- nplt
    }

    OUT$opts$arearange <- plotR
    OUT$opts$status <- 'plot'

    return(convJSON(OUT))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on chart for one AWS.
#' 
#' @param tstep the time step of the data.
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param pars statistic name.
#' @param start start time.
#' @param end end time.
#' @param plotrange get range.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

chartAggrAWSData <- function(tstep, net_aws, var_hgt, pars,
                             start, end, plotrange, aws_dir)
{
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    plotrange <- as.logical(as.integer(plotrange))

    net_aws <- strsplit(net_aws, "_")[[1]]
    var_hgt <- strsplit(var_hgt, "_")[[1]]

    net_code <- sapply(awsPars, "[[", "network_code")
    aws_id <- sapply(awsPars, "[[", "id")

    istn <- which(net_code == net_aws[1] & aws_id == net_aws[2])
    awsPars <- awsPars[[istn]]

    aws_name <- paste0(awsPars$name, " [ID = " , awsPars$id, " ; ", awsPars$network, "]")

    var_name <- awsPars$PARS_Info[[var_hgt[1]]][[1]]$name
    stat_code <- (1:4)[c('Ave', 'Min', 'Max', 'Tot') %in% pars]

    npars <- if(plotrange) paste0(", Min-Ave-Max, ") else paste0(", ", pars, ", ")
    titre <- paste0(paste0(var_name, ' at ', var_hgt[2], 'm'), npars, aws_name)
    nplt <- paste0(var_name, " [", pars, "]")
    nstat <- if(plotrange) "_min-avg-max_" else paste0("_", pars)
    filename <- paste0(paste0(var_name, '_at_', var_hgt[2], 'm'),
                       nstat, '_', awsPars$id, '_', awsPars$network)
    filename <- gsub(" ", "-", filename)

    OUT <- list(opts = list(title = titre, arearange = FALSE, 
                status = 'no-data', name = 'none',
                filename = filename), data = NULL, var = var_hgt[1])

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        OUT$opts$status <- 'unable to connect to database'
        return(convJSON(OUT))
    }

    ######

    datyRg <- getAggrDateRange(tstep, start, end, tz)
    start <- as.numeric(datyRg[1]) - 1
    end <- as.numeric(datyRg[2]) + 1

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        qc_name <- 'qc_output'
    }

    ######

    if(plotrange){
        query <- paste0("SELECT obs_time, stat_code, value, ", qc_name, " FROM ", data_table,
                        " WHERE (", "network=", net_aws[1], " AND id='", net_aws[2], 
                        "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                        " AND stat_code IN (1, 2, 3)) AND (",
                        "obs_time >= ", start, " AND obs_time <= ", end, ")")
    }else{
          query <- paste0("SELECT obs_time, value, ", qc_name, " FROM ", data_table,
                          " WHERE (", "network=", net_aws[1], " AND id='", net_aws[2], 
                          "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                          " AND stat_code=", stat_code, ") AND (",
                          "obs_time >= ", start, " AND obs_time <= ", end, ")")
    }

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0) return(convJSON(OUT))

    qres[!is.na(qres[, qc_name]), 'value'] <- NA

    ######
    plotR <- FALSE

    if(plotrange){
        db_vorder <- c('Ave', 'Min', 'Max')
        qres <- reshape2::acast(qres, obs_time~stat_code, mean, value.var = 'value')
        c_qres <- as.integer(dimnames(qres)[[2]])
        c_qres <- db_vorder[c_qres]
        r_qres <- as.integer(dimnames(qres)[[1]])

        qres <- data.frame(r_qres, qres)
        names(qres) <- c("obs_time", c_qres)

        rvars <- c("Min", "Max", "Ave")
        if(all(rvars %in% names(qres))){
            plotR <- TRUE
            qres <- qres[, c("obs_time", rvars), drop = FALSE]
        }else{
            if(pars %in% names(qres)){
                qres <- qres[, c("obs_time", pars), drop = FALSE]
                OUT$opts$title <- gsub("min-avg-max", tolower(pars), OUT$opts$title)
                OUT$opts$filename <- gsub("min-avg-max", tolower(pars), OUT$opts$filename)
            }else{
                return(convJSON(OUT))
            }
        }
    }else{
        qres <- qres[, c('obs_time', 'value'), drop = FALSE]
        names(qres) <- c('obs_time', pars)
    }

    ######
    qres <- qres[order(qres$obs_time), , drop = FALSE]
    don <- qres[, -1, drop = FALSE]

    if(tstep == "hourly"){
        daty <- as.POSIXct(qres$obs_time, origin = origin, tz = tz)
        odaty <- daty
        seq_daty <- seq(min(daty), max(daty), 'hour')
    }else{
        daty <- as.Date(qres$obs_time, origin = origin)
        odaty <- daty
        seq_daty <- seq(min(daty), max(daty), 'day')
    }

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

            odaty <- as.Date(names(index), "%Y%m%d")
            seq_daty <- seq(min(odaty), max(odaty), 'day')
            tmp <- as.numeric(format(seq_daty, '%d'))
            ix <- tmp < 7
            it <- c(3, 7, 13, 17, 23, 27)[tmp[ix]]
            seq_daty <- as.Date(paste0(format(seq_daty, "%Y-%m-")[ix], it))

            pen <- as.numeric(format(odaty, "%d"))
            pen <- c(3, 7, 13, 17, 23, 27)[pen]
            odaty <- as.Date(paste0(format(odaty, "%Y-%m-"), pen))
        }

        if(tstep ==  "dekadal"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 10, 20, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_dekad

            odaty <- as.Date(names(index), "%Y%m%d")
            seq_daty <- seq(min(odaty), max(odaty), 'day')
            tmp <- as.numeric(format(seq_daty, '%d'))
            ix <- tmp < 4
            it <- c(5, 15, 25)[tmp[ix]]
            seq_daty <- as.Date(paste0(format(seq_daty, "%Y-%m-")[ix], it))

            dek <- as.numeric(format(odaty, "%d"))
            dek <- c(5, 15, 25)[dek]
            odaty <- as.Date(paste0(format(odaty, "%Y-%m-"), dek))
        }

        if(tstep ==  "monthly"){
            index <- split(seq_along(daty), yymm)
            nbday_fun <- nb_day_of_month

            odaty <- as.Date(paste(names(index), 15), "%Y%m%d")
            seq_daty <- seq(min(odaty), max(odaty), 'month')
        }

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
            agg <- lapply(names(x), function(n){
                fun <- switch(n, "Tot" = sum, "Ave" = mean,
                                 "Min" = min, "Max" = max)
                if(all(is.na(x[, n]))) return(NA)
                fun(x[, n], na.rm = TRUE)
            })
            agg <- do.call(cbind.data.frame, agg)
            names(agg) <- names(x)

            return(agg)
        })
        xout[ina, ] <- do.call(rbind, tmp)
        don <- xout
    }

    it <- match(seq_daty, odaty)
    don <- don[it, , drop = FALSE]
    don <- as.matrix(don)

    if(all(is.na(don))) return(convJSON(OUT))

    time <- 1000 * as.numeric(as.POSIXct(seq_daty))
    don <- cbind(time, don)
    dimnames(don) <- NULL

    OUT$opts$name <- if(plotR) c("Range", "Average") else nplt
    OUT$data <- don
    OUT$opts$arearange <- plotR
    OUT$opts$status <- 'plot'

    return(convJSON(OUT))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on table.
#' 
#' @param tstep the time step of the data.
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param start start date.
#' @param end end date.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

tableAggrAWSData <- function(tstep, net_aws, start, end, aws_dir){
    out <- getAggrAWSData_allVars(tstep, net_aws, start, end, aws_dir)
    return(convJSON(out))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on chart for multiple AWS.
#' 
#' @param tstep the time step of the data.
#' @param net_aws a vector of the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koika
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

chartAggrAWSDataSel <- function(tstep, net_aws, var_hgt, pars,
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

    titre <- paste0(var_name, ' at ', var_hgt[2], 'm', " - ", par_name)
    filename <- paste0(tstep, '_', var_name, '_at_', var_hgt[2], 'm', '_', par_name)
    filename <- gsub(" ", "-", filename)

    varlab <- paste(var_hgt[1], var_hgt[2], stat_code, sep = "_")

    opts <- list(filename = filename, status = "plot", title = titre)
    out <- list(data = "null", opts = opts, var = varlab)

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        out$opts$status <- "failed-connection"
        return(convJSON(out))
    }

    ######

    datyRg <- getAggrDateRange(tstep, start, end, tz)
    start <- as.numeric(datyRg[1]) - 1
    end <- as.numeric(datyRg[2]) + 1

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        qc_name <- 'qc_output'
    }

    all_aws <- paste0("(", sel_net, ", ", "'", sel_id, "'", ")")
    all_aws <- paste0(all_aws, collapse = ", ")

    query <- paste0("SELECT * FROM ", data_table, " WHERE (",
        "(network, id) IN (", all_aws, ") AND height=", var_hgt[2],
        " AND var_code=", var_hgt[1], " AND stat_code=", stat_code,
        ") AND (", "obs_time >= ", start, " AND obs_time <= ", end, ")")

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0){
        out$opts$status <- "no-data"
        return(convJSON(out))
    }

    qres[!is.na(qres[, qc_name]), 'value'] <- NA

    qres$aws <- paste0(qres$network, "_", qres$id)
    don <- reshape2::acast(qres, obs_time~aws, mean, value.var = 'value')
    don[is.nan(don)] <- NA
    isel <- match(sel_aws, dimnames(don)[[2]])
    don <- don[, isel, drop = FALSE]
    d_row <- as.integer(dimnames(don)[[1]])

    if(tstep == "hourly"){
        daty <- as.POSIXct(d_row, origin = origin, tz = tz)
        odaty <- daty
        seq_daty <- seq(min(daty), max(daty), 'hour')
    }else{
        daty <- as.Date(d_row, origin = origin)
        odaty <- daty
        seq_daty <- seq(min(daty), max(daty), 'day')
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

            odaty <- as.Date(names(index), "%Y%m%d")
            seq_daty <- seq(min(odaty), max(odaty), 'day')
            tmp <- as.numeric(format(seq_daty, '%d'))
            ix <- tmp < 7
            it <- c(3, 7, 13, 17, 23, 27)[tmp[ix]]
            seq_daty <- as.Date(paste0(format(seq_daty, "%Y-%m-")[ix], it))

            pen <- as.numeric(format(odaty, "%d"))
            pen <- c(3, 7, 13, 17, 23, 27)[pen]
            odaty <- as.Date(paste0(format(odaty, "%Y-%m-"), pen))
        }

        if(tstep ==  "dekadal"){
            jour <- as.numeric(format(daty, "%d"))
            jour <- cut(jour, c(1, 10, 20, 31),
                        labels = FALSE, include.lowest = TRUE)
            index <- split(seq_along(daty), paste0(yymm, jour))
            nbday_fun <- nb_day_of_dekad

            odaty <- as.Date(names(index), "%Y%m%d")
            seq_daty <- seq(min(odaty), max(odaty), 'day')
            tmp <- as.numeric(format(seq_daty, '%d'))
            ix <- tmp < 4
            it <- c(5, 15, 25)[tmp[ix]]
            seq_daty <- as.Date(paste0(format(seq_daty, "%Y-%m-")[ix], it))

            dek <- as.numeric(format(odaty, "%d"))
            dek <- c(5, 15, 25)[dek]
            odaty <- as.Date(paste0(format(odaty, "%Y-%m-"), dek))
        }

        if(tstep ==  "monthly"){
            index <- split(seq_along(daty), yymm)
            nbday_fun <- nb_day_of_month

            odaty <- as.Date(paste(names(index), 15), "%Y%m%d")
            seq_daty <- seq(min(odaty), max(odaty), 'month')
        }

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

    it <- match(seq_daty, odaty)
    don <- don[it, , drop = FALSE]
    don <- as.matrix(don)

    if(all(is.na(don))){
        out$opts$status <- "no-data"
        return(convJSON(out))
    }

    times <- 1000 * as.numeric(as.POSIXct(seq_daty))
    kolor <- fields::tim.colors(length(sel_aws))

    tmp <- lapply(seq_along(sel_aws), function(j){
        dat <- don[, j]
        if(all(is.na(dat))) return(NULL)
        dat <- cbind(times, dat)
        dimnames(dat) <- NULL
        list(name = sel_name[j], color = kolor[j], data = dat)
    })

    inull <- sapply(tmp, is.null)
    out$data <- tmp[!inull]

    return(convJSON(out))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to display on table for multiple AWS.
#' 
#' @param tstep the time step of the data.
#' @param net_aws a vector of the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koika
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export
tableAggrAWSDataSel <- function(tstep, net_aws, var_hgt, pars,
                                start, end, aws_dir)
{
    out <- getAggrAWSData_awsSel(tstep, net_aws, var_hgt, pars, start, end, aws_dir)

    don <- data.frame(Date = NA, Status = "no.data")
    if(out$status != "ok"){
        don$Status <- out$status
        return(convJSON(don))
    }

    ina <- rowSums(!is.na(out$data)) > 0
    if(!any(ina)) return(convJSON(don))
    out$date <- out$date[ina]
    out$data <- out$data[ina, , drop = FALSE]
    out$data <- round(out$data, 1)

    don <- data.frame(out$date, out$data)
    nom <- c('Date', out$net_aws)
    names(don) <- nom
    titre <- paste(out$var_name, out$stat_name, sep = ' - ')

    don <- list(data = don, title = titre, order = nom)

    return(convJSON(don))
}
