#' Get AWS 15 minutes spatial data.
#'
#' Get AWS 15 minutes spatial data to display on map.
#' 
#' @param time the time to display in the format "YYYY-MM-DD-HH-MM".
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

mapMinAWSData <- function(time, aws_dir){
    spdon <- spatialMinAWSData(time, aws_dir)
    if(spdon$status != "ok") return(convJSON(spdon))
    don <- spdon$data

    ############
    varO <- sapply(strsplit(spdon$vars, "_"), '[[', 1)
    varP <- spdon$vars[!varO %in% c("9", "12", "13")]

    outKey <- list()

    donP <- lapply(varP, function(vp){
        x <- strsplit(vp, "_")[[1]][1]
        if(x == "1"){
            ops <- list(var.name = "PR", colorP = 'rainbow')
            ix <- NULL
            vr <- 'PR'
        }
        if(x == "2"){
            ops <- list(var.name = "TT")
            ix <- NULL
            vr <- 'TT'
        }
        if(x == "3"){
            ops <- list(var.name = "TD")
            ix <- NULL
            vr <- 'TD'
        }
        if(x == "4"){
            ops <- list(var.name = "LWT", colorP = 'rainbow')
            ix <- NULL
            vr <- 'LWT'
        }
        if(x == "5"){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlGnBu")
            ops <- list(var.name = "RR", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[vp]]) & don[[vp]] == 0
            vr <- 'RR'
        }
        if(x == "6"){
            colorC <- c('orange', 'yellow', 'chartreuse', 'green', 'darkgreen')
            ops <- list(var.name = "RH", customC = TRUE, colorC = colorC)
            ix <- NULL
            vr <- 'RH'
        }
        if(x == "7"){
            ops <- list(var.name = "SM", colorP = 'rainbow')
            ix <- NULL
            vr <- 'SM'
        }
        if(x == "8"){
            colorC <- c('#37a39a', '#4dbf99', '#69d681', '#95eb5b', '#baf249',
                        '#e0f545', '#fcf33f', '#fae334', '#f7cf2d', '#f5b222',
                        '#ed8e1a', '#eb6117', '#d6361a', '#b01729')
            ops <- list(var.name = "RG", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[vp]]) & don[[vp]] == 0
            vr <- 'RG'
        }

        if(x %in% c("10", "11")){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlOrBr")
            ops <- list(var.name = "FF", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[vp]]) & don[[vp]] == 0
            vr <- 'FF'
        }
        if(x == "14"){
            ops <- list(var.name = "ST", colorP = 'rainbow')
            ix <- NULL
            vr <- 'ST'
        }

        ##########
        pars <- do.call(defColorKeyOptions, ops)

        ##########

        zmin <- suppressWarnings(min(don[[vp]], na.rm = TRUE))
        if(!is.infinite(zmin)){
            pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
        }
        zmax <- suppressWarnings(max(don[[vp]], na.rm = TRUE))
        if(!is.infinite(zmax)){
            nl <- length(pars$breaks)
            pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
        }

        kolor.p <- pars$colors[findInterval(don[[vp]], pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]
        if(!is.null(ix)) kolor.p[ix] <- "#FFFFFF" # "transparent"

        ##########

        nom <- gsub('\\.', '', names(pars))
        names(pars) <- nom

        ##########
        pars <- list(labels = pars$legendaxis$labels, colors = pars$colors)
        ##########

        if(is.null(outKey[[vr]])) outKey[[vr]] <<- pars

        ###############
        return(kolor.p)
    })

    names(donP) <- varP

    ############
    don <- list(date = spdon$date, data = don, color = donP,
                key = outKey, status = spdon$status)

    return(convJSON(don))
}

spatialMinAWSData <- function(time, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    temps <- strptime(time, "%Y-%m-%d-%H-%M", tz = tz)
    time <- as.numeric(temps)
    dout <- format(temps, "%Y-%m-%d %H:%M:%S")

    data.null <- list(date = dout, data = "null", status = "no-data", vars = "null")

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        data.null$status <- "failed-connection"
        return(data.null)
    }

    query <- paste0("SELECT * FROM aws_data WHERE obs_time=", time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0) return(data.null)

    qres$value[!is.na(qres$limit_check)] <- NA

    nmVar <- c("network", "id", "height", "var_code",
                "stat_code", "value", "raw_value")
    qres <- qres[, nmVar, drop = FALSE]

    ######
    adcoCrd <- DBI::dbReadTable(conn, "adcon_crds")
    adcoCrd$network <- "Adcon"
    vaisCrd <- DBI::dbReadTable(conn, "vaisala_crds")
    vaisCrd$network <- "Vaisala"
    koicCrd <- DBI::dbReadTable(conn, "koica_crds")
    koicCrd$network <- "Koica"

    DBI::dbDisconnect(conn)

    nmCol <- c("id", "name", "longitude", "latitude", "altitude", "network")
    # crds <- rbind(adcoCrd[, nmCol, drop = FALSE], vaisCrd[, nmCol, drop = FALSE])
    crds <- rbind(adcoCrd[, nmCol, drop = FALSE],
                  vaisCrd[, nmCol, drop = FALSE],
                  koicCrd[, nmCol, drop = FALSE])

    id_net <- rep(NA, nrow(crds))
    id_net[crds$network == "Vaisala"] <- 1
    id_net[crds$network == "Adcon"] <- 2
    id_net[crds$network == "Koica"] <- 3
    crds$network_code <- id_net
    id_aws <- paste0(id_net, "_", crds$id)

    qres$aws <- paste0(qres$network, "_", qres$id)
    qres$vars <- paste0(qres$var_code, "_", qres$height, "_", qres$stat_code)

    # qres$index <- seq(nrow(qres))
    # index <- reshape2::acast(qres, aws~vars, mean, value.var = 'index')
    # index[is.nan(index)] <- NA
    # qc_value <- qres$value[index]
    # dim(qc_value) <- dim(index)
    # raw_value <- qres$raw_value[index]
    # dim(raw_value) <- dim(index)

    don <- reshape2::acast(qres, aws~vars, mean, value.var = 'value')
    don[is.nan(don)] <- NA
    nomV <- dimnames(don)[[2]]
    ix <- match(dimnames(don)[[1]], id_aws)
    crds <- crds[ix, , drop = FALSE]
    don <- cbind(don, crds)

    ina <- !is.na(don$longitude) & !is.na(don$latitude)
    don <- don[ina, , drop = FALSE]

    if(nrow(don) == 0) return(data.null)

    data.null$data <- don
    data.null$status <- "ok"
    data.null$vars <- nomV

    return(data.null)
}

############
#' Get AWS aggregated spatial data.
#'
#' Get AWS aggregated spatial data to display on map.
#' 
#' @param tstep the time step of the data.
#' @param time the time to display in the format,
#'              hourly: "YYYY-MM-DD-HH",
#'              daily: "YYYY-MM-DD",
#'              pentad: "YYYY-MM-DD",
#'              dekadal: "YYYY-MM-DD",
#'              monthly: "YYYY-MM"
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

# displayMAPAggr
mapAggrAWSData <- function(tstep, time, aws_dir){
    spdon <- spatialAggrAWS(tstep, time, aws_dir)
    if(spdon$status != "ok") return(convJSON(spdon))
    don <- spdon$data

    ############

    outKey <- list()

    donP <- lapply(spdon$vars, function(vp){
        x <- strsplit(vp, "_")[[1]][1]
        if(x == "1"){
            ops <- list(var.name = "PR", colorP = 'rainbow')
            ix <- NULL
            vr <- 'PR'
        }
        if(x == "2"){
            ops <- list(var.name = "TT")
            ix <- NULL
            vr <- 'TT'
        }
        if(x == "3"){
            ops <- list(var.name = "TD")
            ix <- NULL
            vr <- 'TD'
        }
        if(x == "4"){
            ops <- list(var.name = "LWT", colorP = 'rainbow')
            ix <- NULL
            vr <- 'LWT'
        }
        if(x == "5"){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlGnBu")
            ops <- list(var.name = "RR", timestep = tstep, customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[vp]]) & don[[vp]] == 0
            vr <- 'RR'
        }
        if(x == "6"){
            colorC <- c('orange', 'yellow', 'chartreuse', 'green', 'darkgreen')
            ops <- list(var.name = "RH", customC = TRUE, colorC = colorC)
            ix <- NULL
            vr <- 'RH'
        }
        if(x == "7"){
            ops <- list(var.name = "SM", colorP = 'rainbow')
            ix <- NULL
            vr <- 'SM'
        }
        if(x == "8"){
            colorC <- c('#37a39a', '#4dbf99', '#69d681', '#95eb5b', '#baf249',
                        '#e0f545', '#fcf33f', '#fae334', '#f7cf2d', '#f5b222',
                        '#ed8e1a', '#eb6117', '#d6361a', '#b01729')
            ops <- list(var.name = "RG", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[vp]]) & don[[vp]] == 0
            vr <- 'RG'
        }

        if(x == "10"){
            colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlOrBr")
            ops <- list(var.name = "FF", customC = TRUE, colorC = colorC)
            ix <- !is.na(don[[vp]]) & don[[vp]] == 0
            vr <- 'FF'
        }
        if(x == "14"){
            ops <- list(var.name = "ST", colorP = 'rainbow')
            ix <- NULL
            vr <- 'ST'
        }

        ##########
        pars <- do.call(defColorKeyOptions, ops)

        ##########

        zmin <- suppressWarnings(min(don[[vp]], na.rm = TRUE))
        if(!is.infinite(zmin)){
            pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
        }
        zmax <- suppressWarnings(max(don[[vp]], na.rm = TRUE))
        if(!is.infinite(zmax)){
            nl <- length(pars$breaks)
            pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
        }

        kolor.p <- pars$colors[findInterval(don[[vp]], pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]
        if(!is.null(ix)) kolor.p[ix] <- "#FFFFFF" # "transparent"

        ##########

        nom <- gsub('\\.', '', names(pars))
        names(pars) <- nom

        ##########
        pars <- list(labels = pars$legendaxis$labels, colors = pars$colors)
        ##########

        if(is.null(outKey[[vr]])) outKey[[vr]] <<- pars

        ###############
        return(kolor.p)
    })

    names(donP) <- spdon$vars

   ############

    don <- list(date = spdon$date, data = don, color = donP,
                key = outKey, status = spdon$status)

    return(convJSON(don))
}

spatialAggrAWS <- function(tstep, time, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    infoData <- switch(tstep,
                       'hourly' = local({
                            tt <- strptime(time, "%Y-%m-%d-%H", tz = tz)
                            tt1 <- format(tt, "%Y%m%d%H")
                            obs_time <- as.numeric(tt)
                            list(time = tt, date = tt1, obs_time = obs_time)
                       }),
                       'daily' = local({
                            tt <- as.Date(time, "%Y-%m-%d")
                            tt1 <- format(tt, "%Y%m%d")
                            obs_time <- as.numeric(tt)
                            list(time = tt, date = tt1, obs_time = obs_time)
                       }),
                       'pentad' = local({
                            tt <- as.Date(time, "%Y-%m-%d")
                            tmp <- as.numeric(format(tt, '%d'))
                            tt1 <- paste0(format(tt, "%Y%m"), tmp)
                            start <- c(1, 6, 11, 16, 21, 26)[tmp]
                            end <- c(5, 10, 15, 20, 25, nb_day_of_month(tt1))[tmp]
                            start <- as.Date(paste0(format(tt, "%Y-%m-"), start))
                            end <- as.Date(paste0(format(tt, "%Y-%m-"), end))
                            obs_time <- as.numeric(seq(start, end, 'day'))
                            list(time = tt, date = tt1, obs_time = obs_time)
                       }),
                       'dekadal' = local({
                            tt <- as.Date(time, "%Y-%m-%d")
                            tmp <- as.numeric(format(tt, '%d'))
                            tt1 <- paste0(format(tt, "%Y%m"), tmp)
                            start <- c(1, 11, 21)[tmp]
                            end <- c(10, 20, nb_day_of_month(tt1))[tmp]
                            start <- as.Date(paste0(format(tt, "%Y-%m-"), start))
                            end <- as.Date(paste0(format(tt, "%Y-%m-"), end))
                            obs_time <- as.numeric(seq(start, end, 'day'))
                            list(time = tt, date = tt1, obs_time = obs_time)
                       }),
                       'monthly' = local({
                            tt <- as.Date(time, "%Y-%m-%d")
                            tt1 <- format(tt, "%Y%m")
                            start <- as.Date(paste0(format(tt, "%Y-%m-"), 1))
                            end <- as.Date(paste0(format(tt, "%Y-%m-"), nb_day_of_month(tt1)))
                            obs_time <- as.numeric(seq(start, end, 'day'))
                            list(time = tt, date = tt1, obs_time = obs_time)
                       })
                     )

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        pars_file <- 'AWS_dataHourVarObj.json'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        pars_file <- 'AWS_dataDayVarObj.json'
        qc_name <- 'qc_output'
    }

    data.null <- list(date = infoData$date, data = "null", status = "no-data", vars = "null")

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        data.null$opts$status <- "failed-connection"
        return(convJSON(data.null))
    }

    if(tstep %in% c("hourly", "daily")){
        query <- paste0("SELECT * FROM ", data_table,
                        " WHERE obs_time=", infoData$obs_time)
    }else{
        obs_time <- paste0(infoData$obs_time, collapse = ", ")
        query <- paste0("SELECT * FROM aws_daily WHERE obs_time IN (", obs_time, ")")
    }

    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0) return(data.null)

    qres[!is.na(qres[, qc_name]), 'value'] <- NA
    qres_var_hgt <- paste0(qres$var_code, "_", qres$height)

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", pars_file)
    pars_info <- jsonlite::read_json(parsFile)
    pars_info <- pars_info$variables
    var_hgt <- sapply(pars_info, function(v) paste0(v$var_code, '_', v$height))

    qres <- qres[qres_var_hgt %in% var_hgt, , drop = FALSE]
    if(nrow(qres) == 0) return(data.null)

    ######
    adcoCrd <- DBI::dbReadTable(conn, "adcon_crds")
    adcoCrd$network <- "Adcon"
    vaisCrd <- DBI::dbReadTable(conn, "vaisala_crds")
    vaisCrd$network <- "Vaisala"
    koicCrd <- DBI::dbReadTable(conn, "koica_crds")
    koicCrd$network <- "Koica"

    DBI::dbDisconnect(conn)

    nmCol <- c("id", "name", "longitude", "latitude", "altitude", "network")
    # crds <- rbind(adcoCrd[, nmCol, drop = FALSE], vaisCrd[, nmCol, drop = FALSE])
    crds <- rbind(adcoCrd[, nmCol, drop = FALSE],
                  vaisCrd[, nmCol, drop = FALSE],
                  koicCrd[, nmCol, drop = FALSE])

    id_net <- rep(NA, nrow(crds))
    id_net[crds$network == "Vaisala"] <- 1
    id_net[crds$network == "Adcon"] <- 2
    id_net[crds$network == "Koica"] <- 3
    crds$network_code <- id_net
    id_aws <- paste0(id_net, "_", crds$id)

    ############

    if(tstep %in% c("pentad", "dekadal", "monthly")){
        mfracFile <- paste0("Min_Frac_", tools::toTitleCase(tstep), ".json")
        mfracFile <- file.path(aws_dir, "AWS_DATA", "JSON", mfracFile)
        minFrac <- jsonlite::read_json(mfracFile)

        fun_nb_day <- switch(tstep,
                            "pentad" = nb_day_of_pentad,
                            "dekadal" = nb_day_of_dekad,
                            "monthly" = nb_day_of_month)
        nb_day <- fun_nb_day(infoData$date)

        ### aggregate
        qres$aws_time <- paste0(qres$network, "/", qres$id, "/", qres$obs_time)
        qres$vars <- paste0(qres$var_code, "_", qres$height, "_", qres$stat_code)

        don <- reshape2::acast(qres, aws_time~vars, mean, value.var = 'value')
        don[is.nan(don)] <- NA
        d_col <- dimnames(don)[[2]]
        d_row <- dimnames(don)[[1]]
        d_aws <- strsplit(d_row, '/')
        ix_aws <- sapply(d_aws, function(x) paste0(x[1], '_', x[2]))

        d_vars <- strsplit(d_col, '_')
        ic_var <- sapply(d_vars, '[[', 1)
        ic_stat <- sapply(d_vars, '[[', 3)

        # fun_agg <- lapply(ic_stat, function(s) if(s == "4") sum else mean)
        fun_agg <- lapply(ic_stat, function(s){
            switch(s, "4" = sum, "1" = mean,
                      "2" = min, "3" = max)
        })

        idx_row <- split(seq_along(ix_aws), ix_aws)
        don <- lapply(idx_row, function(ii){
            dat <- don[ii, , drop = FALSE]
            if(nrow(dat) == 0) return(rep(NA, length(d_col)))
            sapply(seq_along(d_col), function(i){
                x <- dat[, i]
                x <- x[!is.na(x)]
                if(length(x) == 0) return(NA)
                avail_frac <- length(x)/nb_day
                if(avail_frac < minFrac[[ic_var[[i]]]]) return(NA)
                fun_agg[[i]](x)
            })
        })

        don <- do.call(rbind.data.frame, don)
        names(don) <- d_col
        rownames(don) <- names(idx_row)
        ix <- match(names(idx_row), id_aws)
    }else{
        qres$aws <- paste0(qres$network, "_", qres$id)
        qres$vars <- paste0(qres$var_code, "_", qres$height, "_", qres$stat_code)

        don <- reshape2::acast(qres, aws~vars, mean, value.var = 'value')
        don[is.nan(don)] <- NA
        d_col <- dimnames(don)[[2]]
        ix <- match(dimnames(don)[[1]], id_aws)
    }

    crds <- crds[ix, , drop = FALSE]
    don <- cbind(don, crds)

    ina <- !is.na(don$longitude) & !is.na(don$latitude)
    don <- don[ina, , drop = FALSE]

    if(nrow(don) == 0) return(data.null)

    data.null$data <- don
    data.null$status <- "ok"
    data.null$vars <- d_col

    return(data.null)
}
