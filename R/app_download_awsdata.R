#' Get 15 minutes data.
#'
#' Get 15 minutes data for download.
#' 
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return JSON format object
#' 
#' @export

downAWSMinDataCSV <- function(net_aws, var_hgt, start, end, aws_dir)
{
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    net_aws <- strsplit(net_aws, "_")[[1]]
    var_hgt <- strsplit(var_hgt, "_")[[1]]

    net_code <- sapply(awsPars, "[[", "network_code")
    aws_id <- sapply(awsPars, "[[", "id")

    istn <- which(net_code == net_aws[1] & aws_id == net_aws[2])
    awsPars <- awsPars[[istn]]

    aws_name <- paste0(gsub(" ", "-", awsPars$name), "_" , awsPars$id, "_", awsPars$network)
    var_name <- gsub(" ", "-", awsPars$PARS_Info[[var_hgt[1]]][[1]]$name)
    var_name <- paste0(var_name, "_at_", var_hgt[2], "m")

    filename <- paste0(var_name, "_", aws_name, ".csv")

    stats <- awsPars$STATS[[var_hgt[1]]][[var_hgt[2]]]
    stats <- do.call(rbind, lapply(stats, as.data.frame))

    OUT <- list(data = NULL, filename = filename)

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
        OUT$data <- data.frame(status = "unable to connect to database")
        return(convJSON(OUT))
    }

    query <- paste0("SELECT obs_time, stat_code, value FROM aws_data WHERE (",
                    "network=", net_aws[1], " AND id='", net_aws[2], 
                    "' AND height=", var_hgt[2], " AND var_code=", var_hgt[1], 
                    ") AND (", "obs_time >= ", start, " AND obs_time <= ", end, ")")

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0) {
        OUT$data <- data.frame(status = "no data")
        return(convJSON(OUT))
    }

    stat_code <- unique(qres$stat_code)
    if(length(stat_code) > 1){
        qres <- reshape2::acast(qres, obs_time~stat_code, value.var = 'value')
        ist <- match(dimnames(qres)[[2]], as.character(stats$code))
        dimnames(qres)[[2]] <- stats$name
        temps <- as.integer(dimnames(qres)[[1]])
        temps <- as.POSIXct(temps, origin = origin, tz = tz)
        temps <- format(temps, "%Y%m%d%H%M")
        don <- data.frame(Time = temps, qres)
        rownames(don) <- NULL
    }else{
        qres <- qres[, c('obs_time', 'value'), drop = FALSE]
        qres <- qres[order(qres[, 'obs_time']), , drop = FALSE]
        temps <- as.POSIXct(qres[, 'obs_time'], origin = origin, tz = tz)
        temps <- format(temps, "%Y%m%d%H%M")
        don <- data.frame(temps, qres[, 'value'])
        names(don) <- c('Time', stats$name)
    }

    OUT$data <- convCSV(don)

    return(convJSON(OUT))
}

#############

#' Get aggregated data.
#'
#' Get aggregated data displayed on the table for download.
#' 
#' @param tstep the time step of the data.
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param start start date.
#' @param end end date.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return CSV format object
#' 
#' @export

downTableAggrCSV <- function(tstep, net_aws, start, end, aws_dir){
    out <- getAggrAWSData_allVars(tstep, net_aws, start, end, aws_dir)
    return(convCSV(out))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data displayed on the chart for download.
#' 
#' @param tstep the time step of the data.
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param start start date.
#' @param end end date.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return CSV format object
#' 
#' @export

downAWSAggrOneVarCSV <- function(tstep, net_aws, var_hgt, start, end, aws_dir){
    out <- getAggrAWSData_oneVar(tstep, net_aws, var_hgt, start, end, aws_dir)

    if(out$status != "ok"){
        don <- data.frame(Date = NA, Status = out$status)
    }else{
        stat_name <- c('Ave', 'Min', 'Max', 'Tot')
        don <- out$data
        don[is.na(don)] <- -99
        names(don) <- stat_name[as.integer(names(don))]
        don <- data.frame(Date = out$date, don)
    }

    return(convCSV(don))
}

##########
#' Get aggregated data.
#'
#' Get aggregated data to download for multiple AWS.
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
#' @return CSV format object
#' 
#' @export

downTableAggrDataSelCSV <- function(tstep, net_aws, var_hgt, pars,
                                    start, end, aws_dir)
{
    out <- getAggrAWSData_awsSel(tstep, net_aws, var_hgt, pars, start, end, aws_dir)

    don <- data.frame(Date = NA, Status = "no.data")
    if(out$status != "ok"){
        don$Status <- out$status
        return(convCSV(don))
    }

    ina <- rowSums(!is.na(out$data)) > 0
    if(!any(ina)) return(convCSV(don))
    out$date <- out$date[ina]
    out$data <- out$data[ina, , drop = FALSE]
    out$data <- round(out$data, 1)

    don <- data.frame(out$date, out$data)
    names(don) <- c('Date', out$net_aws)

    return(convCSV(don))
}

##########
#' Get aggregated data in CDT format.
#'
#' Get aggregated data in CDT format for download.
#' 
#' @param tstep the time step of the data.
#' @param var_hgt the variable code and observation height, form  <var code>_<height>.
#' @param pars parameters.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return CSV object
#' 
#' @export

downAWSAggrCDTDataCSV <- function(tstep, var_hgt, pars, start, end, aws_dir)
{
    crds <- readCoordsData(aws_dir)
    crds[crds == ""] <- NA
    net_aws <- paste0(crds$network_code, '_', crds$id)
    don <- getAggrAWSData_awsSel(tstep, net_aws, var_hgt, pars, start, end, aws_dir)

    if(don$status != "ok"){
        return(convCSV(don$status, FALSE))
    } 
    capt <- c("NET_ID", "LON", "DATE/LAT")

    ix <- match(don$net_aws, net_aws)
    xhead <- rbind(don$net_aws, crds$longitude[ix], crds$latitude[ix])

    data_cdt <- rbind(cbind(capt, xhead), cbind(don$date, don$data))
    data_cdt[is.na(data_cdt)] <- -99
    dimnames(data_cdt) <- NULL

    return(convCSV(data_cdt, FALSE))
}

##########
#' Get wind data.
#'
#' Get wind data for download.
#' 
#' @param tstep time step of the data.
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param height the observation height.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return CSV object
#' 
#' @export

downWindBarbCSV <- function(net_aws, height, tstep, start, end, aws_dir)
{
    tz <- Sys.getenv("TZ")
    wnd <- getWindData(net_aws, height, tstep, start, end, aws_dir)
    if(wnd$status != "ok"){
        if(wnd$status == 'no-data')
            msg <- "No available data"
        if(wnd$status == 'failed-connection')
            msg <- "Unable to connect to databasea"

        ret <- data.frame(status = msg)
        return(convCSV(ret))
    }

    daty <- format(wnd$date, "%Y%m%d%H%M%S")
    ret <- data.frame(date = daty, ws = wnd$ws, wd = wnd$wd)
    return(convCSV(ret))
}

##########
#' Get wind frequency.
#'
#' Get wind frequency for download.
#' 
#' @param tstep time step of the data.
#' @param net_aws the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param height the observation height.
#' @param start start time.
#' @param end end time.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return CSV object
#' 
#' @export

downWindFreqCSV <- function(net_aws, height, tstep, start, end, aws_dir)
{
    don <- formatFreqTable(net_aws, height, tstep, start, end, aws_dir)
    if(don$status != "ok"){
        if(don$status == 'no-data')
            msg <- "No available data"
        if(don$status == 'failed-connection')
            msg <- "Unable to connect to databasea"

        ret <- data.frame(status = msg)
        return(convCSV(ret))
    }
    nom <- names(don$freq)
    don <- as.data.frame(don$freq)
    names(don) <- nom

    return(convCSV(don))
}

