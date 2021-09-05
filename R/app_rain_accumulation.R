
#' Compute precipitation accumulation.
#'
#' Compute precipitation accumulation for chart display.
#' 
#' @param tstep time basis to accumulate the data.
#' @param net_aws a vector of the network code and AWS ID, form <network code>_<AWS ID>. AWS network code, 1: vaisala, 2: adcon, 3: koica
#' @param start start date.
#' @param end end date.
#' @param accumul accumulation duration.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

chartRainAccumul <- function(tstep, net_aws, start, end, accumul, aws_dir)
{
    don <- tsRainAccumulAWS(tstep, net_aws, start, end, accumul, aws_dir)

    aws_name <- paste0(don$coords$name, " [ID = " , don$coords$id,
                        " ; ", don$coords$network, "]")
    tt <- switch(tstep, "hourly" = "Hour", "daily" = "Day")
    titre <- paste(accumul, tt, "Rain Accumulation", "_", aws_name)
    nplt <- "Precip_Accumul"
    filename <- gsub(" ", ".", paste0(don$coords$name, '_', don$coords$id))

    opts <- list(title = titre, status = don$status,
                 name = nplt, filename = filename)
    ret <- list(opts = opts, data = NULL)

    if(don$status != 'ok') return(convJSON(ret))

    time <- 1000 * as.numeric(as.POSIXct(don$date))
    dat <- as.matrix(cbind(time, don$data))
    dimnames(dat) <- NULL

    ret$data <- dat
    ret$opts$status <- 'plot'

    return(convJSON(ret))
}

################
#' Compute precipitation accumulation.
#'
#' Compute precipitation accumulation for spatial display.
#' 
#' @param tstep time basis to accumulate the data.
#' @param time target date.
#' @param accumul accumulation duration.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/NMA_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

mapRainAccumul <- function(tstep, time, accumul, aws_dir)
{
    spdon <- spRainAccumulAWS(tstep, time, accumul, aws_dir)
    if(spdon$status != "ok") return(convJSON(spdon))
    don <- spdon$data
    don <- don[!is.na(don$longitude) & !is.na(don$latitude), , drop = FALSE]

    colorC <- RColorBrewer::brewer.pal(n = 9, name = "YlGnBu")
    ops <- list(timestep = tstep, customC = TRUE, colorC = colorC)
    ix <- !is.na(don$accumul) & don$accumul == 0
    pars <- do.call(defColorKeyOptionsAcc, ops)

    zmin <- suppressWarnings(min(don$accumul, na.rm = TRUE))
    if(!is.infinite(zmin)){
        pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
    }
    zmax <- suppressWarnings(max(don$accumul, na.rm = TRUE))
    if(!is.infinite(zmax)){
        nl <- length(pars$breaks)
        pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
    }

    kolor.p <- pars$colors[findInterval(don$accumul, pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]
    kolor.p[ix] <- "#FFFFFF"

    nom <- gsub('\\.', '', names(pars))
    names(pars) <- nom

    ##########
    pars <- list(labels = pars$legendaxis$labels, colors = pars$colors)
    ##########

    don <- list(date = spdon$date, data = don, color = kolor.p,
                key = pars, status = spdon$status)

    return(convJSON(don))
}

################

tsRainAccumulAWS <- function(tstep, net_aws, start, end, accumul, aws_dir)
{
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    accumul <- as.numeric(accumul)

    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    net_aws <- strsplit(net_aws, "_")[[1]]
    net_code <- sapply(awsPars, "[[", "network_code")
    aws_id <- sapply(awsPars, "[[", "id")
    istn <- which(net_code == net_aws[1] & aws_id == net_aws[2])
    awsPars <- awsPars[[istn]]
    coordAWS <- awsPars[c("network_code", "network", "id", "name",
                          "longitude", "latitude", "altitude",
                          "woreda", "zone", "region")]
    out <- list(data = NULL, date = NULL, coords = coordAWS, status = "no-data")

    if(is.null(awsPars$PARS_Info[['5']])) return(out)

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

    query <- paste0("SELECT obs_time, value, ", qc_name, " FROM ", data_table,
                    " WHERE (", "network=", net_aws[1], " AND id='", net_aws[2], 
                    "' AND height=1 AND var_code=5) AND (obs_time >= ",
                    start, " AND obs_time <= ", end, ")")

    qres <- DBI::dbGetQuery(conn, query)
    DBI::dbDisconnect(conn)

    if(nrow(qres) == 0) return(out)

    qres[!is.na(qres[, qc_name]), 'value'] <- NA

    if(tstep == "hourly"){
        out$date <- as.POSIXct(qres$obs_time, origin = origin, tz = tz)
    }else{
        out$date <- as.Date(qres$obs_time, origin = origin)
    }

    x <- qres$value
    if(accumul > 1){
        pth_pars <- file.path(aws_dir, "AWS_DATA", "JSON", "Rolling_Aggr.json")
        pars <- jsonlite::fromJSON(pth_pars)
        aggr_pars <- list(win = accumul, fun = 'sum', na.rm = TRUE,
                          min.data = as.numeric(pars$minfrac) * accumul,
                          na.pad = TRUE, fill = FALSE, align = "right"
                        )
        x <- do.call(.rollfun.vec, c(list(x = x), aggr_pars))
    }

    out$data <- x
    out$status <- 'ok'

    return(out)
}

################

spRainAccumulAWS <- function(tstep, time, accumul, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    accumul <- as.numeric(accumul)

    infoData <- switch(tstep,
                       'hourly' = local({
                            tt <- strptime(time, "%Y-%m-%d-%H", tz = tz)
                            if(accumul > 1){
                               obs_time <- as.numeric(seq(tt - (accumul - 1) * 3600, tt, 'hour'))
                            }else{
                                obs_time <- as.numeric(tt)
                            }
                            tt1 <- format(tt, "%Y%m%d%H")
                            list(time = tt, date = tt1, obs_time = obs_time)
                       }),
                       'daily' = local({
                            tt <- as.Date(time, "%Y-%m-%d")
                            if(accumul > 1){
                               obs_time <- as.numeric(seq(tt - accumul + 1, tt, 'day'))
                            }else{
                                obs_time <- as.numeric(tt)
                            }
                            tt1 <- format(tt, "%Y%m%d")
                            list(time = tt, date = tt1, obs_time = obs_time)
                       })
                     )

    if(tstep == 'hourly'){
        data_table <- 'aws_hourly'
        qc_name <- 'spatial_check'
    }else{
        data_table <- 'aws_daily'
        qc_name <- 'qc_output'
    }

    data.null <- list(date = infoData$date, data = "null", status = "no-data")

    ######
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    conn <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        data.null$status <- "failed-connection"
        return(convJSON(data.null))
    }

    if(accumul > 1){
        obs_time <- paste0(infoData$obs_time, collapse = ", ")
        query <- paste0("SELECT network, id, obs_time, value, ",
                        qc_name, " FROM ", data_table,
                        " WHERE (height=1 AND var_code=5 AND stat_code=4)",
                        " AND obs_time IN (", obs_time, ")")
    }else{
        query <- paste0("SELECT network, id, obs_time, value, ",
                        qc_name, " FROM ", data_table,
                        " WHERE (height=1 AND var_code=5 AND stat_code=4)",
                        " AND (obs_time=", infoData$obs_time, ")")
    }

    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0) return(data.null)

    qres[!is.na(qres[, qc_name]), 'value'] <- NA

    if(accumul > 1){
        pth_pars <- file.path(aws_dir, "AWS_DATA", "JSON", "Rolling_Aggr.json")
        pars <- jsonlite::fromJSON(pth_pars)

        qres$aws <- paste0(qres$network, "_", qres$id)
        don <- reshape2::acast(qres, obs_time~aws, mean, value.var = 'value')
        don[is.nan(don)] <- NA
        ina <- colSums(!is.na(don)) >= as.numeric(pars$minfrac) * accumul
        don <- colSums(don, na.rm = TRUE)
        don <- don[ina]
        id <- names(don)
        don <- as.numeric(don)
    }else{
        id <- paste0(qres$network, "_", qres$id)
        don <- qres$value
    }

    if(all(is.na(don))) return(data.null)

    adcoCrd <- DBI::dbReadTable(conn, "adcon_crds")
    adcoCrd$network <- "Adcon"
    vaisCrd <- DBI::dbReadTable(conn, "vaisala_crds")
    vaisCrd$network <- "Vaisala"
    koicCrd <- DBI::dbReadTable(conn, "koica_crds")
    koicCrd$network <- "Koica"

    DBI::dbDisconnect(conn)

    nmCol <- c("id", "name", "longitude", "latitude", "altitude", "network")
    crds <- rbind(adcoCrd[, nmCol, drop = FALSE],
                  vaisCrd[, nmCol, drop = FALSE],
                  koicCrd[, nmCol, drop = FALSE])

    id_net <- rep(NA, nrow(crds))
    id_net[crds$network == "Vaisala"] <- 1
    id_net[crds$network == "Adcon"] <- 2
    id_net[crds$network == "Koica"] <- 3
    # crds$network_code <- id_net
    id_aws <- paste0(id_net, "_", crds$id)

    ix <- match(id, id_aws)
    crds <- crds[ix, , drop = FALSE]

    crds$accumul <- don
    data.null$data <- crds
    data.null$status <- "ok"

    return(data.null)
}

